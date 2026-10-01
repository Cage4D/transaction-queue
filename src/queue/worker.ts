import { TransactionQueue } from './core.js';
import { handlerRegistry, JobHandler } from './handlers.js';
import { Job, DEFAULT_QUEUE_CONFIG, QueueConfig } from '../jobs/types.js';

export interface WorkerOptions {
  queue?: TransactionQueue;
  config?: Partial<QueueConfig>;
  onJobStart?: (job: Job) => void;
  onJobComplete?: (job: Job) => void;
  onJobError?: (job: Job, error: Error) => void;
  onPollEmpty?: () => void;
}

export class QueueWorker {
  private queue: TransactionQueue;
  private config: QueueConfig;
  private running = false;
  private pollInterval: NodeJS.Timeout | null = null;
  private heartbeatIntervals = new Map<string, NodeJS.Timeout>();
  private shutdownSignal = false;
  private activeJobs = 0;
  private currentPoll: Promise<void> | null = null;
  private onJobStart?: (job: Job) => void;
  private onJobComplete?: (job: Job) => void;
  private onJobError?: (job: Job, error: Error) => void;
  private onPollEmpty?: () => void;

  constructor(options: WorkerOptions = {}) {
    this.queue = options.queue || new TransactionQueue(options.config);
    this.config = { ...DEFAULT_QUEUE_CONFIG, ...options.config };
    this.onJobStart = options.onJobStart;
    this.onJobComplete = options.onJobComplete;
    this.onJobError = options.onJobError;
    this.onPollEmpty = options.onPollEmpty;
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;
    this.shutdownSignal = false;
    this.activeJobs = 0;
    console.log(`Worker ${this.queue.getWorkerId()} started`);

    this.pollLoop();
  }

  async stop(graceful: boolean = true): Promise<void> {
    if (!this.running) {
      return;
    }

    this.shutdownSignal = true;
    console.log(`Worker ${this.queue.getWorkerId()} stopping...`);

    if (this.pollInterval) {
      clearTimeout(this.pollInterval);
      this.pollInterval = null;
    }

    // Let in-flight work finish while heartbeats keep the lease alive.
    if (graceful) {
      await this.waitForRunningJobs();
    }

    for (const interval of this.heartbeatIntervals.values()) {
      clearInterval(interval);
    }
    this.heartbeatIntervals.clear();

    this.running = false;
    console.log(`Worker ${this.queue.getWorkerId()} stopped`);
  }

  private async waitForRunningJobs(): Promise<void> {
    const maxWaitMs = 30000;
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      if (this.activeJobs === 0 && this.currentPoll === null) {
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  }

  private pollLoop(): void {
    if (this.shutdownSignal || this.currentPoll) {
      return;
    }

    const poll = async () => {
      if (this.shutdownSignal) {
        return;
      }

      try {
        const job = await this.queue.claimNextJob();

        if (!job) {
          if (this.onPollEmpty) {
            this.onPollEmpty();
          }
          return;
        }

        // processJob always handles its own errors
        await this.processJob(job);
      } catch (error) {
        console.error('Poll loop error:', error);
      }
    };

    this.currentPoll = poll().finally(() => {
      this.currentPoll = null;
      this.scheduleNextPoll();
    });
  }

  private scheduleNextPoll(): void {
    if (this.shutdownSignal || this.pollInterval) {
      return;
    }
    this.pollInterval = setTimeout(() => {
      this.pollInterval = null;
      this.pollLoop();
    }, this.config.pollIntervalMs);
  }

  private async processJob(job: Job): Promise<void> {
    this.activeJobs++;

    try {
      const handler = handlerRegistry.get(job.type);

      if (!handler) {
        // Retrying can't help if nothing is registered for this type.
        const error = new Error(`No handler registered for job type: ${job.type}`);
        console.error(error.message);
        await this.queue.failJob(job.id, error.message, { permanent: true });
        this.onJobError?.(job, error);
        return;
      }

      this.onJobStart?.(job);

      const heartbeatInterval = setInterval(async () => {
        try {
          await this.queue.extendLease(job.id, this.config.leaseDurationMs);
        } catch (err) {
          console.error(`Failed to extend lease for job ${job.id}:`, err);
        }
      }, this.config.leaseDurationMs / 3);

      this.heartbeatIntervals.set(job.id, heartbeatInterval);

      try {
        await handler(job.payload, job);

        const receiptClaimed = await this.queue.claimReceipt(job.id, job.idempotencyKey);
        if (!receiptClaimed) {
          console.log(`Job ${job.id} already processed (duplicate), marking done`);
        }
        await this.queue.completeJob(job.id);

        this.onJobComplete?.(job);
        console.log(`Job ${job.id} (${job.type}) completed successfully`);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`Job ${job.id} (${job.type}) failed:`, errorMessage);

        await this.queue.failJob(job.id, errorMessage);

        this.onJobError?.(job, error instanceof Error ? error : new Error(errorMessage));
      } finally {
        clearInterval(heartbeatInterval);
        this.heartbeatIntervals.delete(job.id);
      }
    } catch (error) {
      console.error(`Job ${job.id} (${job.type}) could not be finalized:`, error);
    } finally {
      this.activeJobs--;
    }
  }

  registerHandler(type: string, handler: JobHandler): void {
    handlerRegistry.register(type, handler);
  }

  isRunning(): boolean {
    return this.running;
  }

  getQueue(): TransactionQueue {
    return this.queue;
  }
}