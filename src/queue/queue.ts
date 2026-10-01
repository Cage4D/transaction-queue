import { TransactionQueue } from './core.js';
import { QueueWorker, WorkerOptions } from './worker.js';
import { handlerRegistry, JobHandler } from './handlers.js';
import { runMigrations } from '../database/schema.js';
import { EnqueueOptions, Job, QueueConfig, DEFAULT_QUEUE_CONFIG } from '../jobs/types.js';

export interface TransactionQueueOptions {
  config?: Partial<QueueConfig>;
  autoMigrate?: boolean;
  workerOptions?: WorkerOptions;
}

export class LeadflowQueue {
  private queue: TransactionQueue;
  private worker: QueueWorker | null = null;
  private started = false;

  constructor(private options: TransactionQueueOptions = {}) {
    this.queue = new TransactionQueue(options.config);
  }

  async initialize(): Promise<void> {
    if (this.options.autoMigrate !== false) {
      await runMigrations();
    }
    this.started = true;
  }

  async enqueue(options: EnqueueOptions): Promise<Job> {
    return this.queue.enqueue(options);
  }

  async enqueueInTransaction(
    client: any,
    options: EnqueueOptions
  ): Promise<Job> {
    return this.queue.enqueueInTransaction(client, options);
  }

  async enqueueToOutbox(
    client: any,
    options: EnqueueOptions
  ): Promise<void> {
    return this.queue.enqueueToOutbox(client, options);
  }

  getQueue(): TransactionQueue {
    return this.queue;
  }

  registerHandler(type: string, handler: JobHandler): void {
    handlerRegistry.register(type, handler);
  }

  getHandlerRegistry() {
    return handlerRegistry;
  }

  async startWorker(options: WorkerOptions = {}): Promise<QueueWorker> {
    if (this.worker) {
      throw new Error('Worker already started');
    }

    // Per-worker config (e.g. baseBackoffMs) must reach the queue that
    // schedules retries, so build the worker's queue from the merged config.
    const config = { ...this.options.config, ...options.config };
    const queue = options.queue ?? new TransactionQueue(config);

    this.worker = new QueueWorker({
      ...options,
      queue,
      config,
    });

    await this.worker.start();
    return this.worker;
  }

  async stopWorker(graceful: boolean = true): Promise<void> {
    if (this.worker) {
      await this.worker.stop(graceful);
      this.worker = null;
    }
  }

  getWorker(): QueueWorker | null {
    return this.worker;
  }

  async publishOutbox(batchSize?: number): Promise<number> {
    return this.queue.publishOutbox(batchSize);
  }

  async reapExpiredLeases(): Promise<number> {
    return this.queue.reapExpiredLeases();
  }

  async getJob(jobId: string): Promise<Job | null> {
    return this.queue.getJob(jobId);
  }

  async getJobByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    return this.queue.getJobByIdempotencyKey(idempotencyKey);
  }

  async getStuckJobs(thresholdMinutes?: number): Promise<Job[]> {
    return this.queue.getStuckJobs(thresholdMinutes);
  }

  async shutdown(): Promise<void> {
    // The connection pool is shared process-wide; call closePool() yourself
    // when the application is exiting.
    await this.stopWorker(true);
    this.started = false;
  }

  isInitialized(): boolean {
    return this.started;
  }
}

export function createQueue(options: TransactionQueueOptions = {}): LeadflowQueue {
  return new LeadflowQueue(options);
}

export { DEFAULT_QUEUE_CONFIG };
export type { QueueConfig, EnqueueOptions, Job } from '../jobs/types.js';
export type { WorkerOptions } from './worker.js';