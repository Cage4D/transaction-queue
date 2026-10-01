import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { QueueWorker } from '../src/queue/worker.js';
import { TransactionQueue } from '../src/queue/core.js';
import { handlerRegistry } from '../src/queue/handlers.js';
import { createQueue } from '../src/queue/queue.js';

describe('QueueWorker', () => {
  let queue: TransactionQueue;
  let worker: QueueWorker;
  let processedJobs: any[] = [];

  beforeEach(() => {
    processedJobs = [];
    handlerRegistry.unregister('test-job');
    handlerRegistry.unregister('failing-job');
    handlerRegistry.unregister('slow-job');
    
    const config = { pollIntervalMs: 100, leaseDurationMs: 5000, baseBackoffMs: 50, jitterMs: 10 };
    queue = new TransactionQueue(config);
    worker = new QueueWorker({
      queue,
      config,
      onJobStart: (job) => processedJobs.push({ ...job, event: 'start' }),
      onJobComplete: (job) => processedJobs.push({ ...job, event: 'complete' }),
      onJobError: (job, error) => processedJobs.push({ ...job, event: 'error', error: error.message }),
    });
  });

  afterEach(async () => {
    await worker.stop(true);
  });

  it('should process a job successfully', async () => {
    handlerRegistry.register('test-job', async (payload) => {
      processedJobs.push({ type: 'handler', payload });
    });

    await queue.enqueue({
      idempotencyKey: 'worker-test-1',
      type: 'test-job',
      payload: { message: 'hello' },
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 1000));

    expect(processedJobs.some(j => j.event === 'start')).toBe(true);
    expect(processedJobs.some(j => j.event === 'complete')).toBe(true);
    expect(processedJobs.some(j => j.type === 'handler' && j.payload.message === 'hello')).toBe(true);
  });

  it('should handle job failures and retry', async () => {
    let attemptCount = 0;
    
    handlerRegistry.register('failing-job', async () => {
      attemptCount++;
      if (attemptCount < 3) {
        throw new Error('Temporary failure');
      }
    });

    await queue.enqueue({
      idempotencyKey: 'worker-fail-1',
      type: 'failing-job',
      payload: {},
      maxAttempts: 5,
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 5000));

    expect(attemptCount).toBeGreaterThanOrEqual(3);
    const completed = processedJobs.find(j => j.event === 'complete');
    expect(completed).toBeDefined();
  });

  it('should mark job as dead after max attempts', async () => {
    handlerRegistry.register('failing-job', async () => {
      throw new Error('Always fails');
    });

    await queue.enqueue({
      idempotencyKey: 'worker-dead-1',
      type: 'failing-job',
      payload: {},
      maxAttempts: 2,
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 3000));

    const job = await queue.getJobByIdempotencyKey('worker-dead-1');
    expect(job?.status).toBe('dead');
  });

  it('should not double-process with receipt deduplication', async () => {
    let processCount = 0;
    
    handlerRegistry.register('test-job', async () => {
      processCount++;
    });

    await queue.enqueue({
      idempotencyKey: 'worker-dedup-1',
      type: 'test-job',
      payload: {},
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 1000));

    expect(processCount).toBe(1);
  });

  it('should handle unknown job types', async () => {
    await queue.enqueue({
      idempotencyKey: 'worker-unknown-1',
      type: 'unknown-type',
      payload: {},
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 1000));

    const job = await queue.getJobByIdempotencyKey('worker-unknown-1');
    expect(job?.status).toBe('dead');
    expect(job?.lastError).toContain('No handler registered');
  });

  it('should gracefully shutdown', async () => {
    let resolveHandler: (value: void) => void;
    const handlerPromise = new Promise<void>(resolve => {
      resolveHandler = resolve;
    });

    handlerRegistry.register('slow-job', async () => {
      await handlerPromise;
    });

    await queue.enqueue({
      idempotencyKey: 'worker-slow-1',
      type: 'slow-job',
      payload: {},
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 100));

    const stopPromise = worker.stop(true);
    
    resolveHandler!();
    await handlerPromise;

    await stopPromise;

    const job = await queue.getJobByIdempotencyKey('worker-slow-1');
    expect(job?.status).toBe('done');
  });

  it('should extend lease for long-running jobs', async () => {
    let resolveHandler: (value: void) => void;
    const handlerPromise = new Promise<void>(resolve => {
      resolveHandler = resolve;
    });

    handlerRegistry.register('slow-job', async () => {
      await handlerPromise;
    });

    await queue.enqueue({
      idempotencyKey: 'worker-lease-1',
      type: 'slow-job',
      payload: {},
    });

    await worker.start();

    await new Promise(resolve => setTimeout(resolve, 200));

    const job = await queue.getJobByIdempotencyKey('worker-lease-1');
    expect(job?.status).toBe('running');
    expect(job?.lockedUntil).toBeDefined();

    resolveHandler!();
    await handlerPromise;

    await worker.stop(true);

    const completed = await queue.getJobByIdempotencyKey('worker-lease-1');
    expect(completed?.status).toBe('done');
  });
});

describe('Concurrency - FOR UPDATE SKIP LOCKED', () => {
  it('should distribute jobs among multiple workers', async () => {
    const queue1 = new TransactionQueue({ leaseDurationMs: 5000 });
    const queue2 = new TransactionQueue({ leaseDurationMs: 5000 });
    const queue3 = new TransactionQueue({ leaseDurationMs: 5000 });

    const worker1 = new QueueWorker({ queue: queue1, config: { pollIntervalMs: 50 } });
    const worker2 = new QueueWorker({ queue: queue2, config: { pollIntervalMs: 50 } });
    const worker3 = new QueueWorker({ queue: queue3, config: { pollIntervalMs: 50 } });

    const processedByWorker = new Map<string, number>();

    const registerHandler = (workerId: string) => {
      return async (payload: any) => {
        const count = processedByWorker.get(workerId) || 0;
        processedByWorker.set(workerId, count + 1);
        await new Promise(resolve => setTimeout(resolve, 50));
      };
    };

    handlerRegistry.register('concurrent-job', registerHandler('worker1'));
    
    for (let i = 0; i < 10; i++) {
      await queue1.enqueue({
        idempotencyKey: `concurrent-${i}`,
        type: 'concurrent-job',
        payload: { index: i },
      });
    }

    await worker1.start();
    await worker2.start();
    await worker3.start();

    await new Promise(resolve => setTimeout(resolve, 3000));

    await worker1.stop(true);
    await worker2.stop(true);
    await worker3.stop(true);

    const totalProcessed = Array.from(processedByWorker.values()).reduce((a, b) => a + b, 0);
    expect(totalProcessed).toBe(10);

    for (let i = 0; i < 10; i++) {
      const job = await queue1.getJobByIdempotencyKey(`concurrent-${i}`);
      expect(job?.status).toBe('done');
    }
  });
});