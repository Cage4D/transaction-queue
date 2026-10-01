import { describe, it, expect, beforeEach } from 'vitest';
import { TransactionQueue, DEFAULT_QUEUE_CONFIG } from '../src/queue/core.js';
import { QueueConfig } from '../src/jobs/types.js';
import { query } from '../src/database/client.js';

describe('Exponential Backoff with Jitter', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue({ 
      baseBackoffMs: 5000,
      maxBackoffMs: 15 * 60 * 1000,
      jitterMs: 5000,
    });
  });

  it('should calculate exponential backoff correctly', () => {
    const queue = new TransactionQueue({ 
      baseBackoffMs: 5000,
      maxBackoffMs: 15 * 60 * 1000,
      jitterMs: 5000,
    });

    const attempt1 = queue['calculateNextRunAt'](1);
    const attempt2 = queue['calculateNextRunAt'](2);
    const attempt3 = queue['calculateNextRunAt'](3);
    const attempt4 = queue['calculateNextRunAt'](4);
    const attempt5 = queue['calculateNextRunAt'](5);
    const attempt6 = queue['calculateNextRunAt'](6);

    const now = Date.now();
    
    expect(attempt1.getTime() - now).toBeGreaterThanOrEqual(5000);
    expect(attempt1.getTime() - now).toBeLessThanOrEqual(10000);
    
    expect(attempt2.getTime() - now).toBeGreaterThanOrEqual(10000);
    expect(attempt2.getTime() - now).toBeLessThanOrEqual(15000);
    
    expect(attempt3.getTime() - now).toBeGreaterThanOrEqual(20000);
    expect(attempt3.getTime() - now).toBeLessThanOrEqual(25000);
    
    expect(attempt4.getTime() - now).toBeGreaterThanOrEqual(40000);
    expect(attempt4.getTime() - now).toBeLessThanOrEqual(45000);
    
    expect(attempt5.getTime() - now).toBeGreaterThanOrEqual(80000);
    expect(attempt5.getTime() - now).toBeLessThanOrEqual(85000);
    
    expect(attempt6.getTime() - now).toBeGreaterThanOrEqual(160000);
    expect(attempt6.getTime() - now).toBeLessThanOrEqual(165000);
  });

  it('should cap at maxBackoffMs', () => {
    const queue = new TransactionQueue({ 
      baseBackoffMs: 5000,
      maxBackoffMs: 30000,
      jitterMs: 5000,
    });

    const attempt10 = queue['calculateNextRunAt'](10);
    const attempt20 = queue['calculateNextRunAt'](20);

    const now = Date.now();
    
    expect(attempt10.getTime() - now).toBeLessThanOrEqual(35000);
    expect(attempt20.getTime() - now).toBeLessThanOrEqual(35000);
  });

  it('should add jitter to prevent thundering herd', () => {
    const queue = new TransactionQueue({ 
      baseBackoffMs: 5000,
      maxBackoffMs: 15 * 60 * 1000,
      jitterMs: 5000,
    });

    const times = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const runAt = queue['calculateNextRunAt'](3);
      times.add(runAt.getTime());
    }

    expect(times.size).toBeGreaterThan(50);
  });
});

describe('Job Status Transitions', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue();
  });

  it('should transition: pending -> running -> done', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'status-transition-1',
      type: 'test',
      payload: {},
    });

    expect(job.status).toBe('pending');

    const claimed = await queue.claimNextJob();
    expect(claimed?.status).toBe('running');
    expect(claimed?.attempts).toBe(1);

    await queue.completeJob(job.id);

    const completed = await queue.getJob(job.id);
    expect(completed?.status).toBe('done');
  });

  it('should transition: pending -> running -> pending (retry) -> running -> done', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'status-retry-1',
      type: 'test',
      payload: {},
      maxAttempts: 3,
    });

    await queue.claimNextJob();
    await queue.failJob(job.id, 'Error 1');

    let retried = await queue.getJob(job.id);
    expect(retried?.status).toBe('pending');
    expect(retried?.attempts).toBe(1);
    expect(retried?.lastError).toBe('Error 1');

    await queue.claimNextJob();
    await queue.completeJob(job.id);

    const completed = await queue.getJob(job.id);
    expect(completed?.status).toBe('done');
  });

  it('should transition to dead after max attempts', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'status-dead-1',
      type: 'test',
      payload: {},
      maxAttempts: 2,
    });

    await queue.claimNextJob();
    await queue.failJob(job.id, 'Error 1');

    let retried = await queue.getJob(job.id);
    expect(retried?.status).toBe('pending');
    expect(retried?.attempts).toBe(1);

    // Skip the backoff delay so the retry can be claimed right away
    await query(`UPDATE jobs SET run_at = NOW() WHERE id = $1`, [job.id]);

    await queue.claimNextJob();
    await queue.failJob(job.id, 'Error 2');

    const dead = await queue.getJob(job.id);
    expect(dead?.status).toBe('dead');
    expect(dead?.attempts).toBe(2);
  });

  it('should return dead status after max attempts reached', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'status-dead-2',
      type: 'test',
      payload: {},
      maxAttempts: 1,
    });

    await queue.claimNextJob();
    await queue.failJob(job.id, 'Error 1');

    const dead = await queue.getJob(job.id);
    expect(dead?.status).toBe('dead');
    expect(dead?.attempts).toBe(1);
  });

  it('should release lease and return to pending', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'status-release-1',
      type: 'test',
      payload: {},
    });

    await queue.claimNextJob();
    await queue.releaseLease(job.id);

    const released = await queue.getJob(job.id);
    expect(released?.status).toBe('pending');
    expect(released?.lockedUntil).toBeNull();
    expect(released?.lockedBy).toBeNull();
    expect(released?.attempts).toBe(1);
  });
});

describe('Configuration', () => {
  it('should use default config when none provided', () => {
    const queue = new TransactionQueue();
    const config = queue.getConfig();
    
    expect(config.leaseDurationMs).toBe(DEFAULT_QUEUE_CONFIG.leaseDurationMs);
    expect(config.pollIntervalMs).toBe(DEFAULT_QUEUE_CONFIG.pollIntervalMs);
    expect(config.baseBackoffMs).toBe(DEFAULT_QUEUE_CONFIG.baseBackoffMs);
    expect(config.maxBackoffMs).toBe(DEFAULT_QUEUE_CONFIG.maxBackoffMs);
    expect(config.jitterMs).toBe(DEFAULT_QUEUE_CONFIG.jitterMs);
  });

  it('should merge custom config with defaults', () => {
    const customConfig: Partial<QueueConfig> = {
      leaseDurationMs: 60000,
      pollIntervalMs: 500,
      baseBackoffMs: 10000,
    };
    
    const queue = new TransactionQueue(customConfig);
    const config = queue.getConfig();
    
    expect(config.leaseDurationMs).toBe(60000);
    expect(config.pollIntervalMs).toBe(500);
    expect(config.baseBackoffMs).toBe(10000);
    expect(config.maxBackoffMs).toBe(DEFAULT_QUEUE_CONFIG.maxBackoffMs);
    expect(config.jitterMs).toBe(DEFAULT_QUEUE_CONFIG.jitterMs);
  });
});