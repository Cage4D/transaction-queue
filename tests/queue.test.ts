import { describe, it, expect, beforeEach } from 'vitest';
import { LeadflowQueue, createQueue } from '../src/queue/queue.js';
import { TransactionQueue } from '../src/queue/core.js';

describe('TransactionQueue - Core Operations', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue();
  });

  describe('enqueue', () => {
    it('should enqueue a new job', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'test-key-1',
        type: 'email',
        payload: { to: 'test@example.com', subject: 'Test' },
      });

      expect(job).toBeDefined();
      expect(job.idempotencyKey).toBe('test-key-1');
      expect(job.type).toBe('email');
      expect(job.payload).toEqual({ to: 'test@example.com', subject: 'Test' });
      expect(job.status).toBe('pending');
      expect(job.attempts).toBe(0);
      expect(job.maxAttempts).toBe(6);
    });

    it('should handle duplicate idempotency key', async () => {
      await queue.enqueue({
        idempotencyKey: 'duplicate-key',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      const job = await queue.enqueue({
        idempotencyKey: 'duplicate-key',
        type: 'email',
        payload: { to: 'updated@example.com' },
      });

      expect(job.payload.to).toBe('updated@example.com');
      expect(job.attempts).toBe(0);
      expect(job.status).toBe('pending');
    });

    it('should respect runAt scheduling', async () => {
      const futureTime = new Date(Date.now() + 60000);
      const job = await queue.enqueue({
        idempotencyKey: 'scheduled-key',
        type: 'email',
        payload: {},
        runAt: futureTime,
      });

      expect(job.runAt.getTime()).toBeCloseTo(futureTime.getTime(), -2);
    });

    it('should respect maxAttempts', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'max-attempts-key',
        type: 'email',
        payload: {},
        maxAttempts: 3,
      });

      expect(job.maxAttempts).toBe(3);
    });
  });

  describe('claimNextJob', () => {
    it('should claim a pending job', async () => {
      await queue.enqueue({
        idempotencyKey: 'claim-key-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      const job = await queue.claimNextJob();

      expect(job).not.toBeNull();
      expect(job?.status).toBe('running');
      expect(job?.lockedBy).toBeDefined();
      expect(job?.lockedUntil).toBeDefined();
      expect(job?.attempts).toBe(1);
    });

    it('should return null when no jobs available', async () => {
      const job = await queue.claimNextJob();
      expect(job).toBeNull();
    });

    it('should not claim jobs with future runAt', async () => {
      const futureTime = new Date(Date.now() + 60000);
      await queue.enqueue({
        idempotencyKey: 'future-key',
        type: 'email',
        payload: {},
        runAt: futureTime,
      });

      const job = await queue.claimNextJob();
      expect(job).toBeNull();
    });

    it('should claim jobs in FIFO order by runAt', async () => {
      const now = new Date();
      const later = new Date(Date.now() + 1000);

      await queue.enqueue({
        idempotencyKey: 'later-key',
        type: 'email',
        payload: { order: 2 },
        runAt: later,
      });

      await queue.enqueue({
        idempotencyKey: 'first-key',
        type: 'email',
        payload: { order: 1 },
        runAt: now,
      });

      const firstJob = await queue.claimNextJob();
      expect(firstJob?.payload.order).toBe(1);

      // The second job is scheduled in the future, so it isn't claimable yet
      expect(await queue.claimNextJob()).toBeNull();

      await new Promise(resolve => setTimeout(resolve, 1100));

      const secondJob = await queue.claimNextJob();
      expect(secondJob?.payload.order).toBe(2);
    });
  });

  describe('claimReceipt', () => {
    it('should claim receipt for first time', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'receipt-key',
        type: 'email',
        payload: {},
      });

      const claimed = await queue.claimReceipt(job.id, job.idempotencyKey);
      expect(claimed).toBe(true);
    });

    it('should reject duplicate receipt claim', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'duplicate-receipt',
        type: 'email',
        payload: {},
      });

      await queue.claimReceipt(job.id, job.idempotencyKey);
      const claimed = await queue.claimReceipt(job.id, job.idempotencyKey);
      
      expect(claimed).toBe(false);
    });
  });

  describe('completeJob', () => {
    it('should mark job as done', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'complete-key',
        type: 'email',
        payload: {},
      });

      await queue.claimNextJob();
      await queue.completeJob(job.id);

      const completed = await queue.getJob(job.id);
      expect(completed?.status).toBe('done');
      expect(completed?.lockedUntil).toBeNull();
      expect(completed?.lockedBy).toBeNull();
    });
  });

  describe('failJob', () => {
    it('should schedule retry for failed job', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'fail-key',
        type: 'email',
        payload: {},
        maxAttempts: 3,
      });

      await queue.claimNextJob();
      await queue.failJob(job.id, 'Test error');

      const failed = await queue.getJob(job.id);
      expect(failed?.status).toBe('pending');
      expect(failed?.attempts).toBe(1);
      expect(failed?.lastError).toBe('Test error');
      expect(failed?.runAt.getTime()).toBeGreaterThan(Date.now());
    });

    it('should mark job as dead after max attempts', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'dead-key',
        type: 'email',
        payload: {},
        maxAttempts: 1,
      });

      await queue.claimNextJob();
      await queue.failJob(job.id, 'Test error');

      const dead = await queue.getJob(job.id);
      expect(dead?.status).toBe('dead');
      expect(dead?.lastError).toBe('Test error');
    });
  });

  describe('releaseLease', () => {
    it('should release lease and requeue job', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'release-key',
        type: 'email',
        payload: {},
      });

      await queue.claimNextJob();
      await queue.releaseLease(job.id);

      const released = await queue.getJob(job.id);
      expect(released?.status).toBe('pending');
      expect(released?.lockedUntil).toBeNull();
      expect(released?.lockedBy).toBeNull();
    });
  });

  describe('extendLease', () => {
    it('should extend lease for running job', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'extend-key',
        type: 'email',
        payload: {},
      });

      await queue.claimNextJob();
      const originalLease = await queue.getJob(job.id);
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const extended = await queue.extendLease(job.id, 60000);
      expect(extended).toBe(true);

      const updated = await queue.getJob(job.id);
      expect(updated?.lockedUntil!.getTime()).toBeGreaterThan(originalLease!.lockedUntil!.getTime());
    });

    it('should not extend lease for another worker', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'extend-other-key',
        type: 'email',
        payload: {},
      });

      await queue.claimNextJob();
      const otherQueue = new TransactionQueue();
      
      const extended = await otherQueue.extendLease(job.id, 60000);
      expect(extended).toBe(false);
    });
  });

  describe('getJob', () => {
    it('should retrieve job by id', async () => {
      const job = await queue.enqueue({
        idempotencyKey: 'get-key',
        type: 'email',
        payload: { foo: 'bar' },
      });

      const retrieved = await queue.getJob(job.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.idempotencyKey).toBe('get-key');
      expect(retrieved?.payload).toEqual({ foo: 'bar' });
    });

    it('should return null for non-existent job', async () => {
      const retrieved = await queue.getJob('999999');
      expect(retrieved).toBeNull();
    });
  });

  describe('getJobByIdempotencyKey', () => {
    it('should retrieve job by idempotency key', async () => {
      await queue.enqueue({
        idempotencyKey: 'idem-get-key',
        type: 'email',
        payload: { foo: 'bar' },
      });

      const retrieved = await queue.getJobByIdempotencyKey('idem-get-key');
      expect(retrieved).not.toBeNull();
      expect(retrieved?.idempotencyKey).toBe('idem-get-key');
    });
  });
});

describe('LeadflowQueue', () => {
  let queue: LeadflowQueue;

  beforeEach(() => {
    queue = createQueue({ autoMigrate: false });
  });

  it('should initialize and enqueue', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'leadflow-key',
      type: 'email',
      payload: { test: true },
    });

    expect(job).toBeDefined();
    expect(job.idempotencyKey).toBe('leadflow-key');
  });

  it('should register and retrieve handlers', async () => {
    const handler = async (payload: any) => { console.log(payload); };
    queue.registerHandler('test-type', handler);
    
    const retrieved = queue.getHandlerRegistry().get('test-type');
    expect(retrieved).toBe(handler);
  });
});