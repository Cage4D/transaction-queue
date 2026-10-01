import { describe, it, expect, beforeEach } from 'vitest';
import { TransactionQueue } from '../src/queue/core.js';
import { getClient, query } from '../src/database/client.js';

describe('Outbox Pattern', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue();
  });

  it('should enqueue job to outbox within transaction', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueToOutbox(client, {
        idempotencyKey: 'outbox-key-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const outboxResult = await query('SELECT * FROM outbox WHERE job->>\'idempotencyKey\' = $1', ['outbox-key-1']);
    expect(outboxResult.rows.length).toBe(1);
    expect(outboxResult.rows[0].published_at).toBeNull();
  });

  it('should publish outbox entries to jobs table', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueToOutbox(client, {
        idempotencyKey: 'outbox-publish-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const published = await queue.publishOutbox(10);
    expect(published).toBe(1);

    const job = await queue.getJobByIdempotencyKey('outbox-publish-1');
    expect(job).not.toBeNull();
    expect(job?.type).toBe('email');
    expect(job?.payload.to).toBe('test@example.com');
    expect(job?.status).toBe('pending');
  });

  it('should mark outbox entry as published', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueToOutbox(client, {
        idempotencyKey: 'outbox-marked-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    await queue.publishOutbox(10);

    const outboxResult = await query('SELECT * FROM outbox WHERE job->>\'idempotencyKey\' = $1', ['outbox-marked-1']);
    expect(outboxResult.rows[0].published_at).not.toBeNull();
  });

  it('should handle multiple outbox entries in batch', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      for (let i = 0; i < 5; i++) {
        await queue.enqueueToOutbox(client, {
          idempotencyKey: `outbox-batch-${i}`,
          type: 'email',
          payload: { index: i },
        });
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const published = await queue.publishOutbox(10);
    expect(published).toBe(5);

    for (let i = 0; i < 5; i++) {
      const job = await queue.getJobByIdempotencyKey(`outbox-batch-${i}`);
      expect(job).not.toBeNull();
      expect(job?.payload.index).toBe(i);
    }
  });

  it('should enqueue directly in same transaction as business logic', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      const job = await queue.enqueueInTransaction(client, {
        idempotencyKey: 'tx-enqueue-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      await client.query('COMMIT');
      
      expect(job.status).toBe('pending');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const retrieved = await queue.getJobByIdempotencyKey('tx-enqueue-1');
    expect(retrieved).not.toBeNull();
    expect(retrieved?.status).toBe('pending');
  });

  it('should rollback enqueue on transaction failure', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueInTransaction(client, {
        idempotencyKey: 'tx-rollback-1',
        type: 'email',
        payload: { to: 'test@example.com' },
      });

      throw new Error('Simulated failure');
    } catch (error) {
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }

    const retrieved = await queue.getJobByIdempotencyKey('tx-rollback-1');
    expect(retrieved).toBeNull();
  });
});

describe('Reaper - Expired Leases', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue({ leaseDurationMs: 100 });
  });

  it('should reap expired leases', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'reaper-key-1',
      type: 'email',
      payload: {},
    });

    await queue.claimNextJob();

    await new Promise(resolve => setTimeout(resolve, 200));

    const reaped = await queue.reapExpiredLeases();
    expect(reaped).toBe(1);

    const reapedJob = await queue.getJob(job.id);
    expect(reapedJob?.status).toBe('pending');
    expect(reapedJob?.lockedUntil).toBeNull();
    expect(reapedJob?.lockedBy).toBeNull();
  });

  it('should not reap active leases', async () => {
    const job = await queue.enqueue({
      idempotencyKey: 'reaper-active-1',
      type: 'email',
      payload: {},
    });

    await queue.claimNextJob();

    const reaped = await queue.reapExpiredLeases();
    expect(reaped).toBe(0);

    const activeJob = await queue.getJob(job.id);
    expect(activeJob?.status).toBe('running');
  });
});

describe('Stuck Job Detection', () => {
  let queue: TransactionQueue;

  beforeEach(() => {
    queue = new TransactionQueue();
  });

  it('should detect stuck jobs with receipts but no completion', async () => {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
      const job = await queue.enqueueInTransaction(client, {
        idempotencyKey: 'stuck-key-1',
        type: 'email',
        payload: {},
      });

      await client.query(
        `UPDATE jobs SET status = 'running', locked_until = NOW() - INTERVAL '5 minutes', locked_by = 'dead-worker' WHERE id = $1`,
        [job.id]
      );

      await client.query(
        `INSERT INTO job_receipts (idempotency_key, job_id) VALUES ($1, $2)`,
        [job.idempotencyKey, job.id]
      );

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    const stuckJobs = await queue.getStuckJobs(10);
    expect(stuckJobs.length).toBeGreaterThanOrEqual(1);
    
    const stuckJob = stuckJobs.find(j => j.idempotencyKey === 'stuck-key-1');
    expect(stuckJob).toBeDefined();
    expect(stuckJob?.status).toBe('running');
  });
});