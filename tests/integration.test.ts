import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import { createQueue, LeadflowQueue } from '../src/queue/queue.js';
import { handlerRegistry } from '../src/queue/handlers.js';

describe('Integration - Full Job Lifecycle', () => {
  let queue: LeadflowQueue;
  let processed: any[] = [];

  beforeEach(() => {
    processed = [];
    handlerRegistry.unregister('send-email');
    handlerRegistry.unregister('process-payment');
    handlerRegistry.unregister('send-notification');
    
    queue = createQueue({ autoMigrate: false });
  });

  afterAll(async () => {
    await queue.shutdown();
  });

  it('should process email job end-to-end', async () => {
    handlerRegistry.register('send-email', async (payload) => {
      processed.push({ type: 'email', to: payload.to });
    });

    await queue.enqueue({
      idempotencyKey: 'integration-email-1',
      type: 'send-email',
      payload: { to: 'user@example.com', subject: 'Welcome!' },
    });

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 500));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('integration-email-1');
    expect(job?.status).toBe('done');
    expect(processed).toContainEqual({ type: 'email', to: 'user@example.com' });
  });

  it('should process multiple job types', async () => {
    handlerRegistry.register('send-email', async (payload) => {
      processed.push({ type: 'email', to: payload.to });
    });

    handlerRegistry.register('process-payment', async (payload) => {
      processed.push({ type: 'payment', amount: payload.amount });
    });

    await queue.enqueue({
      idempotencyKey: 'integration-multi-1',
      type: 'send-email',
      payload: { to: 'user@example.com' },
    });

    await queue.enqueue({
      idempotencyKey: 'integration-multi-2',
      type: 'process-payment',
      payload: { amount: 100, currency: 'USD' },
    });

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 1000));

    await queue.stopWorker();

    const emailJob = await queue.getJobByIdempotencyKey('integration-multi-1');
    const paymentJob = await queue.getJobByIdempotencyKey('integration-multi-2');

    expect(emailJob?.status).toBe('done');
    expect(paymentJob?.status).toBe('done');
    expect(processed.length).toBe(2);
  });

  it('should handle job with custom maxAttempts', async () => {
    let attempts = 0;
    
    handlerRegistry.register('flaky-job', async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error('Flaky error');
      }
    });

    await queue.enqueue({
      idempotencyKey: 'integration-flaky-1',
      type: 'flaky-job',
      payload: {},
      maxAttempts: 5,
    });

    await queue.startWorker({ config: { pollIntervalMs: 50, baseBackoffMs: 100 } });

    await new Promise(resolve => setTimeout(resolve, 2000));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('integration-flaky-1');
    expect(job?.status).toBe('done');
    expect(attempts).toBe(3);
  });

  it('should support scheduled jobs (runAt in future)', async () => {
    let executed = false;
    
    handlerRegistry.register('scheduled-job', async () => {
      executed = true;
    });

    const futureTime = new Date(Date.now() + 200);
    
    await queue.enqueue({
      idempotencyKey: 'integration-scheduled-1',
      type: 'scheduled-job',
      payload: {},
      runAt: futureTime,
    });

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 150));
    expect(executed).toBe(false);

    await new Promise(resolve => setTimeout(resolve, 200));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('integration-scheduled-1');
    expect(job?.status).toBe('done');
    expect(executed).toBe(true);
  });
});

describe('Integration - Transactional Enqueue', () => {
  let queue: LeadflowQueue;
  let processed: any[] = [];

  beforeEach(() => {
    processed = [];
    handlerRegistry.unregister('order-created');
    queue = createQueue({ autoMigrate: false });
  });

  afterAll(async () => {
    await queue.shutdown();
  });

  it('should enqueue job in same transaction as business logic', async () => {
    handlerRegistry.register('order-created', async (payload) => {
      processed.push(payload);
    });

    const { pool } = await import('../src/database/client.js');
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueInTransaction(client, {
        idempotencyKey: 'tx-order-1',
        type: 'order-created',
        payload: { orderId: 'order-123', amount: 99.99 },
      });

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 500));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('tx-order-1');
    expect(job?.status).toBe('done');
    expect(processed.length).toBe(1);
    expect(processed[0]).toEqual({ orderId: 'order-123', amount: 99.99 });
  });

  it('should rollback enqueue when transaction fails', async () => {
    handlerRegistry.register('order-created', async (payload) => {
      processed.push(payload);
    });

    const { pool } = await import('../src/database/client.js');
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueInTransaction(client, {
        idempotencyKey: 'tx-rollback-1',
        type: 'order-created',
        payload: { orderId: 'order-456' },
      });

      throw new Error('Simulated DB error');
    } catch (error) {
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }

    const job = await queue.getJobByIdempotencyKey('tx-rollback-1');
    expect(job).toBeNull();
    expect(processed.length).toBe(0);
  });
});

describe('Integration - Outbox Pattern', () => {
  let queue: LeadflowQueue;
  let processed: any[] = [];

  beforeEach(() => {
    processed = [];
    handlerRegistry.unregister('outbox-job');
    queue = createQueue({ autoMigrate: false });
  });

  afterAll(async () => {
    await queue.shutdown();
  });

  it('should move jobs from outbox to queue', async () => {
    handlerRegistry.register('outbox-job', async (payload) => {
      processed.push(payload);
    });

    const { pool } = await import('../src/database/client.js');
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      await queue.enqueueToOutbox(client, {
        idempotencyKey: 'outbox-integration-1',
        type: 'outbox-job',
        payload: { data: 'from-outbox' },
      });

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    await queue.publishOutbox();

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 500));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('outbox-integration-1');
    expect(job?.status).toBe('done');
    expect(processed.length).toBe(1);
    expect(processed[0]).toEqual({ data: 'from-outbox' });
  });
});

describe('Integration - Dead Letter / Failed Jobs', () => {
  let queue: LeadflowQueue;

  beforeEach(() => {
    handlerRegistry.unregister('always-fails');
    queue = createQueue({ autoMigrate: false });
  });

  afterAll(async () => {
    await queue.shutdown();
  });

  it('should move poison messages to dead status', async () => {
    handlerRegistry.register('always-fails', async () => {
      throw new Error('Permanent failure');
    });

    await queue.enqueue({
      idempotencyKey: 'dead-letter-1',
      type: 'always-fails',
      payload: {},
      maxAttempts: 2,
    });

    await queue.startWorker({ config: { pollIntervalMs: 50, baseBackoffMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 2000));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('dead-letter-1');
    expect(job?.status).toBe('dead');
    expect(job?.lastError).toBe('Permanent failure');
    expect(job?.attempts).toBe(2);
  });

  it('should keep dead jobs visible for inspection', async () => {
    handlerRegistry.register('always-fails', async () => {
      throw new Error('Permanent failure');
    });

    await queue.enqueue({
      idempotencyKey: 'dead-letter-2',
      type: 'always-fails',
      payload: { important: 'data' },
      maxAttempts: 1,
    });

    await queue.startWorker({ config: { pollIntervalMs: 50 } });

    await new Promise(resolve => setTimeout(resolve, 1000));

    await queue.stopWorker();

    const job = await queue.getJobByIdempotencyKey('dead-letter-2');
    expect(job).not.toBeNull();
    expect(job?.status).toBe('dead');
    expect(job?.payload).toEqual({ important: 'data' });
    expect(job?.lastError).toBe('Permanent failure');
  });
});