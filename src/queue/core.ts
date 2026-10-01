import { query, getClient } from '../database/client.js';
import {
  Job,
  EnqueueOptions,
  JobStatus,
  JobReceipt,
  OutboxEntry,
  DEFAULT_QUEUE_CONFIG,
  QueueConfig,
} from '../jobs/types.js';
import { v4 as uuidv4 } from 'uuid';

export { DEFAULT_QUEUE_CONFIG };

export class TransactionQueue {
  private config: QueueConfig;
  private workerId: string;

  constructor(config: Partial<QueueConfig> = {}) {
    this.config = { ...DEFAULT_QUEUE_CONFIG, ...config };
    this.workerId = `worker-${uuidv4().slice(0, 8)}`;
  }

  getConfig(): QueueConfig {
    return { ...this.config };
  }

  private mapRowToJob(row: any): Job {
    return {
      id: row.id.toString(),
      idempotencyKey: row.idempotency_key,
      type: row.type,
      payload: row.payload,
      status: row.status,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      runAt: new Date(row.run_at),
      lockedUntil: row.locked_until ? new Date(row.locked_until) : null,
      lockedBy: row.locked_by,
      lastError: row.last_error,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
    };
  }

  async enqueue(options: EnqueueOptions): Promise<Job> {
    const {
      idempotencyKey,
      type,
      payload,
      runAt = new Date(),
      maxAttempts = 6,
    } = options;

    const result = await query<Job>(
      `INSERT INTO jobs (idempotency_key, type, payload, run_at, max_attempts)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (idempotency_key) DO UPDATE SET
         type = EXCLUDED.type,
         payload = EXCLUDED.payload,
         run_at = EXCLUDED.run_at,
         max_attempts = EXCLUDED.max_attempts,
         status = 'pending',
         attempts = 0,
         locked_until = NULL,
         locked_by = NULL,
         last_error = NULL,
         updated_at = NOW()
       RETURNING *`,
      [idempotencyKey, type, JSON.stringify(payload), runAt, maxAttempts]
    );

    return this.mapRowToJob(result.rows[0]);
  }

  async enqueueInTransaction(
    client: any,
    options: EnqueueOptions
  ): Promise<Job> {
    const {
      idempotencyKey,
      type,
      payload,
      runAt = new Date(),
      maxAttempts = 6,
    } = options;

    const result = await (client as any).query(
      `INSERT INTO jobs (idempotency_key, type, payload, run_at, max_attempts)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (idempotency_key) DO UPDATE SET
         type = EXCLUDED.type,
         payload = EXCLUDED.payload,
         run_at = EXCLUDED.run_at,
         max_attempts = EXCLUDED.max_attempts,
         status = 'pending',
         attempts = 0,
         locked_until = NULL,
         locked_by = NULL,
         last_error = NULL,
         updated_at = NOW()
       RETURNING *`,
      [idempotencyKey, type, JSON.stringify(payload), runAt, maxAttempts]
    );

    return this.mapRowToJob(result.rows[0]);
  }

  async enqueueToOutbox(
    client: any,
    options: EnqueueOptions
  ): Promise<void> {
    const { idempotencyKey, type, payload, runAt = new Date(), maxAttempts = 6 } = options;
    
    const jobData = {
      idempotencyKey,
      type,
      payload,
      runAt: runAt.toISOString(),
      maxAttempts,
    };

    await client.query(
      `INSERT INTO outbox (job) VALUES ($1)`,
      [JSON.stringify(jobData)]
    );
  }

  async claimNextJob(): Promise<Job | null> {
    const leaseUntil = new Date(Date.now() + this.config.leaseDurationMs);
    
    const result = await query<Job>(
      `UPDATE jobs
       SET status = 'running',
           locked_until = $1,
           locked_by = $2,
           attempts = attempts + 1,
           updated_at = NOW()
       WHERE id = (
         SELECT id FROM jobs
         WHERE status = 'pending' AND run_at <= NOW()
         ORDER BY run_at
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      [leaseUntil, this.workerId]
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToJob(result.rows[0]);
  }

  async claimReceipt(jobId: string, idempotencyKey: string): Promise<boolean> {
    const result = await query<JobReceipt>(
      `INSERT INTO job_receipts (idempotency_key, job_id)
       VALUES ($1, $2)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING idempotency_key`,
      [idempotencyKey, jobId]
    );

    return result.rows.length > 0;
  }

  async completeJob(jobId: string): Promise<void> {
    await query(
      `UPDATE jobs
       SET status = 'done',
           locked_until = NULL,
           locked_by = NULL,
           updated_at = NOW()
       WHERE id = $1`,
      [jobId]
    );
  }

  async failJob(
    jobId: string,
    error: string,
    options: { permanent?: boolean } = {}
  ): Promise<void> {
    const jobResult = await query<Job>(
      `SELECT * FROM jobs WHERE id = $1`,
      [jobId]
    );

    if (jobResult.rows.length === 0) {
      throw new Error(`Job ${jobId} not found`);
    }

    const job = this.mapRowToJob(jobResult.rows[0]);
    // `attempts` is already incremented when the job is claimed, so it is the
    // number of attempts made so far (including the one that just failed).
    const isDead = options.permanent === true || job.attempts >= job.maxAttempts;

    if (isDead) {
      await query(
        `UPDATE jobs
         SET status = 'dead',
             last_error = $1,
             locked_until = NULL,
             locked_by = NULL,
             updated_at = NOW()
         WHERE id = $2`,
        [error, jobId]
      );
    } else {
      const runAt = this.calculateNextRunAt(job.attempts);

      await query(
        `UPDATE jobs
         SET status = 'pending',
             run_at = $1,
             locked_until = NULL,
             locked_by = NULL,
             last_error = $2,
             updated_at = NOW()
         WHERE id = $3`,
        [runAt, error, jobId]
      );
    }
  }

  async releaseLease(jobId: string): Promise<void> {
    await query(
      `UPDATE jobs
       SET status = 'pending',
           locked_until = NULL,
           locked_by = NULL,
           updated_at = NOW()
       WHERE id = $1 AND locked_by = $2`,
      [jobId, this.workerId]
    );
  }

  async extendLease(jobId: string, additionalMs: number = 30000): Promise<boolean> {
    const newLeaseUntil = new Date(Date.now() + additionalMs);
    
    const result = await query(
      `UPDATE jobs
       SET locked_until = $1,
           updated_at = NOW()
       WHERE id = $2 AND locked_by = $3 AND status = 'running'
       RETURNING id`,
      [newLeaseUntil, jobId, this.workerId]
    );

    return result.rows.length > 0;
  }

  async reapExpiredLeases(): Promise<number> {
    const result = await query(
      `UPDATE jobs
       SET status = 'pending',
           locked_until = NULL,
           locked_by = NULL,
           updated_at = NOW()
       WHERE status = 'running' AND locked_until < NOW()
       RETURNING id`
    );

    return result.rowCount || 0;
  }

  async getJob(jobId: string): Promise<Job | null> {
    const result = await query<Job>(
      `SELECT * FROM jobs WHERE id = $1`,
      [jobId]
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToJob(result.rows[0]);
  }

  async getJobByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    const result = await query<Job>(
      `SELECT * FROM jobs WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToJob(result.rows[0]);
  }

  /**
   * Jobs that hold a receipt (the handler may have run) but never reached
   * 'done' and whose lease has expired, i.e. the worker died mid-flight.
   * These need inspection since re-running them could double-send.
   *
   * The receipt's completed_at is the time the receipt was written, which is
   * not a useful age cutoff (a just-crashed job has a fresh receipt), so
   * `_thresholdMinutes` is accepted for API compatibility but not applied.
   */
  async getStuckJobs(_thresholdMinutes: number = 10): Promise<Job[]> {
    const result = await query<Job>(
      `SELECT j.* FROM jobs j
       JOIN job_receipts r ON j.id = r.job_id
       WHERE j.status = 'running'
         AND j.locked_until < NOW()`
    );

    return result.rows.map((row) => this.mapRowToJob(row));
  }

  async publishOutbox(batchSize: number = 100): Promise<number> {
    const client = await getClient();
    let published = 0;

    try {
      await client.query('BEGIN');

      const result = await client.query<OutboxEntry>(
        `SELECT * FROM outbox
         WHERE published_at IS NULL
         ORDER BY created_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [batchSize]
      );

      for (const row of result.rows) {
        const jobData = row.job;
        await this.enqueueInTransaction(client, {
          idempotencyKey: jobData.idempotencyKey,
          type: jobData.type,
          payload: jobData.payload,
          runAt: new Date(jobData.runAt),
          maxAttempts: jobData.maxAttempts,
        });

        await client.query(
          `UPDATE outbox SET published_at = NOW() WHERE id = $1`,
          [row.id]
        );
        published++;
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    return published;
  }

  private calculateNextRunAt(attempt: number): Date {
    const base = this.config.baseBackoffMs;
    const maxBackoff = this.config.maxBackoffMs;
    // Jitter is at most one base interval (README: random(0, base)), so a
    // small base backoff isn't swamped by the default 5s jitter.
    const jitter = Math.min(this.config.jitterMs, base);
    
    const exponentialBackoff = Math.min(base * Math.pow(2, attempt - 1), maxBackoff);
    const jitterAmount = Math.random() * jitter;
    
    return new Date(Date.now() + exponentialBackoff + jitterAmount);
  }

  getWorkerId(): string {
    return this.workerId;
  }
}