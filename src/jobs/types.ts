export type JobStatus = 'pending' | 'running' | 'done' | 'dead';

export interface JobPayload {
  [key: string]: any;
}

export interface Job {
  id: string;
  idempotencyKey: string;
  type: string;
  payload: JobPayload;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  runAt: Date;
  lockedUntil: Date | null;
  lockedBy: string | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface EnqueueOptions {
  idempotencyKey: string;
  type: string;
  payload: JobPayload;
  runAt?: Date;
  maxAttempts?: number;
}

export interface ClaimedJob extends Job {
  // Additional fields for claimed job
}

export interface JobReceipt {
  idempotencyKey: string;
  jobId: string;
  completedAt: Date;
}

export interface OutboxEntry {
  id: string;
  job: Job;
  publishedAt: Date | null;
  createdAt: Date;
}

export interface QueueConfig {
  leaseDurationMs: number;
  pollIntervalMs: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
  jitterMs: number;
}

export const DEFAULT_QUEUE_CONFIG: QueueConfig = {
  leaseDurationMs: 30000,
  pollIntervalMs: 1000,
  baseBackoffMs: 5000,
  maxBackoffMs: 15 * 60 * 1000,
  jitterMs: 5000,
};