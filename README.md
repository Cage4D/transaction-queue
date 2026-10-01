# Transaction Queue

A PostgreSQL-backed job queue for Node.js, written in TypeScript. Jobs can be enqueued atomically inside your own database transactions, claimed safely by many workers at once, retried with exponential backoff, and recovered if a worker dies mid-job.

No broker is needed. If you already run Postgres, you already have the queue.

## Features

- **Transactional enqueue**: insert a job in the same transaction as your business data, so both commit or neither does
- **Outbox pattern**: write jobs to an outbox table inside a transaction, then publish them to the queue separately
- **Safe concurrency**: `FOR UPDATE SKIP LOCKED` lets many workers claim jobs without blocking or double-claiming
- **Lease-based locking**: a claimed job is leased to a worker, and heartbeats extend the lease while the handler runs
- **Retries with backoff**: exponential backoff plus jitter, capped at a maximum
- **Dead-letter state**: jobs that exhaust their attempts move to `dead` and stay queryable
- **Crash recovery**: expired leases are reaped back to `pending`, and suspicious jobs can be listed for inspection
- **Idempotency keys**: enqueueing the same key again updates the existing job instead of creating a duplicate
- **Graceful shutdown**: stopping a worker waits for in-flight jobs to finish

## Quick start

Requirements: Node.js 20+ and Docker.

```bash
# 1. Start Postgres (exposed on localhost:5433)
docker compose up -d

# 2. Install dependencies
npm install

# 3. Configure the connection
echo 'POSTGRES_DB_URL="postgresql://postgres:postgres@localhost:5433/transaction_queue"' > .env

# 4. Run the tests
npm test
```

Without Docker, point `POSTGRES_DB_URL` at any Postgres 14+ database instead.

## Configuration

| Variable | Description |
| --- | --- |
| `POSTGRES_DB_URL` | Full connection string. Takes priority over everything below. |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Used to build a connection string when `POSTGRES_DB_URL` is not set. Defaults: `localhost`, `5432`, `transaction_queue`, `postgres`, `postgres`. |

Queue behavior is configured per queue (all values in milliseconds):

| Option | Default | Meaning |
| --- | --- | --- |
| `leaseDurationMs` | 30000 | How long a claimed job is leased to a worker |
| `pollIntervalMs` | 1000 | How often an idle worker polls for jobs |
| `baseBackoffMs` | 5000 | Base delay before the first retry |
| `maxBackoffMs` | 900000 | Upper bound on the retry delay |
| `jitterMs` | 5000 | Maximum random jitter added to a retry delay (never more than `baseBackoffMs`) |

## Usage

### Basic

```typescript
import { createQueue } from './src/queue/queue.js';
import { handlerRegistry } from './src/queue/handlers.js';
import { closePool } from './src/database/client.js';

const queue = createQueue({ autoMigrate: true });
await queue.initialize(); // creates tables if they don't exist

handlerRegistry.register('send-email', async (payload) => {
  await sendEmail(payload.to, payload.subject);
});

await queue.enqueue({
  idempotencyKey: 'welcome-email-123',
  type: 'send-email',
  payload: { to: 'user@example.com', subject: 'Welcome!' },
});

await queue.startWorker({
  config: { pollIntervalMs: 1000 },
  onJobComplete: (job) => console.log(`Completed: ${job.type}`),
});

// on exit:
await queue.shutdown(); // stops the worker, waiting for in-flight jobs
await closePool();      // closes the shared connection pool
```

### Enqueue inside a transaction

```typescript
import { getClient } from './src/database/client.js';

const client = await getClient();
try {
  await client.query('BEGIN');
  await client.query('INSERT INTO orders ...');

  await queue.enqueueInTransaction(client, {
    idempotencyKey: 'order-created-123',
    type: 'order-created',
    payload: { orderId: '123' },
  });

  await client.query('COMMIT');
} catch (error) {
  await client.query('ROLLBACK');
  throw error;
} finally {
  client.release();
}
```

If the transaction rolls back, the job is never created.

### Outbox pattern

Write to the outbox inside your transaction, then publish from a separate process or timer:

```typescript
await queue.enqueueToOutbox(client, {
  idempotencyKey: 'outbox-order-123',
  type: 'order-created',
  payload: { orderId: '123' },
});

// elsewhere, on an interval:
const published = await queue.publishOutbox(100); // moves up to 100 entries into jobs
```

### Running the examples

```bash
npx tsx examples/basic-usage.ts
npx tsx examples/transactional-usage.ts
```

## How it works

### Job lifecycle

```
pending -> running -> done
   ^          |
   |          +-- handler fails, attempts left --> pending (after backoff)
   |          +-- handler fails, no attempts left --> dead
   |          +-- no handler registered for the type --> dead
   |          +-- lease expires (worker died) --> pending (via reapExpiredLeases)
```

### Data model

- **`jobs`**: the queue. Holds the payload, status, attempt count, `run_at`, and lease fields (`locked_until`, `locked_by`).
- **`job_receipts`**: a ledger written when a handler finishes successfully, keyed by idempotency key.
- **`outbox`**: pending jobs written inside business transactions, waiting to be published.

The full schema is in `src/database/schema.ts`.

### Design decisions

1. **`FOR UPDATE SKIP LOCKED`** claims jobs atomically. Workers skip rows another worker has locked instead of waiting, so throughput scales with worker count.
2. **`attempts` is incremented at claim time**, so it always counts attempts started, including one that is still running. A job fails permanently when `attempts >= max_attempts`.
3. **Leases** make crashes recoverable. If a worker dies, its lease expires and `reapExpiredLeases()` returns the job to `pending`. Heartbeats extend the lease at one third of the lease duration while a handler runs.
4. **Backoff** for retry number `n` is `min(baseBackoffMs * 2^(n-1), maxBackoffMs)` plus a random jitter between 0 and `min(jitterMs, baseBackoffMs)`. Jitter stops many failed jobs from retrying at the same instant.
5. **Missing handlers are permanent failures.** Retrying cannot help if no handler is registered for a job type, so those jobs go straight to `dead`.
6. **The connection pool is shared** across the process, so `queue.shutdown()` does not close it. Call `closePool()` when your application exits.

### Delivery guarantees

This queue provides **at-least-once** delivery. A worker runs the handler first and records the receipt and marks the job done afterwards. If a worker crashes after the handler's side effects but before completion, the job will run again after its lease expires.

**Write handlers to be idempotent**, for example by passing the job's `idempotencyKey` to the external service you call.

`getStuckJobs()` lists jobs that are `running` with an expired lease and already have a receipt. Those are the ones to inspect, since re-running them could repeat a side effect.

## API

### `LeadflowQueue` (from `createQueue()`)

| Method | Description |
| --- | --- |
| `initialize()` | Runs migrations when `autoMigrate` is not `false` |
| `enqueue(options)` | Add a job. Re-enqueuing an existing key resets and updates that job |
| `enqueueInTransaction(client, options)` | Add a job using your open transaction's client |
| `enqueueToOutbox(client, options)` | Write a job to the outbox inside your transaction |
| `publishOutbox(batchSize?)` | Move outbox entries into the jobs table |
| `startWorker(options?)` | Start the background worker |
| `stopWorker(graceful?)` | Stop the worker, waiting for in-flight jobs by default |
| `reapExpiredLeases()` | Return jobs with expired leases to `pending`; resolves to the count |
| `getStuckJobs()` | Jobs that are running, lease-expired, and hold a receipt |
| `getJob(id)` / `getJobByIdempotencyKey(key)` | Look up a job |
| `registerHandler(type, handler)` | Register a handler (same as `handlerRegistry.register`) |
| `shutdown()` | Stop the worker gracefully. Does not close the pool |

`EnqueueOptions`: `idempotencyKey`, `type`, `payload`, plus optional `runAt` (schedule for later) and `maxAttempts` (default 6).

### `TransactionQueue` (low level)

| Method | Description |
| --- | --- |
| `claimNextJob()` | Atomically claim the next due job, or `null` |
| `claimReceipt(jobId, key)` | Insert a receipt; returns `false` if one already exists |
| `completeJob(jobId)` | Mark a job done |
| `failJob(jobId, error, { permanent? })` | Retry with backoff, or mark dead when attempts run out or `permanent` is set |
| `releaseLease(jobId)` | Return a job this worker holds to `pending` |
| `extendLease(jobId, ms)` | Extend this worker's lease (the heartbeat) |

### Handlers

Register one handler per job type. Registering a type twice throws. Handlers receive `(payload, job)`; throwing marks the attempt as failed.

## Testing

```bash
npm test
```

The suite needs a reachable Postgres. **Tests truncate `jobs`, `job_receipts` and `outbox` before every test**, so use a throwaway database:

```bash
docker compose exec db createdb -U postgres transaction_queue_test
POSTGRES_DB_URL="postgresql://postgres:postgres@localhost:5433/transaction_queue_test" npm test
```

Test files run one at a time (`fileParallelism: false` in `vitest.config.ts`) because they share a database. Some tests wait on real timers, so the full run takes about 30 seconds.

## Project structure

```
src/
  database/    connection pool, schema and migrations
  jobs/        job and config types
  queue/       core.ts (SQL operations), worker.ts (poll loop),
               queue.ts (public API), handlers.ts (handler registry)
tests/         vitest suites against a real Postgres
examples/      runnable usage examples
```

## Limitations

- Handlers are registered in a process-wide registry
- There is no built-in scheduler for `publishOutbox()` or `reapExpiredLeases()`; call them on a timer
- A single worker processes one job at a time
- There is no dashboard or metrics endpoint yet

## License

MIT