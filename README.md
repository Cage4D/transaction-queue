# Transaction Queue (Leadflow)

An application-agnostic transaction queue system built with TypeScript and PostgreSQL. Supports atomic job enqueueing within database transactions, outbox pattern, exponential backoff with jitter, lease-based locking, and graceful worker shutdown.

## Features

- **V1 - Basic Queue**: Enqueue, worker, handlers, status tracking
- **V2 - Transactional Enqueue**: Atomic enqueue within DB transactions, outbox pattern
- **V3 - Reliability**: Retries, exponential backoff, job timeouts, stuck-job recovery, graceful shutdown
- **V4 - Concurrency**: Multiple workers with `FOR UPDATE SKIP LOCKED`
- **V5 - Observability**: Stuck job detection, structured logging

## Architecture

### Data Model

```sql
-- Jobs table: core queue storage
CREATE TABLE jobs (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT        NOT NULL UNIQUE,
  type            TEXT        NOT NULL,
  payload         JSONB       NOT NULL,
  status          TEXT        NOT NULL DEFAULT 'pending',
  attempts        INT         NOT NULL DEFAULT 0,
  max_attempts    INT         NOT NULL DEFAULT 6,
  run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_until    TIMESTAMPTZ,
  locked_by       TEXT,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('pending','running','done','dead')),
  CHECK (attempts <= max_attempts)
);

-- Job receipts: deduplication ledger
CREATE TABLE job_receipts (
  idempotency_key TEXT PRIMARY KEY,
  job_id          BIGINT NOT NULL REFERENCES jobs(id),
  completed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Outbox: for jobs enqueued inside a business transaction
CREATE TABLE outbox (
  id           BIGSERIAL PRIMARY KEY,
  job          JSONB NOT NULL,
  published_at TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Key Design Decisions

1. **FOR UPDATE SKIP LOCKED**: Atomic job claiming without blocking
2. **Receipt-first Deduplication**: Never double-send; lost sends are detectable
3. **Lease-based Locking**: Workers hold leases (30s default), heartbeats extend them
4. **Exponential Backoff + Jitter**: `run_at = now() + (2^attempts * base) + random(0, base)`
5. **Outbox Pattern**: Transactional enqueue via separate shipper process

## Installation

```bash
npm install
```

## Configuration

Environment variables:
- `PGHOST` - PostgreSQL host (default: localhost)
- `PGPORT` - PostgreSQL port (default: 5432)
- `PGDATABASE` - Database name (default: transaction_queue)
- `PGUSER` - PostgreSQL user (default: postgres)
- `PGPASSWORD` - PostgreSQL password (default: postgres)

## Usage

### Basic Usage

```typescript
import { createQueue } from './src/queue/queue.js';
import { handlerRegistry } from './src/queue/handlers.js';

const queue = createQueue({ autoMigrate: true });

handlerRegistry.register('send-email', async (payload) => {
  await sendEmail(payload.to, payload.subject, payload.body);
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
```

### Transactional Enqueue (V2)

```typescript
import { getClient } from './src/database/client.js';

const client = await getClient();
try {
  await client.query('BEGIN');
  
  await client.query('INSERT INTO orders ...');
  
  await queue.enqueueInTransaction(client, {
    idempotencyKey: 'order-created-123',
    type: 'order-created',
    payload: { orderId: '123', amount: 99.99 },
  });
  
  await client.query('COMMIT');
} catch (error) {
  await client.query('ROLLBACK');
} finally {
  client.release();
}
```

### Outbox Pattern

```typescript
const client = await getClient();
try {
  await client.query('BEGIN');
  
  await client.query('INSERT INTO orders ...');
  
  await queue.enqueueToOutbox(client, {
    idempotencyKey: 'outbox-order-123',
    type: 'order-created',
    payload: { orderId: '123' },
  });
  
  await client.query('COMMIT');
} finally {
  client.release();
}

// Separate process publishes outbox
await queue.publishOutbox();
```

## Running Examples

```bash
# Basic usage
npx tsx examples/basic-usage.ts

# Transactional enqueue
npx tsx examples/transactional-usage.ts
```

## Testing

```bash
# Requires PostgreSQL running on localhost:5432
# Create test database:
createdb transaction_queue_test

npm test
```

## API

### LeadflowQueue

- `enqueue(options)` - Add job to queue
- `enqueueInTransaction(client, options)` - Add job within existing transaction
- `enqueueToOutbox(client, options)` - Add job to outbox
- `startWorker(options)` - Start background worker
- `stopWorker(graceful)` - Stop worker
- `publishOutbox(batchSize)` - Move outbox entries to jobs
- `reapExpiredLeases()` - Recover stuck jobs
- `getStuckJobs(thresholdMinutes)` - Find stuck jobs
- `shutdown()` - Graceful shutdown

### TransactionQueue (low-level)

- `claimNextJob()` - Atomically claim next job
- `claimReceipt(jobId, idempotencyKey)` - Deduplication check
- `completeJob(jobId)` - Mark job done
- `failJob(jobId, error)` - Mark job failed (retry or dead)
- `releaseLease(jobId)` - Release lease, requeue
- `extendLease(jobId, additionalMs)` - Extend lease (heartbeat)

### Job Status Flow

```
pending -> running -> done
    |           |
    |           +-> fail -> pending (retry) -> running -> done
    |                           |
    |                           +-> fail -> ... -> dead (max attempts)
    |
    +-> releaseLease -> pending
```

## License

MIT