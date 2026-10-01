export const SCHEMA = `
-- Jobs table: core queue storage
CREATE TABLE IF NOT EXISTS jobs (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT        NOT NULL UNIQUE,
  type            TEXT        NOT NULL,
  payload         JSONB       NOT NULL,
  status          TEXT        NOT NULL DEFAULT 'pending',
                    -- pending | running | done | dead
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
CREATE TABLE IF NOT EXISTS job_receipts (
  idempotency_key TEXT PRIMARY KEY,
  job_id          BIGINT NOT NULL REFERENCES jobs(id),
  completed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Outbox: for jobs enqueued inside a business transaction
CREATE TABLE IF NOT EXISTS outbox (
  id           BIGSERIAL PRIMARY KEY,
  job          JSONB NOT NULL,
  published_at TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS jobs_pollable ON jobs (run_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS jobs_expired_lease ON jobs (locked_until)
  WHERE status = 'running';

-- Updated at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_jobs_updated_at ON jobs;
CREATE TRIGGER update_jobs_updated_at
  BEFORE UPDATE ON jobs
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();
`;

// Arbitrary constant; any process running migrations takes this lock first
const MIGRATION_LOCK_ID = 727274;

export async function runMigrations(): Promise<void> {
  const { getClient } = await import('./client.js');
  const client = await getClient();
  try {
    // Serialize concurrent migrations (e.g. several processes starting at once);
    // CREATE OR REPLACE FUNCTION / DROP TRIGGER can deadlock otherwise.
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    try {
      await client.query(SCHEMA);
    } finally {
      await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
    }
  } finally {
    client.release();
  }
}
