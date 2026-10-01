import { beforeAll, afterAll, beforeEach, vi } from 'vitest';
import { pool, closePool } from '../src/database/client.js';
import { runMigrations } from '../src/database/schema.js';

const mainUrl = process.env.POSTGRES_DB_URL || 'postgresql://postgres:postgres@localhost:5432/transaction_queue';

process.env.POSTGRES_DB_URL = mainUrl;

let migrationsRun = false;

beforeAll(async () => {
  if (!migrationsRun) {
    try {
      await runMigrations();
      migrationsRun = true;
    } catch (error) {
      console.warn('Migration failed, tests may not work:', error);
    }
  }
}, 30000);

afterAll(async () => {
  await closePool();
});

beforeEach(async () => {
  try {
    await pool.query('TRUNCATE jobs, job_receipts, outbox RESTART IDENTITY CASCADE');
  } catch (error) {
    // Tables might not exist
  }
});

vi.setConfig({ testTimeout: 30000 });