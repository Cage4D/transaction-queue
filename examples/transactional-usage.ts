import { closePool } from '../src/database/client.js';
import { createQueue, LeadflowQueue } from '../src/queue/queue.js';
import { handlerRegistry } from '../src/queue/handlers.js';
import { getClient } from '../src/database/client.js';

async function main() {
  const queue = createQueue({ autoMigrate: true });

  handlerRegistry.register('order-created', async (payload) => {
    console.log(`Sending order confirmation for order ${payload.orderId}`);
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log(`Order confirmation sent`);
  });

  handlerRegistry.register('inventory-reserved', async (payload) => {
    console.log(`Reserving inventory for order ${payload.orderId}`);
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log(`Inventory reserved`);
  });

  console.log('=== Example 1: Transactional Enqueue ===');
  const client = await getClient();
  
  try {
    await client.query('BEGIN');
    
    await client.query(
      `INSERT INTO orders (id, customer_id, total) VALUES ($1, $2, $3)`,
      ['order-123', 'customer-456', 149.99]
    );

    await queue.enqueueInTransaction(client, {
      idempotencyKey: 'order-created-order-123',
      type: 'order-created',
      payload: { orderId: 'order-123', customerId: 'customer-456', total: 149.99 },
    });

    await queue.enqueueInTransaction(client, {
      idempotencyKey: 'inventory-reserved-order-123',
      type: 'inventory-reserved',
      payload: { orderId: 'order-123', items: ['item-a', 'item-b'] },
    });

    await client.query('COMMIT');
    console.log('Transaction committed, jobs enqueued atomically');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Transaction rolled back:', error);
  } finally {
    client.release();
  }

  console.log('\n=== Example 2: Outbox Pattern ===');
  const client2 = await getClient();
  
  try {
    await client2.query('BEGIN');
    
    await client2.query(
      `INSERT INTO orders (id, customer_id, total) VALUES ($1, $2, $3)`,
      ['order-456', 'customer-789', 299.99]
    );

    await queue.enqueueToOutbox(client2, {
      idempotencyKey: 'outbox-order-created-456',
      type: 'order-created',
      payload: { orderId: 'order-456', customerId: 'customer-789', total: 299.99 },
    });

    await queue.enqueueToOutbox(client2, {
      idempotencyKey: 'outbox-inventory-reserved-456',
      type: 'inventory-reserved',
      payload: { orderId: 'order-456', items: ['item-c', 'item-d'] },
    });

    await client2.query('COMMIT');
    console.log('Transaction committed, jobs written to outbox');
  } catch (error) {
    await client2.query('ROLLBACK');
    console.error('Transaction rolled back:', error);
  } finally {
    client2.release();
  }

  console.log('\n=== Publishing Outbox ===');
  const published = await queue.publishOutbox();
  console.log(`Published ${published} jobs from outbox`);

  console.log('\n=== Starting Worker ===');
  await queue.startWorker({ 
    config: { pollIntervalMs: 500 },
    onJobStart: (job) => console.log(`Started: ${job.type} (${job.idempotencyKey})`),
    onJobComplete: (job) => console.log(`Completed: ${job.type} (${job.idempotencyKey})`),
    onJobError: (job, error) => console.error(`Failed: ${job.type} - ${error.message}`),
  });

  console.log('Worker started. Press Ctrl+C to stop.');

  process.on('SIGINT', async () => {
    console.log('\nShutting down...');
    await queue.shutdown();
    await closePool();
    process.exit(0);
  });
}

main().catch(console.error);