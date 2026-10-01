import { closePool } from '../src/database/client.js';
import { createQueue, LeadflowQueue } from '../src/queue/queue.js';
import { handlerRegistry } from '../src/queue/handlers.js';

async function main() {
  const queue = createQueue({ autoMigrate: true });

  handlerRegistry.register('send-welcome-email', async (payload) => {
    console.log(`Sending welcome email to ${payload.email}`);
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log(`Email sent to ${payload.email}`);
  });

  handlerRegistry.register('process-order', async (payload) => {
    console.log(`Processing order ${payload.orderId} for $${payload.amount}`);
    await new Promise(resolve => setTimeout(resolve, 200));
    console.log(`Order ${payload.orderId} processed`);
  });

  handlerRegistry.register('send-notification', async (payload) => {
    console.log(`Sending notification: ${payload.message}`);
  });

  await queue.enqueue({
    idempotencyKey: 'welcome-user-123',
    type: 'send-welcome-email',
    payload: { email: 'user@example.com', name: 'John' },
  });

  await queue.enqueue({
    idempotencyKey: 'order-456',
    type: 'process-order',
    payload: { orderId: 'order-456', amount: 99.99, items: ['item1', 'item2'] },
  });

  const futureTime = new Date(Date.now() + 5000);
  await queue.enqueue({
    idempotencyKey: 'notification-789',
    type: 'send-notification',
    payload: { message: 'Your order has shipped!' },
    runAt: futureTime,
  });

  await queue.startWorker({ 
    config: { pollIntervalMs: 1000 },
    onJobStart: (job) => console.log(`Started: ${job.type} (${job.idempotencyKey})`),
    onJobComplete: (job) => console.log(`Completed: ${job.type} (${job.idempotencyKey})`),
    onJobError: (job, error) => console.error(`Failed: ${job.type} (${job.idempotencyKey}) - ${error.message}`),
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