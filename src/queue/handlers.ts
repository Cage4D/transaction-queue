import { Job, JobPayload } from '../jobs/types.js';

export type JobHandler = (payload: JobPayload, job: Job) => Promise<void>;

export class HandlerRegistry {
  private handlers = new Map<string, JobHandler>();

  register(type: string, handler: JobHandler): void {
    if (this.handlers.has(type)) {
      throw new Error(`Handler for job type "${type}" already registered`);
    }
    this.handlers.set(type, handler);
  }

  unregister(type: string): boolean {
    return this.handlers.delete(type);
  }

  get(type: string): JobHandler | undefined {
    return this.handlers.get(type);
  }

  has(type: string): boolean {
    return this.handlers.has(type);
  }

  getTypes(): string[] {
    return Array.from(this.handlers.keys());
  }
}

export const handlerRegistry = new HandlerRegistry();