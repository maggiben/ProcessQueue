import {ProcessQueue} from "./ProcessQueue.js";
import {QueueCapacityError, QueueClearedError} from "./errors.js";
import type {EnqueueOptions, ProcessQueueOptions} from "./types.js";

export {ProcessQueue, QueueCapacityError, QueueClearedError};
export type {ProcessQueueOptions, EnqueueOptions};

export default function createProcessQueue<TItem = unknown>(options: ProcessQueueOptions<TItem> = {}): ProcessQueue<TItem> {
  return new ProcessQueue<TItem>(options);
}

export function limitFunction<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => TResult | Promise<TResult>,
  options: ProcessQueueOptions = {}
): (...args: TArgs) => Promise<TResult> {
  const limiter = new ProcessQueue(options);
  return (...args: TArgs) => limiter.limit(fn, ...args);
}
