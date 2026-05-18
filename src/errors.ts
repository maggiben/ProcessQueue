export class QueueClearedError extends Error {
  constructor(message = "Queue was cleared before task execution") {
    super(message);
    this.name = "QueueClearedError";
  }
}

export class QueueCapacityError extends Error {
  constructor(maxQueueSize: number) {
    super(`Queue reached max size of ${maxQueueSize}`);
    this.name = "QueueCapacityError";
  }
}

export function validateConcurrency(value: number): void {
  if (!((Number.isInteger(value) || value === Number.POSITIVE_INFINITY) && value > 0)) {
    throw new TypeError("Expected `concurrency` to be a number from 1 and up");
  }
}

export function validateDelay(value: number): void {
  if (!Number.isInteger(value) || value < -1) {
    throw new TypeError("Expected `delay` to be an integer >= -1");
  }
}

export function validateBatch(value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new TypeError("Expected `batch` to be an integer >= 1");
  }
}
