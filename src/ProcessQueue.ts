import {Deque} from "./deque.js";
import {
  QueueCapacityError,
  QueueClearedError,
  validateBatch,
  validateConcurrency,
  validateDelay
} from "./errors.js";
import type {
  ClearOptions,
  EnqueueOptions,
  QueueEntry,
  QueueMetrics,
  TaskFactory,
  ProcessQueueOptions
} from "./types.js";

const DEFAULT_CONCURRENCY = 1;
const DEFAULT_DELAY = 0;
const DEFAULT_BATCH = 1;

export class ProcessQueue<TItem = unknown> {
  private readonly queue = new Deque<QueueEntry<TItem, unknown>>();
  private activeCountValue = 0;
  private paused: boolean;
  private delay: number;
  private batch: number;
  private concurrencyValue: number;
  private rejectOnClear: boolean;
  private readonly maxQueueSize: number | undefined;
  private onEach: ((this: ProcessQueue<TItem>, item: TItem | TItem[]) => unknown) | undefined;
  private onComplete: ((this: ProcessQueue<TItem>) => unknown) | undefined;
  private recentEntries: QueueEntry<TItem, unknown>[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private readonly drainResolvers: Array<() => void> = [];
  private completedSinceDrainSignal = true;

  constructor(options: ProcessQueueOptions<TItem> = {}) {
    this.concurrencyValue = options.concurrency ?? DEFAULT_CONCURRENCY;
    this.delay = options.delay ?? DEFAULT_DELAY;
    this.batch = options.batch ?? DEFAULT_BATCH;
    this.paused = options.paused ?? false;
    this.rejectOnClear = options.rejectOnClear ?? false;
    this.maxQueueSize = options.maxQueueSize;
    this.onEach = options.callback;
    this.onComplete = options.complete;

    validateConcurrency(this.concurrencyValue);
    validateDelay(this.delay);
    validateBatch(this.batch);

    if (options.queue?.length) {
      this.addEach(options.queue);
    } else if (!this.paused) {
      this.kick();
    }
  }

  get activeCount(): number {
    return this.activeCountValue;
  }

  get pendingCount(): number {
    return this.queue.size;
  }

  get isPaused(): boolean {
    return this.paused;
  }

  get concurrency(): number {
    return this.concurrencyValue;
  }

  set concurrency(value: number) {
    this.setConcurrency(value);
  }

  metrics(): QueueMetrics {
    return {
      activeCount: this.activeCountValue,
      pendingCount: this.queue.size,
      isPaused: this.paused,
      isDrained: this.isDrained()
    };
  }

  setConcurrency(value: number): this {
    validateConcurrency(value);
    this.concurrencyValue = value;
    this.kick();
    return this;
  }

  each(handler: (this: ProcessQueue<TItem>, item: TItem | TItem[]) => unknown): this {
    this.onEach = handler;
    return this;
  }

  complete(handler: (this: ProcessQueue<TItem>) => unknown): this {
    this.onComplete = handler;
    return this;
  }

  update(options: Partial<Omit<ProcessQueueOptions<TItem>, "queue">>): this {
    if (typeof options.concurrency !== "undefined") {
      this.setConcurrency(options.concurrency);
    }

    if (typeof options.delay !== "undefined") {
      validateDelay(options.delay);
      this.delay = options.delay;
    }

    if (typeof options.batch !== "undefined") {
      validateBatch(options.batch);
      this.batch = options.batch;
    }

    if (typeof options.rejectOnClear === "boolean") {
      this.rejectOnClear = options.rejectOnClear;
    }

    if (typeof options.paused === "boolean") {
      if (options.paused) {
        this.pause();
      } else {
        this.resume();
      }
    }

    if (options.callback) {
      this.onEach = options.callback;
    }

    if (options.complete) {
      this.onComplete = options.complete;
    }

    this.kick();
    return this;
  }

  enqueue<TResult>(task: TaskFactory<TResult>, options: EnqueueOptions = {}): Promise<TResult> {
    if (options.signal?.aborted) {
      return Promise.reject(options.signal.reason);
    }

    if (typeof this.maxQueueSize === "number" && this.queue.size >= this.maxQueueSize) {
      return Promise.reject(new QueueCapacityError(this.maxQueueSize));
    }

    this.completedSinceDrainSignal = false;

    const promise = new Promise<TResult>((resolve, reject) => {
      const entry: QueueEntry<TItem, TResult> = {
        kind: "task",
        run: task,
        resolve,
        reject
      };

      if (options.signal) {
        entry.signal = options.signal;
      }

      if (options.priority) {
        this.queue.pushFront(entry as QueueEntry<TItem, unknown>);
      } else {
        this.queue.push(entry as QueueEntry<TItem, unknown>);
      }
    });

    this.kick();
    return promise;
  }

  limit<TResult, TArgs extends unknown[]>(fn: (...args: TArgs) => TResult | Promise<TResult>, ...args: TArgs): Promise<TResult> {
    return this.enqueue(() => fn(...args));
  }

  add(item: TItem, priority = false): this {
    return this.addEach([item], priority);
  }

  addEach(items: readonly TItem[], priority = false): this {
    for (const item of items) {
      void this.enqueueItem(item, priority);
    }

    return this;
  }

  size(): number {
    return this.queue.size;
  }

  indexOf(item: TItem): number {
    const pendingItems = this.queue.toArray().map(entry => entry.payload).filter((value): value is TItem => typeof value !== "undefined");
    return pendingItems.indexOf(item);
  }

  start(): this {
    this.paused = false;
    this.kick(true);
    return this;
  }

  resume(): this {
    return this.start();
  }

  pause(): this {
    this.paused = true;
    this.stopTimer();
    return this;
  }

  next(retry = false): void {
    if (retry && this.recentEntries.length > 0) {
      this.queue.prepend(this.recentEntries);
    }

    this.recentEntries = [];
    this.kick(true);
  }

  clear(options: ClearOptions = {}): number {
    const shouldReject = options.rejectPending ?? this.rejectOnClear;
    const reason = options.reason ?? new QueueClearedError();
    const entries = this.queue.clear();

    if (shouldReject) {
      for (const entry of entries) {
        entry.reject(reason);
      }
    }

    this.recentEntries = [];
    this.signalDrainIfNeeded();
    return entries.length;
  }

  clearQueue(options: ClearOptions = {}): void {
    this.clear(options);
  }

  async drain(): Promise<void> {
    if (this.isDrained()) {
      return;
    }

    await new Promise<void>(resolve => {
      this.drainResolvers.push(resolve);
    });
  }

  async map<TValue, TResult>(
    iterable: Iterable<TValue>,
    mapper: (value: TValue, index: number) => TResult | Promise<TResult>
  ): Promise<TResult[]> {
    const promises = Array.from(iterable, (value, index) => this.limit(mapper, value, index));
    return Promise.all(promises);
  }

  private enqueueItem(item: TItem, priority: boolean): Promise<TItem> {
    this.completedSinceDrainSignal = false;
    return new Promise<TItem>((resolve, reject) => {
      const entry: QueueEntry<TItem, TItem> = {
        kind: "item",
        payload: item,
        run: () => item,
        resolve,
        reject
      };

      if (priority) {
        this.queue.pushFront(entry as QueueEntry<TItem, unknown>);
      } else {
        this.queue.push(entry as QueueEntry<TItem, unknown>);
      }
    });
  }

  private kick(forceImmediate = false): void {
    if (this.paused) {
      return;
    }

    if (forceImmediate || this.delay <= 0) {
      queueMicrotask(() => this.dispatch());
      return;
    }

    if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.dispatch();
      }, this.delay);
    }
  }

  private dispatch(): void {
    if (this.paused) {
      return;
    }

    if (this.delay === -1 && this.recentEntries.length > 0) {
      return;
    }

    let startedThisCycle = 0;
    this.recentEntries = [];

    while (
      startedThisCycle < this.batch &&
      this.activeCountValue < this.concurrencyValue &&
      this.queue.size > 0 &&
      !this.paused
    ) {
      const entry = this.queue.shift();
      if (!entry) {
        break;
      }

      if (entry.signal?.aborted) {
        entry.reject(entry.signal.reason);
        continue;
      }

      this.activeCountValue++;
      startedThisCycle++;
      this.recentEntries.push(entry);
      this.executeEntry(entry);
    }

    this.signalDrainIfNeeded();
  }

  private executeEntry(entry: QueueEntry<TItem, unknown>): void {
    if (entry.kind === "item" && this.onEach) {
      Promise.resolve(this.onEach.call(this, entry.payload as TItem))
        .then(result => {
          if (result === true) {
            this.queue.pushFront(entry);
          }
          entry.resolve(entry.payload);
        })
        .catch(error => {
          entry.reject(error);
        })
        .finally(() => {
          this.onEntryFinished();
        });
      return;
    }

    Promise.resolve(entry.run())
      .then(value => {
        entry.resolve(value);
      })
      .catch(error => {
        entry.reject(error);
      })
      .finally(() => {
        this.onEntryFinished();
      });
  }

  private onEntryFinished(): void {
    this.activeCountValue--;

    if (this.activeCountValue < 0) {
      this.activeCountValue = 0;
    }

    if (this.delay !== -1) {
      this.kick();
    }

    this.signalDrainIfNeeded();
  }

  private isDrained(): boolean {
    return this.activeCountValue === 0 && this.queue.size === 0;
  }

  private stopTimer(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  private signalDrainIfNeeded(): void {
    if (!this.isDrained()) {
      return;
    }

    if (!this.completedSinceDrainSignal && this.onComplete) {
      this.onComplete.call(this);
    }

    this.completedSinceDrainSignal = true;
    this.recentEntries = [];

    while (this.drainResolvers.length > 0) {
      const resolve = this.drainResolvers.shift();
      resolve?.();
    }
  }
}
