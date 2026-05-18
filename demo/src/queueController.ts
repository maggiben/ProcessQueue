import {ProcessQueue, QueueClearedError} from "process-queue";
import type {QueueMetrics} from "../types.js";

export type WorkloadKind = "tasks" | "items";

export const MILLION = 1_000_000;
export const TARGET_RUN_MS = 45_000;
export const DEFAULT_CONCURRENCY = 256;
export const DEFAULT_BATCH = 32;

export interface DemoStats {
  enqueued: number;
  completed: number;
  failed: number;
  cleared: number;
  priorityBurst: number;
  mapRuns: number;
  itemRetries: number;
  startedAt: number | null;
  finishedAt: number | null;
  loadInProgress: boolean;
  workload: WorkloadKind | null;
  totalTarget: number;
}

export interface DemoSnapshot {
  metrics: QueueMetrics;
  stats: DemoStats;
  throughput: number;
  elapsedMs: number;
  progress: number;
  etaSeconds: number | null;
  isFinished: boolean;
}

export interface LogEntry {
  id: number;
  time: string;
  level: "info" | "warn" | "action" | "success";
  message: string;
}

interface QueueItem {
  id: number;
  retryOnce: boolean;
}

/** Each queue task represents this many logical operations (1M total ops, fewer queue entries). */
const ITEMS_PER_TASK = 20;
const TASK_COUNT = MILLION / ITEMS_PER_TASK;
const ENQUEUE_CHUNK = 5_000;
const THROUGHPUT_WINDOW_MS = 1_000;
const COMPLETED_FLUSH_MS = 50;

export class QueueDemoController {
  queue: ProcessQueue<number | QueueItem>;
  stats: DemoStats;
  private loadGeneration = 0;
  private logId = 0;
  private logs: LogEntry[] = [];
  private readonly listeners = new Set<() => void>();
  private abortController: AbortController | null = null;
  private itemAttempts = new Map<number, number>();
  private pendingCompleted = 0;
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private throughputSample = {at: 0, completed: 0};
  private lastThroughput = 0;

  constructor() {
    this.stats = this.freshStats();
    this.queue = this.createTaskQueue(DEFAULT_CONCURRENCY, 0, DEFAULT_BATCH);
    this.startCompletedFlush();
  }

  private freshStats(): DemoStats {
    return {
      enqueued: 0,
      completed: 0,
      failed: 0,
      cleared: 0,
      priorityBurst: 0,
      mapRuns: 0,
      itemRetries: 0,
      startedAt: null,
      finishedAt: null,
      loadInProgress: false,
      workload: null,
      totalTarget: 0
    };
  }

  private completedCount(): number {
    return this.stats.completed + this.pendingCompleted;
  }

  private checkRunComplete(): void {
    if (this.stats.finishedAt !== null || !this.stats.startedAt || this.stats.totalTarget === 0) {
      return;
    }

    if (this.stats.loadInProgress) {
      return;
    }

    if (this.completedCount() < this.stats.totalTarget) {
      return;
    }

    if (!this.queue.metrics().isDrained) {
      return;
    }

    this.stats.finishedAt = Date.now();
    this.lastThroughput = 0;
    const elapsed = this.stats.finishedAt - this.stats.startedAt;
    this.log("success", `Run complete in ${(elapsed / 1000).toFixed(1)}s`);
    this.emit();
  }

  private resumeRunClock(): void {
    this.stats.finishedAt = null;
  }

  private createTaskQueue(concurrency: number, delay: number, batch: number): ProcessQueue<number> {
    return new ProcessQueue<number>({
      concurrency,
      delay,
      batch,
      rejectOnClear: true
    });
  }

  private createItemQueue(concurrency: number, delay: number, batch: number): ProcessQueue<QueueItem> {
    return new ProcessQueue<QueueItem>({
      concurrency,
      delay,
      batch,
      rejectOnClear: true,
      callback: item => {
        const attempt = this.itemAttempts.get(item.id) ?? 0;
        const next = attempt + 1;
        this.itemAttempts.set(item.id, next);

        if (item.retryOnce && next === 1) {
          this.stats.itemRetries++;
          this.emit();
          return true;
        }

        this.pendingCompleted++;
        return false;
      },
      complete: () => {
        this.log("success", "complete() handler — all items drained");
        this.emit();
      }
    });
  }

  private startCompletedFlush(): void {
    this.flushTimer = setInterval(() => {
      if (this.pendingCompleted === 0) {
        return;
      }

      this.stats.completed += this.pendingCompleted;
      this.pendingCompleted = 0;
      this.checkRunComplete();
      this.emit();
    }, COMPLETED_FLUSH_MS);
  }

  private markCompleted(): void {
    this.pendingCompleted++;
  }

  private resetThroughputSample(): void {
    this.throughputSample = {at: 0, completed: 0};
    this.lastThroughput = 0;
  }

  private measureThroughput(): number {
    const now = Date.now();
    const sample = this.throughputSample;

    if (sample.at === 0) {
      this.throughputSample = {at: now, completed: this.stats.completed};
      return 0;
    }

    const elapsed = now - sample.at;
    if (elapsed < THROUGHPUT_WINDOW_MS) {
      return this.lastThroughput;
    }

    const delta = this.stats.completed - sample.completed;
    this.lastThroughput = (delta / elapsed) * 1000;
    this.throughputSample = {at: now, completed: this.stats.completed};
    return this.lastThroughput;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getRecentLogs(limit = 24): LogEntry[] {
    return this.logs.slice(-limit);
  }

  snapshot(): DemoSnapshot {
    const metrics = this.queue.metrics();
    const completed = this.completedCount();
    const isFinished = this.stats.finishedAt !== null;
    const elapsedMs =
      this.stats.startedAt && this.stats.finishedAt
        ? this.stats.finishedAt - this.stats.startedAt
        : this.stats.startedAt
          ? Date.now() - this.stats.startedAt
          : 0;
    const throughput = isFinished ? 0 : this.measureThroughput();
    const target = this.stats.totalTarget || 0;
    const progress = target > 0 ? Math.min(completed / target, 1) : 0;

    let etaSeconds: number | null = null;
    if (target > 0 && completed < target && throughput > 0) {
      etaSeconds = (target - completed) / throughput;
    } else if (target > 0 && completed >= target) {
      etaSeconds = 0;
    }

    return {
      metrics,
      stats: {...this.stats, completed},
      throughput,
      elapsedMs,
      progress,
      etaSeconds,
      isFinished
    };
  }

  private emit(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }

  private log(level: LogEntry["level"], message: string): void {
    this.logs.push({
      id: ++this.logId,
      time: new Date().toLocaleTimeString(),
      level,
      message
    });
    if (this.logs.length > 120) {
      this.logs.splice(0, this.logs.length - 120);
    }
    this.emit();
  }

  reset(): void {
    this.loadGeneration++;
    this.abortController?.abort();
    this.abortController = null;
    const {concurrency} = this.queue;
    this.queue = this.createTaskQueue(concurrency, 0, DEFAULT_BATCH);
    this.itemAttempts.clear();
    this.pendingCompleted = 0;
    this.stats = this.freshStats();
    this.resetThroughputSample();
    this.log("warn", "Reset — new task queue, counters cleared");
    this.emit();
  }

  async loadMillion(): Promise<void> {
    if (this.stats.loadInProgress) {
      return;
    }

    this.reset();
    this.queue = this.createTaskQueue(DEFAULT_CONCURRENCY, 0, DEFAULT_BATCH);
    this.stats.loadInProgress = true;
    this.stats.workload = "tasks";
    this.stats.totalTarget = MILLION;
    this.stats.startedAt = Date.now();
    this.resetThroughputSample();
    const generation = ++this.loadGeneration;

    this.log(
      "action",
      `${TASK_COUNT.toLocaleString()} queue tasks × ${ITEMS_PER_TASK} ops = ${MILLION.toLocaleString()} (target ~${TARGET_RUN_MS / 1000}s)`
    );

    const runTask = () => {
      this.pendingCompleted += ITEMS_PER_TASK;
      return 1;
    };

    try {
      for (let offset = 0; offset < TASK_COUNT; offset += ENQUEUE_CHUNK) {
        if (generation !== this.loadGeneration) {
          return;
        }

        const end = Math.min(offset + ENQUEUE_CHUNK, TASK_COUNT);
        for (let task = offset; task < end; task++) {
          this.stats.enqueued += ITEMS_PER_TASK;
          void this.queue.enqueue(runTask).catch(reason => this.onTaskRejected(reason));
        }

        this.emit();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
      }

      this.log("success", "All tasks queued — processing toward 1M operations");
    } finally {
      if (generation === this.loadGeneration) {
        this.stats.loadInProgress = false;
        this.emit();
      }
    }
  }

  async loadItemPipeline(count = 50_000): Promise<void> {
    if (this.stats.loadInProgress) {
      return;
    }

    this.reset();
    this.queue = this.createItemQueue(DEFAULT_CONCURRENCY, 0, DEFAULT_BATCH);
    this.stats.loadInProgress = true;
    this.stats.workload = "items";
    this.stats.totalTarget = count;
    this.stats.startedAt = Date.now();
    this.resetThroughputSample();
    const generation = ++this.loadGeneration;

    this.log("action", `addEach() × ${count.toLocaleString()} with each() retry (return true)`);

    const chunk = 5_000;
    for (let offset = 0; offset < count; offset += chunk) {
      if (generation !== this.loadGeneration) {
        return;
      }

      const slice = Array.from({length: Math.min(chunk, count - offset)}, (_, index) => {
        const id = offset + index;
        return {id, retryOnce: id % 97 === 0};
      });

      this.queue.addEach(slice);
      this.stats.enqueued += slice.length;
      this.emit();
      await new Promise<void>(resolve => setTimeout(resolve, 0));
    }

    this.stats.loadInProgress = false;
    this.log("success", "Item pipeline queued — use pause / next(retry) / clear");
    this.emit();
  }

  async runMapSample(size = 10_000): Promise<void> {
    if (this.stats.workload === "items") {
      this.log("warn", "map() needs task queue — reset or load 1M tasks first");
      return;
    }

    const taskQueue = this.queue as ProcessQueue<number>;
    this.stats.mapRuns++;
    this.stats.totalTarget = (this.stats.totalTarget || 0) + size;
    this.resumeRunClock();
    this.log("action", `map() + limit() on ${size.toLocaleString()} integers`);

    const values = Array.from({length: size}, (_, index) => index);
    const started = Date.now();
    await taskQueue.map(values, value => value * 2);
    const elapsed = Date.now() - started;

    this.stats.completed += size;
    this.stats.enqueued += size;
    if (!this.stats.startedAt) {
      this.stats.startedAt = started;
    }

    this.log("success", `map() finished in ${elapsed}ms`);
    this.emit();
  }

  pause(): void {
    this.queue.pause();
    this.log("info", "pause()");
    this.emit();
  }

  resume(): void {
    this.queue.resume();
    this.log("info", "resume() / start()");
    this.emit();
  }

  stepNext(retry = false): void {
    this.queue.next(retry);
    this.log("action", retry ? "next(true)" : "next()");
    this.emit();
  }

  clearPending(): void {
    const removed = this.queue.clear({
      rejectPending: true,
      reason: new QueueClearedError("cleared from demo UI")
    });
    this.stats.cleared += removed;
    this.log("warn", `clear() → ${removed.toLocaleString()} removed`);
    this.emit();
  }

  async drain(): Promise<void> {
    this.log("info", "await drain()");
    const started = Date.now();
    await this.queue.drain();
    this.log("success", `drain() in ${Date.now() - started}ms`);
    this.emit();
  }

  injectPriorityBurst(count = 500): void {
    if (this.stats.workload === "items") {
      this.log("warn", "Priority enqueue needs task queue");
      return;
    }

    const taskQueue = this.queue as ProcessQueue<number>;
    this.stats.totalTarget += count;
    this.resumeRunClock();

    for (let index = 0; index < count; index++) {
      this.stats.priorityBurst++;
      this.stats.enqueued++;
      void taskQueue
        .enqueue(() => {
          this.markCompleted();
          return -index;
        }, {priority: true})
        .catch(reason => this.onTaskRejected(reason));
    }

    this.log("action", `enqueue({ priority: true }) × ${count}`);
    this.emit();
  }

  abortInflightSample(): void {
    if (this.stats.workload === "items") {
      this.log("warn", "AbortSignal demo needs task queue");
      return;
    }

    const taskQueue = this.queue as ProcessQueue<number>;
    this.abortController?.abort();
    this.abortController = new AbortController();
    const {signal} = this.abortController;
    this.stats.totalTarget += 200;
    this.resumeRunClock();

    for (let index = 0; index < 200; index++) {
      this.stats.enqueued++;
      void taskQueue
        .enqueue(
          () =>
            new Promise<number>(resolve => {
              setTimeout(() => resolve(index), 250);
            }),
          {signal}
        )
        .then(() => this.markCompleted())
        .catch(reason => this.onTaskRejected(reason));
    }

    setTimeout(() => this.abortController?.abort(), 40);
    this.log("warn", "AbortSignal on 200 slow tasks");
    this.emit();
  }

  setConcurrency(value: number): void {
    this.queue.concurrency = value;
    this.log("info", `concurrency → ${value}`);
    this.emit();
  }

  setDelay(value: number): void {
    this.queue.update({delay: value});
    this.log("info", `delay → ${value}${value === -1 ? " (manual next)" : "ms"}`);
    this.emit();
  }

  setBatch(value: number): void {
    this.queue.update({batch: value});
    this.log("info", `batch → ${value}`);
    this.emit();
  }

  updateAll(options: {concurrency: number; delay: number; batch: number}): void {
    this.queue.update(options);
    this.log("info", `update(${JSON.stringify(options)})`);
    this.emit();
  }

  private onTaskRejected(reason: unknown): void {
    if (reason instanceof QueueClearedError) {
      this.stats.cleared++;
    } else {
      this.stats.failed++;
    }
    this.emit();
  }
}
