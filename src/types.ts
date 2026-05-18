export type TaskFactory<T> = () => Promise<T> | T;
export type QueueInstanceLike = {next(retry?: boolean): void};

export interface ProcessQueueOptions<TItem = unknown> {
  concurrency?: number;
  delay?: number;
  batch?: number;
  paused?: boolean;
  rejectOnClear?: boolean;
  maxQueueSize?: number;
  queue?: TItem[];
  callback?: (this: QueueInstanceLike, item: TItem | TItem[]) => unknown;
  complete?: (this: QueueInstanceLike) => unknown;
}

export interface EnqueueOptions {
  priority?: boolean;
  signal?: AbortSignal;
}

export interface ClearOptions {
  rejectPending?: boolean;
  reason?: unknown;
}

export interface QueueMetrics {
  activeCount: number;
  pendingCount: number;
  isPaused: boolean;
  isDrained: boolean;
}

export type QueueEntryKind = "task" | "item";

export interface QueueEntry<TItem = unknown, TResult = unknown> {
  kind: QueueEntryKind;
  run: TaskFactory<TResult>;
  resolve: (value: TResult | PromiseLike<TResult>) => void;
  reject: (reason?: unknown) => void;
  payload?: TItem;
  signal?: AbortSignal;
}

export interface DispatchContext<TItem = unknown> {
  recentEntries: QueueEntry<TItem>[];
}
