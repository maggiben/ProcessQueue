# process-queue

`process-queue` unifies queue lifecycle controls from QueueManager with promise-based concurrency limiting inspired by p-limit, in a single TypeScript-first package.

## Features

- Hybrid API: `enqueue`/`limit` plus `start`/`pause`/`resume`/`next`/`clear`/`drain`
- Runtime control for `concurrency`, `delay`, and `batch`
- QueueManager-style item processing via `add`, `addEach`, `each`, and `complete`
- No external queue dependency
- Typed API and npm-ready build output

## Install

```bash
npm install process-queue
```

## Quick Start

```ts
import {ProcessQueue} from "process-queue";

const queue = new ProcessQueue({concurrency: 2});

const results = await Promise.all([
  queue.limit(async (value: number) => value * 2, 2),
  queue.enqueue(async () => "hello")
]);

console.log(results); // [4, "hello"]
```

## Examples

Runnable examples are available in `examples/`:

- `examples/basic-limit.mjs`
- `examples/queue-items.mjs`
- `examples/pause-resume-and-clear.mjs`

Run them after building the package:

```bash
npm run build
node examples/basic-limit.mjs
```

## API

### Constructor

```ts
new ProcessQueue({
  concurrency?: number;
  delay?: number; // -1 to disable automatic post-completion scheduling
  batch?: number;
  paused?: boolean;
  rejectOnClear?: boolean;
  maxQueueSize?: number;
  queue?: TItem[]; // seed item queue
  callback?: (item: TItem | TItem[]) => unknown;
  complete?: () => unknown;
});
```

### Task-centric methods

- `enqueue(task, options?)`
- `limit(fn, ...args)`
- `map(iterable, mapper)`
- `drain()`

### Queue lifecycle

- `start()` / `resume()`
- `pause()`
- `next(retry?)`
- `clear({rejectPending, reason}?)`
- `clearQueue(...)`

### QueueManager compatibility methods

- `add(item, priority?)`
- `addEach(items, priority?)`
- `each(handler)`
- `complete(handler)`
- `size()`
- `indexOf(item)`
- `update(options)`

### Metrics

- `activeCount`
- `pendingCount`
- `isPaused`
- `concurrency` (get/set)
- `metrics()`

## Migration Notes

- This package provides a new hybrid API and is intentionally breaking versus both legacy projects.
- If you used `p-limit`, start with `limit`, `map`, and `concurrency`.
- If you used QueueManager, use `add`/`addEach` + `each`/`complete` + lifecycle methods.

## Development

```bash
npm install
npm run lint
npm run typecheck
npm test
npm run build
```
