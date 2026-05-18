# Examples

Build the library first so the examples can import from `dist`:

```bash
npm run build
```

Then run any example with Node 20+:

```bash
node examples/basic-limit.mjs
node examples/queue-items.mjs
node examples/pause-resume-and-clear.mjs
```

## What each example shows

- `basic-limit.mjs`: task-style usage with `limit`, `concurrency`, and `drain`
- `queue-items.mjs`: QueueManager-compatible item processing with `addEach`, `callback`, and retry-by-returning-`true`
- `pause-resume-and-clear.mjs`: lifecycle controls (`pause`, `resume`, `clear`) and handling pending task rejections
