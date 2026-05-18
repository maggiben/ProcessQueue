let ProcessQueue;
let QueueClearedError;

try {
  ({ProcessQueue, QueueClearedError} = await import("../dist/index.js"));
} catch {
  console.error("Could not load ../dist/index.js. Run `npm run build` first, then re-run this example.");
  process.exit(1);
}

const queue = new ProcessQueue({concurrency: 1, paused: true});

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const jobs = [
  queue.enqueue(async () => {
    await sleep(120);
    return "job-a";
  }),
  queue.enqueue(async () => {
    await sleep(120);
    return "job-b";
  }),
  queue.enqueue(async () => {
    await sleep(120);
    return "job-c";
  })
];

console.log("queued while paused:", queue.metrics());
queue.start();

setTimeout(() => {
  queue.pause();
  console.log("paused:", queue.metrics());
}, 50);

setTimeout(() => {
  const removed = queue.clear({rejectPending: true, reason: new QueueClearedError("manual clear in example")});
  console.log(`cleared ${removed} queued job(s)`);
  queue.resume();
}, 200);

const settled = await Promise.allSettled(jobs);
const printableResults = settled.map(result => {
  if (result.status === "fulfilled") {
    return result;
  }

  return {
    status: "rejected",
    reason: result.reason instanceof Error ? result.reason.message : String(result.reason)
  };
});

console.log("settled results:", printableResults);
console.log("final metrics:", queue.metrics());
