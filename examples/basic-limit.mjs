import {ProcessQueue} from "../dist/index.js";

const queue = new ProcessQueue({concurrency: 2});

const start = Date.now();

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const timedTask = async (name, ms) => {
  console.log(`[${Date.now() - start}ms] start ${name}`);
  await sleep(ms);
  console.log(`[${Date.now() - start}ms] done  ${name}`);
  return `${name}:ok`;
};

const results = await Promise.all([
  queue.limit(timedTask, "task-a", 350),
  queue.limit(timedTask, "task-b", 150),
  queue.limit(timedTask, "task-c", 120),
  queue.limit(timedTask, "task-d", 200)
]);

await queue.drain();
console.log("results:", results);
console.log("metrics:", queue.metrics());
