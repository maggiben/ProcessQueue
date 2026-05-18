import {ProcessQueue} from "../dist/index.js";

const queue = new ProcessQueue({
  concurrency: 1,
  callback(item) {
    const attempt = attempts.get(item.id) ?? 0;
    const nextAttempt = attempt + 1;
    attempts.set(item.id, nextAttempt);

    console.log(`processing ${item.id} (attempt ${nextAttempt})`);

    if (item.retryOnce && nextAttempt === 1) {
      console.log(`retrying ${item.id}`);
      return true;
    }

    processed.push(item.id);
    return false;
  },
  complete() {
    console.log("queue complete:", processed.join(", "));
  }
});

const attempts = new Map();
const processed = [];

queue.addEach([
  {id: "job-1"},
  {id: "job-2", retryOnce: true},
  {id: "job-3"}
]);

await queue.drain();
console.log("final metrics:", queue.metrics());
