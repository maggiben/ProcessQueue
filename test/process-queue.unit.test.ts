import {describe, expect, test} from "vitest";
import {ProcessQueue} from "../src/index.js";

const sleep = async (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

describe("ProcessQueue unit behavior", () => {
  test("limits concurrency", async () => {
    const queue = new ProcessQueue({concurrency: 2});
    let running = 0;
    let maxRunning = 0;

    const tasks = Array.from({length: 8}, (_, index) =>
      queue.enqueue(async () => {
        running++;
        maxRunning = Math.max(maxRunning, running);
        await sleep(10);
        running--;
        return index;
      })
    );

    const result = await Promise.all(tasks);
    expect(result).toHaveLength(8);
    expect(maxRunning).toBeLessThanOrEqual(2);
  });

  test("supports pause/resume lifecycle", async () => {
    const queue = new ProcessQueue({concurrency: 1, paused: true});
    const events: string[] = [];

    const job = queue.enqueue(async () => {
      events.push("run");
      return "ok";
    });

    await sleep(20);
    expect(events).toEqual([]);

    queue.resume();
    await expect(job).resolves.toBe("ok");
    expect(events).toEqual(["run"]);
  });

  test("clear removes pending tasks", async () => {
    const queue = new ProcessQueue({concurrency: 1});
    const blocker = queue.enqueue(async () => sleep(30));
    await sleep(5);
    const pending = queue.enqueue(async () => "never");
    const clearedCount = queue.clear();

    await blocker;
    expect(clearedCount).toBe(1);
    expect(queue.pendingCount).toBe(0);
    void pending;
  });

  test("supports changing concurrency at runtime", async () => {
    const queue = new ProcessQueue({concurrency: 1});
    let running = 0;
    let maxRunning = 0;

    const tasks = Array.from({length: 6}, () =>
      queue.enqueue(async () => {
        running++;
        maxRunning = Math.max(maxRunning, running);
        await sleep(15);
        running--;
        return true;
      })
    );

    await sleep(20);
    queue.setConcurrency(3);
    await Promise.all(tasks);
    expect(maxRunning).toBeGreaterThanOrEqual(2);
  });
});
