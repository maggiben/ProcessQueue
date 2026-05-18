import {describe, expect, test} from "vitest";
import {ProcessQueue} from "../src/index.js";

const sleep = async (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

describe("ProcessQueue integration", () => {
  test("works with mixed limit/map/drain workflow", async () => {
    const queue = new ProcessQueue<number>({concurrency: 3});
    const direct = queue.limit(async (value: number) => value * 2, 3);
    const mapped = queue.map([1, 2, 3, 4], async value => {
      await sleep(5);
      return value * 10;
    });

    await queue.drain();
    await expect(direct).resolves.toBe(6);
    await expect(mapped).resolves.toEqual([10, 20, 30, 40]);
    expect(queue.metrics().isDrained).toBe(true);
  });

  test("supports QueueManager-style item callbacks", async () => {
    const seen: number[] = [];
    const queue = new ProcessQueue<number>({
      concurrency: 1,
      callback(item) {
        seen.push(item as number);
      }
    });

    queue.addEach([1, 2, 3]);
    await queue.drain();
    expect(seen).toEqual([1, 2, 3]);
  });
});
