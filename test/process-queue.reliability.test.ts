import {describe, expect, test} from "vitest";
import {ProcessQueue} from "../src/index.js";

const sleep = async (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

describe("ProcessQueue reliability smoke", () => {
  test("continues processing after task rejection", async () => {
    const queue = new ProcessQueue({concurrency: 2});
    const results: number[] = [];

    const jobs = [
      queue.enqueue(async () => {
        throw new Error("expected failure");
      }),
      queue.enqueue(async () => {
        await sleep(5);
        results.push(1);
        return 1;
      }),
      queue.enqueue(async () => {
        results.push(2);
        return 2;
      })
    ];

    await expect(jobs[0]).rejects.toThrow("expected failure");
    await expect(jobs[1]).resolves.toBe(1);
    await expect(jobs[2]).resolves.toBe(2);
    await queue.drain();
    expect(results.sort()).toEqual([1, 2]);
  });

  test("can clear and then accept new work", async () => {
    const queue = new ProcessQueue({concurrency: 1});
    const first = queue.enqueue(async () => sleep(20));
    await sleep(5);
    const second = queue.enqueue(async () => "to-clear");
    const clearedCount = queue.clear();

    await first;
    expect(clearedCount).toBe(1);
    void second;

    const recovery = queue.enqueue(async () => "recovered");
    await expect(recovery).resolves.toBe("recovered");
  });
});
