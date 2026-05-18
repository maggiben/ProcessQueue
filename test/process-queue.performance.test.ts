import {describe, expect, test} from "vitest";
import {ProcessQueue} from "../src/index.js";

describe("ProcessQueue performance smoke", () => {
  test("processes a moderate workload without stalling", async () => {
    const queue = new ProcessQueue({concurrency: 25});
    const workload = 1500;
    const started = Date.now();

    const tasks = Array.from({length: workload}, (_, index) => queue.enqueue(async () => index + 1));
    const result = await Promise.all(tasks);
    const elapsed = Date.now() - started;

    expect(result).toHaveLength(workload);
    expect(result[0]).toBe(1);
    expect(elapsed).toBeLessThan(2500);
  });
});
