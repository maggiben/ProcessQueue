export class Deque<T> {
  private readonly values: T[] = [];

  get size(): number {
    return this.values.length;
  }

  push(value: T): void {
    this.values.push(value);
  }

  pushFront(value: T): void {
    this.values.unshift(value);
  }

  shift(): T | undefined {
    return this.values.shift();
  }

  clear(): T[] {
    const snapshot = this.values.slice();
    this.values.length = 0;
    return snapshot;
  }

  append(values: readonly T[]): void {
    this.values.push(...values);
  }

  prepend(values: readonly T[]): void {
    this.values.unshift(...values);
  }

  indexOf(value: T): number {
    return this.values.indexOf(value);
  }

  toArray(): T[] {
    return this.values.slice();
  }
}
