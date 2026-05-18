import {useEffect, useMemo, useRef, useState} from "react";
import {QueueDemoController, type DemoSnapshot, type LogEntry} from "./queueController.js";

function formatNumber(value: number): string {
  return value.toLocaleString(undefined, {maximumFractionDigits: 0});
}

function formatRate(value: number): string {
  return value.toLocaleString(undefined, {maximumFractionDigits: 0});
}

function progressPercent(snapshot: DemoSnapshot): string {
  return `${(snapshot.progress * 100).toFixed(2)}%`;
}

export function App() {
  const controllerRef = useRef<QueueDemoController | null>(null);
  if (!controllerRef.current) {
    controllerRef.current = new QueueDemoController();
  }

  const controller = controllerRef.current;
  const [snapshot, setSnapshot] = useState<DemoSnapshot>(() => controller.snapshot());
  const [logs, setLogs] = useState<LogEntry[]>(() => controller.getRecentLogs());
  const [concurrency, setConcurrency] = useState(64);
  const [delay, setDelay] = useState(0);
  const [batch, setBatch] = useState(8);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const sync = () => {
      setSnapshot(controller.snapshot());
      setLogs(controller.getRecentLogs());
    };

    sync();
    const unsubscribe = controller.subscribe(sync);
    let frame = 0;

    const tick = () => {
      sync();
      frame = requestAnimationFrame(tick);
    };

    frame = requestAnimationFrame(tick);
    return () => {
      unsubscribe();
      cancelAnimationFrame(frame);
    };
  }, [controller]);

  const run = async (action: () => Promise<void> | void) => {
    setBusy(true);
    try {
      await action();
    } finally {
      setBusy(false);
    }
  };

  const pendingMillion = useMemo(
    () => snapshot.stats.enqueued >= 1_000_000 || snapshot.stats.workload === "tasks",
    [snapshot.stats.enqueued, snapshot.stats.workload]
  );

  return (
    <div className="app">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">process-queue live bench</p>
          <h1>One million operations.<br />Full control while it runs.</h1>
          <p className="lede">
            Enqueue, limit, map, addEach, pause, clear, drain, and tune concurrency — all against a real{" "}
            <code>ProcessQueue</code> instance in your browser.
          </p>
        </div>
        <div className="hero-actions">
          <button
            className="cta primary"
            disabled={busy || snapshot.stats.loadInProgress}
            onClick={() => run(() => controller.loadMillion())}
          >
            {snapshot.stats.loadInProgress ? "Loading…" : "Load 1,000,000 tasks"}
          </button>
          <button className="cta ghost" disabled={busy} onClick={() => controller.reset()}>
            Reset
          </button>
        </div>
      </header>

      <section className="stats-grid">
        <StatCard label="Completed" value={formatNumber(snapshot.stats.completed)} accent />
        <StatCard label="Pending" value={formatNumber(snapshot.metrics.pendingCount)} />
        <StatCard label="Active" value={formatNumber(snapshot.metrics.activeCount)} />
        <StatCard label="Throughput" value={`${formatRate(snapshot.throughput)} /s`} />
        <StatCard label="Enqueued" value={formatNumber(snapshot.stats.enqueued)} />
        <StatCard label="Cleared" value={formatNumber(snapshot.stats.cleared)} warn />
      </section>

      <section className="progress-panel">
        <div className="progress-head">
          <span>Progress</span>
          <span>{progressPercent(snapshot)}</span>
        </div>
        <div className="progress-track">
          <div className="progress-fill" style={{width: `${Math.min(snapshot.progress * 100, 100)}%`}} />
        </div>
        <div className="progress-meta">
          <span>{snapshot.metrics.isPaused ? "Paused" : "Running"}</span>
          <span>{snapshot.metrics.isDrained ? "Drained" : "In flight"}</span>
          <span>{(snapshot.elapsedMs / 1000).toFixed(1)}s elapsed</span>
        </div>
      </section>

      <div className="workspace">
        <section className="panel controls">
          <h2>Runtime controls</h2>
          <p className="panel-note">Changes apply immediately to the live queue.</p>

          <label className="slider-field">
            <span>Concurrency — {concurrency}</span>
            <input
              type="range"
              min={1}
              max={512}
              value={concurrency}
              onChange={event => {
                const value = Number(event.target.value);
                setConcurrency(value);
                controller.setConcurrency(value);
              }}
            />
          </label>

          <label className="slider-field">
            <span>Delay — {delay === -1 ? "manual (next only)" : `${delay}ms`}</span>
            <input
              type="range"
              min={-1}
              max={50}
              value={delay}
              onChange={event => {
                const value = Number(event.target.value);
                setDelay(value);
                controller.setDelay(value);
              }}
            />
          </label>

          <label className="slider-field">
            <span>Batch — {batch}</span>
            <input
              type="range"
              min={1}
              max={64}
              value={batch}
              onChange={event => {
                const value = Number(event.target.value);
                setBatch(value);
                controller.setBatch(value);
              }}
            />
          </label>

          <button
            className="chip"
            onClick={() => controller.updateAll({concurrency, delay, batch})}
          >
            update() all sliders
          </button>

          <div className="btn-row">
            <button onClick={() => controller.pause()} disabled={snapshot.metrics.isPaused}>
              pause()
            </button>
            <button onClick={() => controller.resume()} disabled={!snapshot.metrics.isPaused}>
              resume()
            </button>
            <button onClick={() => controller.stepNext(false)}>next()</button>
            <button onClick={() => controller.stepNext(true)}>next(true)</button>
          </div>

          <div className="btn-row danger">
            <button onClick={() => controller.clearPending()}>clear()</button>
            <button onClick={() => run(() => controller.drain())}>drain()</button>
          </div>
        </section>

        <section className="panel operations">
          <h2>API showcase</h2>
          <p className="panel-note">
            {pendingMillion
              ? "Million-task run in progress — try controls on the left."
              : "Load the million run or try smaller API paths below."}
          </p>

          <div className="btn-grid">
            <button disabled={busy} onClick={() => run(() => controller.loadItemPipeline())}>
              addEach() — 50k items + each()
            </button>
            <button disabled={busy} onClick={() => run(() => controller.runMapSample())}>
              map() — 10k sample
            </button>
            <button disabled={busy} onClick={() => controller.injectPriorityBurst()}>
              enqueue(priority) × 500
            </button>
            <button disabled={busy} onClick={() => controller.abortInflightSample()}>
              AbortSignal sample
            </button>
          </div>

          <ul className="api-list">
            <li>
              <code>limit(fn, id)</code> — million-task workload
            </li>
            <li>
              <code>addEach</code> + <code>each()</code> — retry via <code>return true</code>
            </li>
            <li>
              <code>map(iterable, mapper)</code> — batched promises
            </li>
            <li>
              <code>metrics()</code> — active / pending / paused / drained
            </li>
          </ul>
        </section>

        <section className="panel log">
          <h2>Event log</h2>
          <ul>
            {logs.map(entry => (
              <li key={entry.id} data-level={entry.level}>
                <time>{entry.time}</time>
                <span>{entry.message}</span>
              </li>
            ))}
          </ul>
        </section>
      </div>

      <footer>
        <p>
          Deploy to Vercel with root directory <code>demo</code>. Built with Vite + React; bundles{" "}
          <code>process-queue</code> from source.
        </p>
      </footer>
    </div>
  );
}

function StatCard({
  label,
  value,
  accent,
  warn
}: {
  label: string;
  value: string;
  accent?: boolean;
  warn?: boolean;
}) {
  return (
    <article className={`stat-card${accent ? " accent" : ""}${warn ? " warn" : ""}`}>
      <p>{label}</p>
      <strong>{value}</strong>
    </article>
  );
}
