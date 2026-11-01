// Re-armable cell alarms on celld (`celld dev`, no Docker).
// Build first: (cd tests/celld/alarm-worker && worker-build --release)
// Owns an isolated celld process group, port and object store; it never touches
// a caller's fleet. Artifacts (logs, results.json) are kept in a temp dir.
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const here = dirname(fileURLToPath(import.meta.url));
const artifacts = await mkdtemp(join(tmpdir(), "distributed-cell-alarms-"));
const project = join(artifacts, "worker");
await mkdir(project);
await cp(join(here, "alarm-worker/build"), join(project, "build"), { recursive: true });
const config = JSON.parse(await readFile(join(here, "alarm-worker/wrangler.jsonc"), "utf8"));
await writeFile(join(project, "wrangler.jsonc"), JSON.stringify(config, null, 2));
const watchdogMs = Number(config.vars.OUTBOX_DRAIN_INTERVAL_MS);

const server = createServer();
server.listen(0, "127.0.0.1");
await once(server, "listening");
const port = server.address().port;
await new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
const base = `http://127.0.0.1:${port}`;
const headers = {
  "content-type": "application/json",
  "x-distributed-internal-secret": config.vars.DISTRIBUTED_INTERNAL_SECRET,
  "x-distributed-service-id": "cell-alarm-conformance",
  "x-distributed-principal-partition": "alice",
  "x-user-id": "alice",
  "x-roles": "user",
};
const nonce = Date.now().toString(36);
// `CELLD_BIN` selects the binary (default: `celld` on PATH). celld 0.6 watches the
// project by default; this proof owns its build, so it opts out when supported.
const celld = process.env.CELLD_BIN || "celld";
const celldVersion = execFileSync(celld, ["--version"], { encoding: "utf8" })
  .split("\n")
  .find(line => line.startsWith("celld "));
const devHelp = execFileSync(celld, ["dev", "--help"], { encoding: "utf8" });
const devFlags = devHelp.includes("--no-watch") ? ["--no-watch"] : [];
let child;
let generation = 0;
let sequence = 0;
const results = [];
const commandId = () => `0190a000-0000-7000-8000-${String(++sequence).padStart(12, "0")}`;

async function until(description, predicate, timeout = 60_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    if (child?.exitCode !== null && child?.exitCode !== undefined) throw new Error("celld exited");
    await delay(100);
  }
  throw new Error(`timed out: ${description}; artifacts: ${artifacts}`);
}

async function start() {
  const log = createWriteStream(join(artifacts, `celld-${++generation}.log`));
  child = spawn(celld, ["dev", project, "--host", "127.0.0.1", "--port", String(port), "--logs", ...devFlags], {
    detached: true,
    env: { ...process.env, RUST_LOG: "warn" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  child.once("close", () => log.end());
  child.once("error", error => console.error(error));
  await until("Worker readiness", async () => {
    try {
      return (await fetch(base + "/health", { signal: AbortSignal.timeout(1000) })).ok;
    } catch {
      return false;
    }
  });
}

// Kill only the celld launcher this script started and its descendants.
async function crash() {
  const owned = child;
  child = undefined;
  if (!owned || owned.exitCode !== null) return;
  const exited = once(owned, "close");
  process.kill(owned.pid, "SIGSTOP");
  const processes = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" })
    .trim()
    .split("\n")
    .map(line => line.trim().split(/\s+/).map(Number));
  const descendants = [];
  (function visit(parent) {
    for (const [pid, ppid] of processes) {
      if (ppid === parent) {
        visit(pid);
        descendants.push(pid);
      }
    }
  })(owned.pid);
  for (const pid of [...descendants, owned.pid]) {
    try {
      process.kill(pid, "SIGKILL");
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  }
  await exited;
}

async function post(id, command, input, extraHeaders = {}) {
  const response = await fetch(`${base}/ticker/${id}/${command}`, {
    method: "POST",
    headers: { ...headers, ...extraHeaders },
    body: JSON.stringify({ commandId: commandId(), input }),
    signal: AbortSignal.timeout(20_000),
  });
  return { status: response.status, body: await response.json() };
}

async function state(id) {
  const response = await fetch(`${base}/ticker/${id}`, { headers, signal: AbortSignal.timeout(10_000) });
  assert.equal(response.status, 200);
  return response.json();
}

async function startTicker(id, limit, periodMs) {
  const started = await post(id, "ticker.start", { limit, period_ms: periodMs, fail_after_arming: false });
  assert.equal(started.status, 200, JSON.stringify(started.body));
  return Date.now();
}

async function record(name, evidence) {
  results.push({ name, evidence });
  console.log(JSON.stringify({ name, evidence }));
  await writeFile(join(artifacts, "results.json"), JSON.stringify(results, null, 2));
}

try {
  console.log(`cell alarm proof artifacts: ${artifacts} (${celldVersion})`);
  await record("celld", { version: celldVersion });
  await start();

  // 1. The alarm row commits with the command or not at all.
  {
    const id = `refused-${nonce}`;
    const refused = await post(id, "ticker.start", { limit: 1, period_ms: 500, fail_after_arming: true });
    assert.equal(refused.status, 422, JSON.stringify(refused.body));
    const after = await state(id);
    assert.equal(after.exists, false);
    assert.deepEqual(after.armed, []);
    await record("failed command leaves no alarm", { status: refused.status, armed: after.armed });
  }

  // 2. Ordinary callers cannot run the alarm-only command.
  {
    const id = `forged-${nonce}`;
    await startTicker(id, 1, 60_000);
    const forged = await post(id, "ticker.tick", {}, { "x-roles": "cell-alarm", "x-user-id": "system:cell-alarm" });
    assert.ok(forged.status >= 400, JSON.stringify(forged.body));
    assert.equal((await state(id)).ticks, 0);
    await record("alarm-only command refuses callers", { status: forged.status, code: forged.body.code });
  }

  // 3. Re-arming in the handler gives a periodic tick at the alarm cadence,
  //    not the 5 s watchdog cadence: domain alarms are never postponed by
  //    watchdog rearms (each tick publishes an outbox event, so the watchdog
  //    is armed throughout).
  {
    const id = `periodic-${nonce}`;
    const limit = 8;
    const periodMs = 500;
    const startedAt = await startTicker(id, limit, periodMs);
    const done = await until("periodic ticks", async () => {
      const current = await state(id);
      return current.ticks >= limit && current;
    }, 30_000);
    const elapsed = Date.now() - startedAt;
    assert.ok(elapsed < limit * periodMs + watchdogMs, `ticks took ${elapsed} ms`);
    await delay(2 * periodMs + 500);
    const settled = await until("periodic outbox drained", async () => {
      const current = await state(id);
      return current.outboxPending === 0 && current;
    }, 3 * watchdogMs);
    assert.equal(settled.ticks, limit, "fires once per arm");
    assert.equal(settled.tickEvents, limit);
    assert.deepEqual(settled.armed, []);
    await record("periodic re-arm", { limit, periodMs, elapsed, ticks: settled.ticks, tickEvents: settled.tickEvents });
  }

  // 4. A far domain alarm never starves the outbox watchdog: a row committed
  //    without the immediate drain is still delivered by the alarm wake, and
  //    the domain alarm stays armed with its original due time.
  {
    const id = `watchdog-${nonce}`;
    await startTicker(id, 1, 3_600_000);
    const armed = (await state(id)).armed;
    assert.equal(armed.length, 1);
    const touched = await post(id, "ticker.touch", {}, { "x-distributed-test-defer-drain": "1" });
    assert.equal(touched.status, 200, JSON.stringify(touched.body));
    const pending = await state(id);
    assert.ok(pending.outboxPending >= 1, JSON.stringify(pending));
    const startedAt = Date.now();
    const drained = await until("watchdog drain", async () => {
      const current = await state(id);
      return current.outboxPending === 0 && current;
    }, 3 * watchdogMs);
    const elapsed = Date.now() - startedAt;
    assert.deepEqual(drained.armed, armed, "the domain alarm survives the watchdog wake");
    assert.equal(drained.ticks, 0);
    await record("watchdog not starved by a far domain alarm", { elapsed, pendingBefore: pending.outboxPending });
  }

  // 5. Fires once per arm across a celld restart: the alarm comes due while
  //    the fleet is down and fires exactly once after it restarts.
  {
    const id = `restart-${nonce}`;
    await startTicker(id, 2, 3_000);
    const before = await state(id);
    assert.equal(before.armed.length, 1);
    await crash();
    await delay(4_000);
    await start();
    const fired = await until("overdue alarm after restart", async () => {
      const current = await state(id);
      return current.ticks >= 1 && current;
    }, 30_000);
    assert.equal(fired.ticks, 1);
    const second = await until("re-armed alarm after restart", async () => {
      const current = await state(id);
      return current.ticks >= 2 && current;
    }, 30_000);
    await delay(4_000);
    const final = await state(id);
    assert.equal(final.ticks, 2, "no duplicate fire after restart");
    assert.equal(final.tickEvents, 2);
    assert.deepEqual(final.armed, []);
    await record("fires once across restart", { generationBefore: before.armed[0].generation, ticks: final.ticks, secondAt: second.nowMs });
  }

  console.log(`cell alarm proof passed (${results.length} checks)`);
} catch (error) {
  console.error(`cell alarm proof FAILED; artifacts: ${artifacts}`);
  throw error;
} finally {
  await crash();
}
