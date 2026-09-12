// Build the fixture with worker-build --release --features storage-conformance.
// Owns an isolated celld process group and temporary object store. It never
// restarts a caller's fleet or edits its data; artifacts are retained on failure.
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { access, cp, mkdir, mkdtemp, readFile, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const here = dirname(fileURLToPath(import.meta.url));
const artifacts = await mkdtemp(join(tmpdir(), "distributed-cell-storage-"));
const project = join(artifacts, "worker");
await mkdir(project);
await cp(join(here, "worker/build"), join(project, "build"), { recursive: true });
const config = JSON.parse(await readFile(join(here, "worker/wrangler.jsonc"), "utf8"));
await writeFile(join(project, "wrangler.jsonc"), JSON.stringify(config, null, 2));
const server = createServer();
server.listen(0, "127.0.0.1");
await once(server, "listening");
const port = server.address().port;
await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
const base = `http://127.0.0.1:${port}`;
const headers = {
  "content-type": "application/json",
  "x-distributed-internal-secret": config.vars.DISTRIBUTED_INTERNAL_SECRET,
  "x-distributed-service-id": "cell-storage-conformance",
  "x-distributed-principal-partition": "alice",
  "x-user-id": "alice",
  "x-roles": "user",
};
let child;
let generation = 0;
let commandSequence = 0;
const results = [];
const commandId = () => `0190a000-0000-7000-8000-${String(++commandSequence).padStart(12, "0")}`;
async function until(description, predicate, timeout = 70_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    if (child?.exitCode !== null && child?.exitCode !== undefined) throw new Error("celld exited");
    await delay(200);
  }
  throw new Error(`timed out: ${description}; artifacts: ${project}`);
}
async function start() {
  const log = createWriteStream(join(artifacts, `celld-${++generation}.log`));
  child = spawn("celld", ["dev", project, "--host", "127.0.0.1", "--port", String(port), "--logs"], {
    detached: true,
    env: { ...process.env, RUST_LOG: "warn" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  child.once("close", () => log.end());
  child.once("error", error => console.error(error));
  await until("isolated Worker readiness", async () => {
    try { return (await fetch(base + "/health", { signal: AbortSignal.timeout(1000) })).ok; }
    catch { return false; }
  });
}
async function crash() {
  const owned = child;
  child = undefined;
  if (!owned || owned.exitCode !== null) return;
  const exited = once(owned, "close");
  // celld dev creates a separate process group for its runtime child. Freeze
  // our launcher while resolving its exact descendants so it cannot respawn.
  process.kill(owned.pid, "SIGSTOP");
  const processes = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" })
    .trim().split("\n").map(line => line.trim().split(/\s+/).map(Number));
  const descendants = [];
  function visit(parent) {
    for (const [pid, ppid] of processes) if (ppid === parent) {
      visit(pid);
      descendants.push(pid);
    }
  }
  visit(owned.pid);
  for (const pid of [...descendants, owned.pid]) {
    try { process.kill(pid, "SIGKILL"); }
    catch (error) { if (error.code !== "ESRCH") throw error; }
  }
  await exited;
}
async function post(id, path, body, extraHeaders = {}) {
  return fetch(`${base}/todo/${id}/${path}`, {
    method: "POST", headers: { ...headers, ...extraHeaders },
    body: JSON.stringify(body), signal: AbortSignal.timeout(20_000),
  });
}
async function probe(id, operation) {
  const response = await post(id, "__storage_test", { operation });
  assert.equal(response.status, 200, `${operation}: ${await response.clone().text()}`);
  return operation === "inspect" ? response.json() : response.text();
}
async function command(id, name, input, command = commandId(), extraHeaders = {}) {
  const response = await post(id, name, { commandId: command, input }, extraHeaders);
  const body = await response.json();
  assert.equal(response.status, name === "todo.create" ? 201 : 200, JSON.stringify(body));
  return body;
}
async function queueRows(countOnly = false) {
  const runtime = join(project, ".celld/dev/runtime");
  try {
    const queue = (await readdir(runtime)).find(name => name.startsWith("__Queue:"));
    if (!queue) return [];
    const ltx = join(runtime, queue, "ltx");
    const epochs = (await readdir(ltx)).filter(name => /^e[0-9]+$/.test(name))
      .sort((a, b) => Number(b.slice(1)) - Number(a.slice(1)));
    if (!epochs.length) return [];
    const database = join(ltx, epochs[0], "db.sqlite");
    // Recovery creates the epoch directory before materializing its database.
    // Observe only the newest epoch; never mistake a stale epoch for delivery.
    await access(database);
    let rows;
    try {
      rows = execFileSync("sqlite3", ["-readonly", "-json", database,
      countOnly ? "SELECT COUNT(*) AS count FROM __queue_messages" :
        "SELECT seq, hex(body) AS body FROM __queue_messages ORDER BY seq"], { encoding: "utf8", maxBuffer: 128 * 1024 * 1024 });
    } catch (error) {
      // A retired epoch may disappear between discovery and the read.
      // Missing files are retryable observations; SQL errors are not.
      await access(database);
      throw error;
    }
    return rows.trim() ? JSON.parse(rows) : [];
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}
async function record(name, evidence) {
  results.push({ name, evidence });
  console.log(JSON.stringify({ name, evidence }));
  await writeFile(join(artifacts, "results.json"), JSON.stringify(results, null, 2));
}

try {
  console.log(`Storage proof artifacts: ${artifacts}`);
  await start();
  for (const fault of ["fail-completion", "expire-at-commit"]) {
    const id = fault;
    await probe(id, fault);
    const cid = commandId();
    const failed = await post(id, "todo.create", { commandId: cid, input: { title: fault } });
    assert.ok(failed.status >= 400, await failed.text());
    const rollback = (await probe(id, "inspect")).counts;
    assert.equal(rollback.events, 0);
    assert.equal(rollback.snapshots, 0);
    assert.equal(rollback.completed, 0);
    assert.equal(rollback.outbox, 0);
    await probe(id, "clear-faults");
    await command(id, "todo.create", { title: fault }, cid);
    const recovered = (await probe(id, "inspect")).counts;
    assert.equal(recovered.events, 1);
    assert.equal(recovered.snapshots, 1);
    assert.equal(recovered.completed, 1);
    await record(fault, { rollback, recovered });
  }

  // One command crosses both event and outbox SQL insert chunk boundaries.
  await command("batch", "todo.create", { title: "batch" });
  const batchId = commandId();
  await probe("batch", "fail-completion");
  const failedBatch = await post("batch", "todo.test_batch", {
    commandId: batchId, input: { title: "batch" },
  });
  assert.ok(failedBatch.status >= 400, await failedBatch.text());
  const batchRollback = (await probe("batch", "inspect")).counts;
  assert.equal(batchRollback.events, 1);
  assert.equal(batchRollback.snapshotVersion, 1);
  assert.equal(batchRollback.completed, 1);
  assert.equal(batchRollback.outbox, 0);
  await probe("batch", "clear-faults");
  const batchResult = await command("batch", "todo.test_batch", { title: "batch" }, batchId);
  assert.equal(batchResult.events.length, 32);
  const batchCommit = (await probe("batch", "inspect")).counts;
  assert.equal(batchCommit.events, 33);
  assert.equal(batchCommit.snapshotVersion, 33);
  assert.equal(batchCommit.completed, 2);
  assert.equal(batchCommit.outbox, 0);
  await record("multi-chunk atomic command", { batchRollback, batchCommit });

  // The acceptance/delete gap: the test Worker clears its fault on activation.
  // Only the retained lease and alarm may cause the duplicate send.
  const before = (await queueRows()).length;
  await probe("settlement-crash", "fail-settlement");
  const cid = commandId();
  const original = await command("settlement-crash", "todo.create", { title: "delivery proof" }, cid);
  const accepted = await queueRows();
  assert.equal(accepted.length, before + 1);
  const claimed = await probe("settlement-crash", "inspect");
  assert.equal(claimed.outbox[0].status, "in_flight");
  await crash();
  await start();
  const retried = await until("alarm redelivers after acceptance/delete crash, without a cell request", async () => {
    const rows = await queueRows();
    return rows.length >= before + 2 && rows;
  });
  assert.equal(retried[before].body, retried[before + 1].body, "stable delivery envelope survives restart");
  const replay = await command("settlement-crash", "todo.create", { title: "delivery proof" }, cid);
  assert.equal(replay.receipt.replayed, true);
  assert.deepEqual(replay.payload, original.payload);
  assert.deepEqual(replay.events, original.events);
  const settled = (await probe("settlement-crash", "inspect")).counts;
  assert.equal(settled.outbox, 0);
  assert.equal(settled.events, 1);
  await record("accepted-before-delete restart", settled);

  const beforeDeferred = (await queueRows()).length;
  await command("commit-crash", "todo.create", { title: "watchdog proof" }, commandId(), {
    "x-distributed-test-defer-drain": "1",
  });
  await crash();
  await start();
  await until("prearmed alarm publishes a committed row without another cell request", async () =>
    (await queueRows()).length > beforeDeferred);
  await record("committed-before-send restart", (await probe("commit-crash", "inspect")).counts);

  // Grow event history AND an unsent outbox beyond the former single-value
  // ceiling. Claim failures cannot reject an already committed command.
  const beforeGrowth = (await queueRows(true))[0].count;
  await probe("growth", "fail-claim");
  const payload = "x".repeat(8192);
  const firstId = commandId();
  const first = await command("growth", "todo.create", { title: "0 " + payload }, firstId);
  for (let i = 1; i <= 1100; i++) {
    await command("growth", "todo.rename", { title: i + " " + payload });
    if (i % 100 === 0) console.log(`Growth proof: ${i} appended commands`);
  }
  const grown = (await probe("growth", "inspect")).counts;
  assert.equal(grown.events, 1101);
  assert.ok(grown.eventBytes > 8 * 1024 * 1024);
  assert.equal(grown.snapshotVersion, 1101);
  assert.equal(grown.snapshots, 1);
  assert.equal(grown.completed, 1101);
  assert.equal(grown.outbox, 1101);
  assert.equal(grown.wholeStateTables, 0);
  await crash();
  await start();
  await until("large pending outbox drains from alarms without a cell request", async () =>
    (await queueRows(true))[0]?.count >= beforeGrowth + 1101, 120_000);
  const oldRetry = await command("growth", "todo.create", { title: "0 " + payload }, firstId);
  assert.equal(oldRetry.receipt.replayed, true);
  assert.deepEqual(oldRetry.events, first.events);
  await command("growth", "todo.rename", { title: "after restart" });
  const afterRestart = (await probe("growth", "inspect")).counts;
  assert.equal(afterRestart.events, 1102);
  assert.equal(afterRestart.snapshotVersion, 1102);
  assert.equal(afterRestart.outbox, 0);
  await record("growth and snapshot-tail restart", { grown, afterRestart });
} finally {
  await crash();
  console.log(`Retained storage proof artifacts: ${artifacts}`);
}
