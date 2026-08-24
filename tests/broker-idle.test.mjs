import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { makeTempDir } from "./helpers.mjs";
import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";

const ROOT = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const BROKER = path.join(ROOT, "plugins", "codex", "scripts", "app-server-broker.mjs");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, { timeoutMs = 10000, intervalMs = 25 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) {
      return true;
    }
    await delay(intervalMs);
  }
  return false;
}

function startBroker({ idleTimeout } = {}) {
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const sessionDir = makeTempDir("codex-broker-idle-");
  const cwd = makeTempDir("codex-broker-cwd-");
  const socketPath = path.join(sessionDir, "broker.sock");
  const pidFile = path.join(sessionDir, "broker.pid");

  const args = [BROKER, "serve", "--endpoint", `unix:${socketPath}`, "--cwd", cwd, "--pid-file", pidFile];
  if (idleTimeout !== undefined) {
    args.push("--idle-timeout", String(idleTimeout));
  }

  const child = spawn(process.execPath, args, { env: buildEnv(binDir), stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  const exited = new Promise((resolve) => {
    child.on("exit", (code, signal) => resolve({ code, signal }));
  });

  const alive = () => child.exitCode === null && child.signalCode === null;

  // Bounded: a broker that never exits must turn into a RED test, not a hung run.
  // Awaiting `exited` unbounded is what made this file hang against an unpatched broker.
  const exitedWithin = (timeoutMs) =>
    Promise.race([exited, delay(timeoutMs).then(() => null)]);

  const dispose = () => {
    if (alive()) {
      child.kill("SIGKILL");
    }
  };

  return {
    child,
    socketPath,
    pidFile,
    exited,
    exitedWithin,
    dispose,
    stderr: () => stderr,
    alive,
    listening: () => waitFor(() => fs.existsSync(socketPath))
  };
}

function connectTo(socketPath) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath });
    socket.on("connect", () => resolve(socket));
    socket.on("error", reject);
  });
}

test("broker shuts itself down once it has been idle for the timeout", async (t) => {
  const broker = startBroker({ idleTimeout: 400 });
  t.after(() => broker.dispose());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const result = await broker.exitedWithin(8000);
  assert.ok(result, `broker never exited after the idle timeout, stderr: ${broker.stderr()}`);
  assert.equal(result.code, 0, `expected a clean idle exit, stderr: ${broker.stderr()}`);
  // shutdown() must still clean up after itself on the idle path, or the next
  // ensureBrokerSession would find a stale socket and a stale pidfile.
  assert.equal(fs.existsSync(broker.socketPath), false, "idle shutdown left the socket behind");
  assert.equal(fs.existsSync(broker.pidFile), false, "idle shutdown left the pidfile behind");
});

test("broker stays alive while a client is connected, then exits after it disconnects", async (t) => {
  const broker = startBroker({ idleTimeout: 400 });
  t.after(() => broker.dispose());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const socket = await connectTo(broker.socketPath);
  // Well past the idle timeout: an open connection must hold the broker open, which is
  // what stops a long streaming turn from being cut off mid-flight.
  await delay(1600);
  assert.equal(broker.alive(), true, "broker exited while a client was still connected");

  socket.destroy();
  const result = await broker.exitedWithin(8000);
  assert.ok(result, `broker never exited after the client disconnected, stderr: ${broker.stderr()}`);
  assert.equal(result.code, 0, `expected a clean idle exit after disconnect, stderr: ${broker.stderr()}`);
});

test("broker with --idle-timeout 0 never idles out", async (t) => {
  const broker = startBroker({ idleTimeout: 0 });
  t.after(() => broker.dispose());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  await delay(1600);
  assert.equal(broker.alive(), true, "idle shutdown ran even though it was disabled");

  broker.child.kill("SIGTERM");
  await broker.exitedWithin(8000);
});

test("broker rejects a non-numeric --idle-timeout instead of silently never expiring", async (t) => {
  const broker = startBroker({ idleTimeout: "not-a-number" });
  t.after(() => broker.dispose());
  const result = await broker.exitedWithin(8000);
  assert.ok(result, "broker did not exit on an invalid idle timeout");
  assert.equal(result.code, 1);
  assert.match(broker.stderr(), /Invalid idle timeout/);
});

test("blank --idle-timeout falls back to the default instead of silently disabling", async (t) => {
  // Number("  ") === 0, so a whitespace value must NOT be read as "never expire".
  // Only an explicit 0 disables idle shutdown.
  const broker = startBroker({ idleTimeout: "  " });
  t.after(() => broker.dispose());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  // The default is 30 minutes, so it must still be alive well past the short timeouts
  // used elsewhere in this file.
  await delay(1600);
  assert.equal(broker.alive(), true, "a blank idle timeout disabled idle shutdown");

  broker.child.kill("SIGTERM");
  await broker.exitedWithin(8000);
});
