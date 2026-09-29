import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { createServer, createConnection } from "node:net";
import { join, dirname } from "node:path";

// Test-only preload: pause real filesystem operations in the publisher process.
// No production failpoint or timing window is needed by the reader.
if (process.env.PI_TEST_PUBLICATION_SOCKET) {
  const originalOpen = fs.open;
  let paused = false;
  const pause = async (path, point) => {
    if (paused || point !== (process.env.PI_TEST_PUBLICATION_POINT ?? "after-install")) return;
    paused = true;
    const temporary = (await fs.readdir(dirname(path))).filter(name => name.startsWith(path.split("/").at(-1) + ".") && name.endsWith(".tmp"));
    const info = await fs.lstat(path).catch(error => { if (error.code !== "ENOENT") throw error; return null; });
    const canonical = info ? await fs.realpath(path) : null;
    const aliases = await Promise.all(temporary.map(async name => { const stat = await fs.lstat(join(dirname(path), name)); return { name, dev: stat.dev, ino: stat.ino, nlink: stat.nlink }; }));
    await new Promise((resolve, reject) => {
      const socket = createConnection(process.env.PI_TEST_PUBLICATION_SOCKET);
      socket.on("error", reject);
      socket.on("connect", () => socket.write(JSON.stringify({ path, point, pid: process.pid, temporary, aliases, stat: info && { dev: info.dev, ino: info.ino, nlink: info.nlink, size: info.size, isFile: info.isFile(), isSymbolicLink: info.isSymbolicLink() }, canonical }) + "\n"));
      socket.on("data", () => { socket.end(); resolve(); });
      socket.on("end", resolve);
    });
  };
  fs.open = async function(path, ...args) {
    const handle = await originalOpen.call(this, path, ...args);
    const name = String(path), target = process.env.PI_TEST_PUBLICATION_NAME ?? "result.json";
    if (args[0] === "wx" && name.includes(`/${target}.`) && name.endsWith(".tmp")) {
      const close = handle.close.bind(handle);
      handle.close = async () => { await close(); await pause(name.slice(0, name.lastIndexOf(`/${target}.`)) + `/${target}`, "before-install"); };
    } else if (args[0] === "r") {
      const sync = handle.sync.bind(handle);
      handle.sync = async () => {
        const final = join(name, target);
        if ((await handle.stat()).isDirectory() && await fs.lstat(final).then(() => true, e => { if (e.code !== "ENOENT") throw e; return false; })) await pause(final, "after-install");
        return sync();
      };
    }
    return handle;
  };
  syncBuiltinESMExports();
}

export async function publicationBarrier(root, options = {}) {
  const path = join(root, "publication.sock");
  let peer, resolveReached, rejectReached;
  const reached = new Promise((resolve, reject) => { resolveReached = resolve; rejectReached = reject; });
  // Timeout is only a deadlock guard, never synchronization.
  const timer = setTimeout(() => rejectReached(Error("publisher did not reach filesystem barrier")), 30000);
  const server = createServer(socket => {
    peer = socket; let text = "";
    socket.on("data", chunk => {
      text += chunk;
      if (text.includes("\n")) { clearTimeout(timer); resolveReached(JSON.parse(text.trim())); }
    });
    socket.on("error", rejectReached);
  });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(path, resolve); });
  return {
    reached,
    env: { NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --import=${import.meta.url}`, PI_TEST_PUBLICATION_SOCKET: path, PI_TEST_PUBLICATION_POINT: options.point ?? "after-install", PI_TEST_PUBLICATION_NAME: options.name ?? "result.json" },
    release() { assert(peer, "publisher must reach barrier before release"); peer.write("release\n"); },
    async close() { clearTimeout(timer); peer?.destroy(); await new Promise(resolve => server.close(resolve)); await fs.rm(path, { force: true }); },
  };
}
