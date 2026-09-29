import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, lstat, link, symlink, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { writeImmutableJson, writeImmutableBytes } from "../extensions/dag-workflow/worker-runtime/core.mjs";
import { publicationBarrier } from "./fixtures/immutable-publication-barrier.mjs";

const root = await mkdtemp(join(tmpdir(), "immutable-publication-"));
let count = 0;
async function scenario(kind, label, point, run) {
  const directory = join(root, `${kind}-${count}`); await mkdir(directory);
  const path = join(directory, "result.json"), barrier = await publicationBarrier(directory, { point });
  const write = (value = "first") => kind === "json" ? writeImmutableJson(path, { value }) : writeImmutableBytes(path, value);
  const bytes = value => kind === "json" ? `${JSON.stringify({ value }, null, 2)}\n` : value;
  const child = spawn(process.execPath, [fileURLToPath(new URL("./fixtures/immutable-publisher.mjs", import.meta.url)), kind, path, kind === "json" ? JSON.stringify({ value: "first" }) : "first"], { env: { ...process.env, ...barrier.env }, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; }); child.stderr.on("data", chunk => { stderr += chunk; });
  const exited = new Promise((resolve, reject) => { child.once("error", reject); child.once("close", (code, signal) => resolve({ code, signal })); });
  try {
    const publication = await barrier.reached;
    await run({ path, directory, publication, barrier, child, exited, write, bytes, output: () => ({ stdout, stderr }) });
    console.log(`PASS immutable ${kind}: ${label}`); count++;
  } finally { child.kill("SIGKILL"); await exited; await barrier.close(); }
}
try {
  for (const kind of ["json", "bytes"]) {
    await scenario(kind, "single-link visibility and concurrent identical/conflicting replay", "after-install", async ({ path, publication, barrier, exited, write, bytes }) => {
      assert.equal(publication.stat.nlink, 1); assert.deepEqual(publication.temporary, []);
      assert.equal(await readFile(path, "utf8"), bytes("first"));
      assert.equal(await write(), false);
      await assert.rejects(write("second"), /already exists with different/);
      assert.equal((await lstat(path)).ino, publication.stat.ino);
      barrier.release(); assert.equal((await exited).code, 0);
    });
    for (const same of [false, true]) await scenario(kind, `no-clobber destination race (${same ? "identical" : "different"})`, "before-install", async ({ path, publication, barrier, exited, write, bytes, output }) => {
      assert.equal(publication.stat, null); assert.equal(publication.temporary.length, 1);
      assert.equal(await write(same ? "first" : "second"), true);
      const winner = await lstat(path);
      barrier.release(); const exit = await exited;
      assert.equal(exit.code, same ? 0 : 1);
      if (same) assert.deepEqual(JSON.parse(output().stdout), { published: false });
      else assert.match(output().stderr, /race produced different/);
      assert.equal((await lstat(path)).ino, winner.ino);
      assert.equal(await readFile(path, "utf8"), bytes(same ? "first" : "second"));
    });
    for (const point of ["before-install", "after-install"]) await scenario(kind, `SIGKILL ${point} and exact replay`, point, async ({ path, publication, child, exited, write, bytes }) => {
      child.kill("SIGKILL"); assert.equal((await exited).signal, "SIGKILL");
      if (point === "before-install") await assert.rejects(lstat(path), { code: "ENOENT" });
      else { assert.equal(publication.stat.nlink, 1); assert.equal(await readFile(path, "utf8"), bytes("first")); }
      assert.equal(await write(), point === "before-install");
      assert.equal((await lstat(path)).nlink, 1);
      if (point === "after-install") assert.equal((await lstat(path)).ino, publication.stat.ino);
      assert.equal(await readFile(path, "utf8"), bytes("first"));
      await assert.rejects(write("second"), /already exists with different/);
    });
    await scenario(kind, "foreign pre-install alias is not hidden or removed", "before-install", async ({ path, directory, publication, barrier, exited, bytes }) => {
      const foreign = join(directory, "foreign-alias");
      await link(join(directory, publication.temporary[0]), foreign);
      barrier.release(); assert.equal((await exited).code, 0);
      assert.equal((await lstat(path)).nlink, 2);
      assert.equal((await lstat(foreign)).ino, (await lstat(path)).ino);
      assert.equal(await readFile(path, "utf8"), bytes("first"));
    });
    await scenario(kind, "no-clobber symlink installed by competitor", "before-install", async ({ path, directory, barrier, exited, output }) => {
      const missing = join(directory, "missing-target"); await symlink(missing, path);
      barrier.release(); assert.equal((await exited).code, 1);
      assert.equal((await lstat(path)).isSymbolicLink(), true);
      await assert.rejects(lstat(missing), { code: "ENOENT" });
      assert.match(output().stderr, /ENOENT/);
    });
  }
  console.log(`${count} deterministic immutable publication scenarios passed`);
} finally { await rm(root, { recursive: true, force: true }); }
