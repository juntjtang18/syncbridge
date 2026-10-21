import path from "node:path"
import assert from "node:assert/strict"
import test from "node:test"
import { createSyncServer } from "../src/node.js"
import { createNodeServerStorage } from "../src/node-server-storage.js"
import { createServer } from "../src/sync-server.js"
import { hostFiles, startClient, startServer, subject, tempDir } from "./helpers.js"

// A server whose apply takes 30 ms and records when each change starts and ends.
async function timedServer(t, root, applyConcurrency = 3) {
  const events = []
  let active = 0
  let most = 0
  const { server, url } = await startServer(t, root, {
    applyConcurrency,
    applyKey: (entry) => entry.package.data.key ?? null,
    apply: async (entry) => {
      active += 1
      most = Math.max(most, active)
      events.push(`start ${entry.package.data.name}`)
      await new Promise((resolve) => setTimeout(resolve, 30))
      events.push(`end ${entry.package.data.name}`)
      active -= 1
    },
  })
  const client = (await startClient(root, "a", url, hostFiles())).client
  return { server, client, events, most: () => most }
}

test("changes with different keys apply side by side, up to the limit", async (t) => {
  const { server, client, most } = await timedServer(t, await tempDir(t))
  for (const key of ["g1", "g2", "g3", "g4", "g5", "g6"]) await client.appendChanges({ data: { name: key, key } })
  const started = Date.now()
  await client.sync()
  await server.whenIdle()
  assert.equal(most(), 3)
  assert.ok(Date.now() - started < 6 * 30)
})

test("changes with the same key apply one at a time, in log order", async (t) => {
  const { server, client, events, most } = await timedServer(t, await tempDir(t))
  for (const name of ["a1", "a2", "a3"]) await client.appendChanges({ data: { name, key: "group-a" } })
  await client.sync()
  await server.whenIdle()
  assert.equal(most(), 1)
  assert.deepEqual(events, ["start a1", "end a1", "start a2", "end a2", "start a3", "end a3"])
})

test("a change without a key waits for everything before it and runs alone", async (t) => {
  const { server, client, events } = await timedServer(t, await tempDir(t))
  await client.appendChanges({ data: { name: "p1", key: "g1" } })
  await client.appendChanges({ data: { name: "p2", key: "g2" } })
  await client.appendChanges({ data: { name: "delete-location" } })
  await client.appendChanges({ data: { name: "p3", key: "g3" } })
  await client.sync()
  await server.whenIdle()
  const at = (event) => events.indexOf(event)
  assert.ok(at("start delete-location") > at("end p1") && at("start delete-location") > at("end p2"))
  assert.ok(at("start p3") > at("end delete-location"))
})

test("a restarted server names the subjects with changes still waiting, and applies them once they are initialized", async (t) => {
  const root = await tempDir(t)
  const dataDir = path.join(root, "restart")
  const first = createSyncServer({ dataDir })
  first.init(subject, async () => {})
  // Logged with apply, but the process stops before the apply runs.
  await first.close()
  const stored = createSyncServer({ dataDir })
  await assert.doesNotReject(async () => {
    const waiting = stored.appendChanges(subject, { data: "queued" }, { apply: true })
    assert.deepEqual(stored.pendingSubjects(), [subject])
    const applied = []
    stored.init(subject, async (entry) => { applied.push(entry.package.data) })
    await waiting
    assert.deepEqual(applied, ["queued"])
    assert.deepEqual(stored.pendingSubjects(), [])
  })
  await stored.close()
})

test("one user's backlog does not hold up another user's changes", async (t) => {
  const root = await tempDir(t)
  const server = createSyncServer({ dataDir: path.join(root, "two-users") })
  t.after(() => server.close())
  const done = []
  const slow = (name) => async (entry) => { await new Promise((resolve) => setTimeout(resolve, 30)); done.push(`${name}:${entry.package.data}`) }
  server.init("user.4", slow("user.4"))
  server.init("user.5", slow("user.5"))
  // user.4 logs ten changes without a key (each waits for the one before), then user.5 logs one.
  const backlog = Array.from({ length: 10 }, (_, index) => server.appendChanges("user.4", { data: index }, { apply: true }))
  await server.appendChanges("user.5", { data: "web edit" }, { apply: true })
  assert.ok(done.indexOf("user.5:web edit") < 3, `user.5 applied after ${done.indexOf("user.5:web edit")} of user.4's changes`)
  await Promise.all(backlog)
  assert.deepEqual(done.filter((item) => item.startsWith("user.4")), Array.from({ length: 10 }, (_, index) => `user.4:${index}`))
})

test("waiting for one user's applies does not wait for another user's", async (t) => {
  const root = await tempDir(t)
  const server = createSyncServer({ dataDir: path.join(root, "idle-per-user") })
  t.after(() => server.close())
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const done = []
  server.init("user.4", async () => { await gate; done.push("user.4") })
  server.init("user.5", async () => { await new Promise((resolve) => setTimeout(resolve, 10)); done.push("user.5") })
  const stuck = server.appendChanges("user.4", { data: "big upload" }, { apply: true })
  const quick = server.appendChanges("user.5", { data: "report visit" }, { apply: true })
  await server.whenIdle("user.5")
  assert.deepEqual(done, ["user.5"])
  release()
  await Promise.all([stuck, quick])
  await server.whenIdle()
  assert.deepEqual(done.sort(), ["user.4", "user.5"])
})

test("a finished apply looks only at its own subject's waiting changes, however many subjects wait", async (t) => {
  const root = await tempDir(t)
  const storage = createNodeServerStorage(path.join(root, "many-users"))
  const reads = new Map()
  const head = storage.unprocessedHead
  storage.unprocessedHead = (subject, limit) => { reads.set(subject, (reads.get(subject) || 0) + 1); return head(subject, limit) }
  const server = createServer({ storage })
  t.after(() => server.close())
  const users = Array.from({ length: 50 }, (_, index) => `user.${index}`)
  for (const user of users) server.init(user, async () => { await new Promise((resolve) => setTimeout(resolve, 2)) })
  await Promise.all(users.map(async (user) => {
    for (let index = 0; index < 4; index += 1) await server.appendChanges(user, { data: index }, { apply: true })
  }))
  // Each subject is read when it is initialized, when a change is logged, and when one of its own
  // applies ends: about ten reads for four changes. A scan of every subject after each of the 200
  // applies would be thousands.
  const most = Math.max(...reads.values())
  assert.ok(most <= 12, `a subject's waiting changes were read ${most} times`)
  assert.equal(reads.size, users.length)
})
