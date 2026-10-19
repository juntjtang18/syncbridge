import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import assert from "node:assert/strict"
import test from "node:test"
import { createSyncClient, createSyncServer } from "../src/node.js"
import { listen } from "../testing/listen.js"

const subject = "user.4.google.folder-a"

// A server whose apply takes `ms` per change and records when each starts and ends.
async function setup(t, { ms = 30, applyConcurrency } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-apply-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const server = createSyncServer({ dataDir: path.join(root, "server"), currentSubject() { return subject }, ...(applyConcurrency ? { applyConcurrency } : {}) })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  const events = []
  let active = 0
  let most = 0
  server.init(subject, async (entry) => {
    active += 1
    most = Math.max(most, active)
    events.push(`start ${entry.package.data.name}`)
    await new Promise((resolve) => setTimeout(resolve, ms))
    events.push(`end ${entry.package.data.name}`)
    active -= 1
  }, { readAttachment: () => { throw new Error("no attachments") }, applyKey: (entry) => entry.package.data.key ?? null })
  const client = createSyncClient({ dataDir: path.join(root, "client"), url: endpoints.url })
  await client.use(subject, async () => {})
  return { server, client, events, most: () => most }
}

test("changes with different keys apply side by side, up to the limit", async (t) => {
  const { server, client, most } = await setup(t, { applyConcurrency: 3 })
  for (const key of ["g1", "g2", "g3", "g4", "g5", "g6"]) await client.appendChanges({ data: { name: key, key } })
  const started = Date.now()
  await client.sync()
  await server.whenIdle()
  assert.equal(most(), 3)
  assert.ok(Date.now() - started < 6 * 30, "faster than one at a time")
})

test("changes with the same key apply one at a time, in log order", async (t) => {
  const { server, client, events, most } = await setup(t)
  for (const name of ["a1", "a2", "a3"]) await client.appendChanges({ data: { name, key: "group-a" } })
  await client.sync()
  await server.whenIdle()
  assert.equal(most(), 1)
  assert.deepEqual(events, ["start a1", "end a1", "start a2", "end a2", "start a3", "end a3"])
})

test("a change without a key waits for everything before it and runs alone", async (t) => {
  const { server, client, events } = await setup(t)
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

test("a restarted server names the subjects whose changes are still waiting", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-pending-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const dataDir = path.join(root, "server")
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const first = createSyncServer({ dataDir, currentSubject() { return subject } })
  const endpoints = await listen(first, t)
  first.init(subject, async () => { await gate })
  const client = createSyncClient({ dataDir: path.join(root, "client"), url: endpoints.url })
  await client.use(subject, async () => {})
  await client.appendChanges({ data: "waiting" })
  await client.sync()
  assert.deepEqual(first.pendingSubjects(), [subject])
  release()
  await first.whenIdle()
  assert.deepEqual(first.pendingSubjects(), [])
  await first.close()
})
