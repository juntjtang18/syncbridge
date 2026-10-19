import path from "node:path"
import assert from "node:assert/strict"
import test from "node:test"
import { createSyncServer } from "../src/node.js"
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
