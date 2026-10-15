import { once } from "node:events"
import http from "node:http"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { randomId, sha256, subjectDigest } from "../src/contracts.js"
import { createFetchTransport } from "../src/fetch-transport.js"
import { createHttpTransport } from "../src/http-transport.js"
import { createNodeClientStorage } from "../src/node-client-storage.js"
import { createClient } from "../src/sync-client.js"
import { createSyncClient, createSyncServer } from "../src/node.js"
import { assertNoClientStaging, assertNoServerStaging, createHostStore } from "../testing/host.js"
import { listen } from "../testing/listen.js"

const subjectA = "user.4.google.folder-a"
const subjectB = "user.4.google.folder-b"

test("e2e: after changelog sync, a new unseen subject starts at pointer 0", async (t) => {
  const dirs = await tempDirs(t)
  const appliedA = []
  const appliedB = []
  let live = subjectA
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() {
      return live
    },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async () => {})
  server.init(subjectB, async () => {})

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async (entry) => {
    appliedA.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "project", action: "add", id: "harbor", name: "Harbor Yard" },
  })
  await client.appendChanges({
    data: { object: "inspection", action: "add", projectId: "harbor", id: "2026-10-07", title: "North wall" },
  })
  const first = await client.sync()
  await client.whenIdle()
  assert.deepEqual(first, { status: "succeed", subject: subjectA })
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.equal(appliedA.length, 0)
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectA])

  live = subjectB
  const switched = await client.sync()
  assert.deepEqual(switched, { status: "switched", switched: true, subject: subjectB })
  assert.equal(await client.current(), subjectB)
  assert.equal(await pointerOf(dirs.client, subjectB), "0")
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.equal(appliedA.length, 0)
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectA])

  await client.use(subjectB, async (entry) => {
    appliedB.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "project", action: "add", id: "metro", name: "Metro Bridge" },
  })
  const syncedB = await client.sync()
  await client.whenIdle()
  assert.deepEqual(syncedB, { status: "succeed", subject: subjectB })
  assert.equal(await pointerOf(dirs.client, subjectB), "1")
  assert.deepEqual(appliedB, [])
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectA, subjectB])
})

test("e2e: after changelog sync, an old known subject reuses its pointer", async (t) => {
  const dirs = await tempDirs(t)
  const appliedA = []
  let live = subjectA
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() {
      return live
    },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async () => {})
  server.init(subjectB, async () => {})

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async (entry) => {
    appliedA.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "project", action: "add", id: "harbor", name: "Harbor Yard" },
  })
  await client.sync()
  await client.whenIdle()
  assert.equal(await pointerOf(dirs.client, subjectA), "1")

  live = subjectB
  assert.deepEqual(await client.sync(), { status: "switched", switched: true, subject: subjectB })
  await client.use(subjectB, async () => {})
  await client.appendChanges({
    data: { object: "project", action: "add", id: "metro", name: "Metro Bridge" },
  })
  await client.sync()
  assert.equal(await pointerOf(dirs.client, subjectB), "1")

  live = subjectA
  const back = await client.sync()
  assert.deepEqual(back, { status: "switched", switched: true, subject: subjectA })
  assert.equal(await client.current(), subjectA)
  assert.equal(await pointerOf(dirs.client, subjectA), "1")
  assert.equal(await pointerOf(dirs.client, subjectB), "1")
  assert.deepEqual(appliedA, [])

  await client.use(subjectA, async (entry) => {
    appliedA.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "inspection", action: "add", projectId: "harbor", id: "2026-10-07", title: "North wall" },
  })
  const continued = await client.sync()
  await client.whenIdle()
  assert.deepEqual(continued, { status: "succeed", subject: subjectA })
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectB, subjectA])
})

test("e2e: a Node client syncs a file onto the server and staging is deleted", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("node-photo")
  const store = createHostStore([["shot.jpg", photo]])
  const applied = []
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() { return subjectA },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async (entry, attachments) => {
    applied.push(Buffer.from(await attachments.get("photo-1").read()))
    store.put("server/shot.jpg", photo)
    return { attachments: [{ id: "photo-1", path: "server/shot.jpg" }] }
  }, { readAttachment: store.readAttachment })

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async () => {}, { readAttachment: store.readAttachment })
  await client.appendChanges({
    data: { object: "photo", action: "add" },
    attachments: [{ id: "photo-1", path: "shot.jpg", contentType: "image/jpeg" }],
  })
  await assertNoClientStaging(dirs.client)
  await client.sync()
  await server.whenIdle()
  await client.whenIdle()
  assert.deepEqual(applied, [photo])
  await assertNoServerStaging(dirs.server)
  await assertNoClientStaging(dirs.client)

  const received = []
  const other = createSyncClient({ dataDir: path.join(path.dirname(dirs.client), "client-b"), url: endpoints.url })
  await other.use(subjectA, async (entry, attachments) => {
    received.push({
      path: entry.package.attachments[0].path,
      bytes: Buffer.from(await attachments.get("photo-1").read()),
    })
  }, { readAttachment: store.readAttachment })
  await other.sync()
  await other.whenIdle()
  assert.deepEqual(received, [{ path: "server/shot.jpg", bytes: photo }])
})

test("e2e: an Expo-style client syncs a file onto the server and staging is deleted", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.alloc(64 * 1024, 0x41)
  const store = createHostStore([["shot.jpg", photo]])
  const applied = []
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async (entry, attachments) => {
    applied.push(Buffer.from(await attachments.get("photo-1").read()))
    store.put("server/shot.jpg", photo)
    return { attachments: [{ id: "photo-1", path: "server/shot.jpg" }] }
  })

  const client = createClient({
    storage: createNodeClientStorage(dirs.client),
    transport: createFetchTransport({ url: endpoints.url }),
    sha256,
    randomId,
  })
  await client.use(subjectA, async () => {}, { readAttachment: store.readAttachment })
  await client.appendChanges({
    data: { object: "photo", action: "add" },
    attachments: [{ id: "photo-1", path: "shot.jpg", contentType: "image/jpeg" }],
  })
  await client.sync()
  await server.whenIdle()
  await client.whenIdle()
  assert.deepEqual(applied, [photo])
  await assertNoServerStaging(dirs.server)
  await assertNoClientStaging(dirs.client)
})

test("e2e: a sender keeps a refused change for two acks and records it after the third", async (t) => {
  const dirs = await tempDirs(t)
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  const applied = []
  server.init(subjectA, async (entry) => {
    applied.push(entry.package.data)
  })

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async () => {})
  await client.setOutbox([
    {
      id: "bad",
      attempts: 0,
      package: {
        data: "refused",
        attachments: [{ id: "a", path: "a.jpg", sha256: "ab".repeat(32), size: 1 }],
      },
    },
    {
      id: "good",
      attempts: 0,
      package: { data: "kept", attachments: [] },
    },
  ])

  const first = await client.sync()
  assert.equal(first.status, "failed")
  assert.equal(server.failedEntries(subjectA).length, 0)
  assert.deepEqual(logData(dirs.server), [])
  assert.deepEqual((await client.outbox()).map((row) => row.id), ["bad", "good"])

  const second = await client.sync()
  assert.equal(second.status, "failed")
  assert.deepEqual(logData(dirs.server), [])

  const third = await client.sync()
  await server.whenIdle()
  assert.equal(third.status, "succeed")
  const failures = await client.failedEntries()
  assert.equal(failures.length, 1)
  assert.match(failures[0].error, /send failed after 3 attempts/)
  assert.equal(server.failedEntries(subjectA).length, 0)
  assert.deepEqual(await client.outbox(), [])
  assert.deepEqual(logData(dirs.server), ["kept"])
  assert.deepEqual(applied, ["kept"])
})

test("e2e: a receiver apply fails three times, then a later entry still applies", async (t) => {
  const dirs = await tempDirs(t)
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  let calls = 0
  const applied = []
  server.init(subjectA, async (entry) => {
    calls += 1
    if (entry.package.data === "poison") throw new Error("apply failed")
    applied.push(entry.package.data)
  })

  server.setUnprocessed(subjectA, [
    { id: "poison", position: 1, attempts: 0, package: { data: "poison", attachments: [] } },
  ])
  await server.whenIdle()
  assert.equal(calls, 3)
  assert.deepEqual(applied, [])
  assert.deepEqual(server.unprocessed(subjectA), [])
  const failed = server.failedEntries(subjectA)
  assert.equal(failed.length, 1)
  assert.equal(failed[0].kind, "apply")
  assert.match(failed[0].error, /apply failed after 3 attempts: apply failed/)

  server.setUnprocessed(subjectA, [
    { id: "later", position: 2, attempts: 0, package: { data: "later", attachments: [] } },
  ])
  await server.whenIdle()
  assert.equal(calls, 4)
  assert.deepEqual(applied, ["later"])
  assert.deepEqual(server.unprocessed(subjectA), [])
})

test("e2e: a robot client records the host log for a sync and a failed apply", async (t) => {
  const dirs = await tempDirs(t)
  const serverLog = []
  const robotLog = []
  const server = createSyncServer({
    dataDir: dirs.server,
    log: { write(event) { serverLog.push(event) } },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async (entry) => {
    if (entry.package.data.poi === "poison") throw new Error("apply failed")
  })
  const robot = createRobotClient({
    dataDir: dirs.client,
    url: endpoints.url,
    log: { write(event) { robotLog.push(event) } },
  })

  const synced = await robot.syncChange(subjectA, { poi: "north-wall" })
  await server.whenIdle()
  assert.equal(synced.status, "succeed")
  assert.deepEqual(steps(robotLog), ["append", "post", "ack", "pointer"])
  assert.deepEqual(steps(serverLog), ["post", "enqueue", "ack", "apply", "applied"])
  assert.equal(robotLog.find((event) => event.step === "ack").status, "succeed")
  assert.equal(robotLog[0].side, "client")
  assert.equal(serverLog[0].side, "server")
  assert.equal(robotLog[0].subject, subjectA)

  robotLog.length = 0
  serverLog.length = 0
  const failed = await robot.syncChange(subjectA, { poi: "poison" })
  await server.whenIdle()
  assert.equal(failed.status, "succeed")
  const applyFailures = serverLog.filter((event) => event.step === "apply-failed")
  assert.equal(applyFailures.length, 3)
  assert.match(applyFailures[2].error, /apply failed/)
  assert.equal(applyFailures[2].attempt, 3)
  assert.ok(robotLog.some((event) => event.step === "ack" && event.status === "succeed"))
  assert.equal(robotLog.some((event) => event.step === "applied"), false)
})

function createRobotClient({ dataDir, url, log }) {
  const client = createSyncClient({ dataDir, url, log })
  return {
    async syncChange(subject, data) {
      await client.use(subject, async () => {})
      await client.appendChanges({ data })
      const result = await client.sync()
      await client.whenIdle()
      return result
    },
  }
}

function steps(events) {
  return events.map((event) => event.step)
}

test("e2e: uploaded changes are skipped on the way down in the same sync", async (t) => {
  const dirs = await tempDirs(t)
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async () => {})

  const applied = []
  const posts = []
  const real = createHttpTransport({ url: endpoints.url })
  const client = createClient({
    storage: createNodeClientStorage(dirs.client),
    transport: {
      async post(body) {
        const response = await real.post(body)
        posts.push({ body, response })
        return response
      },
      put: (body) => real.put(body),
      get: (hash) => real.get(hash),
    },
    sha256,
    randomId,
  })
  await client.use(subjectA, async (entry) => {
    applied.push(entry.package.data)
  })
  const first = await client.appendChanges({ data: "one" })
  const second = await client.appendChanges({ data: "two" })
  const synced = await client.sync()
  await client.whenIdle()

  assert.equal(synced.status, "succeed")
  assert.equal(posts.length, 1)
  const sentIds = posts[0].body.changes.map((change) => change.id)
  assert.deepEqual(sentIds, [first.id, second.id])
  assert.deepEqual(posts[0].response.acceptedIds, sentIds)
  assert.deepEqual(posts[0].response.entries, [])
  assert.deepEqual(posts[0].response.echoed.map((row) => row.id), sentIds)
  assert.deepEqual(posts[0].response.echoed.map((row) => row.position), [1, 2])
  assert.deepEqual(applied, [])
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.deepEqual(logData(dirs.server), ["one", "two"])

  const otherApplied = []
  const other = createSyncClient({
    dataDir: path.join(path.dirname(dirs.client), "client-b"),
    url: endpoints.url,
  })
  await other.use(subjectA, async (entry) => {
    otherApplied.push(entry.package.data)
  })
  await other.sync()
  await other.whenIdle()
  assert.deepEqual(otherApplied, ["one", "two"])
  assert.equal(await pointerOf(path.join(path.dirname(dirs.client), "client-b"), subjectA), "2")
})

test("e2e: the client that uploaded a change does not download it", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("own-photo")
  const store = createHostStore([["shot.jpg", photo]])
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() { return subjectA },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subjectA, async (entry, attachments) => {
    const manifest = entry.package.attachments?.[0]
    if (!manifest) return
    const bytes = Buffer.from(await attachments.get(manifest.id).read())
    store.put("server/shot.jpg", bytes)
    return { attachments: [{ id: manifest.id, path: "server/shot.jpg" }] }
  }, { readAttachment: store.readAttachment })

  const receiverDir = path.join(path.dirname(dirs.client), "client-b")
  const receiverApplied = []
  const receiver = createSyncClient({ dataDir: receiverDir, url: endpoints.url })
  await receiver.use(subjectA, async (entry, attachments) => {
    const manifest = entry.package.attachments?.[0]
    receiverApplied.push({
      data: entry.package.data,
      bytes: manifest ? Buffer.from(await attachments.get(manifest.id).read()) : null,
    })
  }, { readAttachment: store.readAttachment })
  await receiver.appendChanges({ data: "first" })
  await receiver.sync()
  await receiver.whenIdle()
  assert.deepEqual(receiverApplied, [])
  assert.equal(await pointerOf(receiverDir, subjectA), "1")

  let senderGets = 0
  const senderApplied = []
  const real = createHttpTransport({ url: endpoints.url })
  const sender = createClient({
    storage: createNodeClientStorage(dirs.client),
    transport: {
      post: (body) => real.post(body),
      put: (body) => real.put(body),
      get(hash) {
        senderGets += 1
        return real.get(hash)
      },
    },
    sha256,
    randomId,
  })
  await sender.use(subjectA, async (entry) => {
    senderApplied.push(entry.package.data)
  }, { readAttachment: store.readAttachment })
  await sender.appendChanges({
    data: "photo",
    attachments: [{ id: "photo-1", path: "shot.jpg", contentType: "image/jpeg" }],
  })
  await sender.sync()
  await sender.whenIdle()
  await server.whenIdle()
  assert.deepEqual(senderApplied, ["first"])
  assert.equal(senderGets, 0)
  assert.equal(await pointerOf(dirs.client, subjectA), "2")

  await receiver.sync()
  await receiver.whenIdle()
  assert.deepEqual(receiverApplied, [{ data: "photo", bytes: photo }])
  assert.equal(await pointerOf(receiverDir, subjectA), "2")

  await server.appendChanges(subjectA, { data: "from-server" })
  await sender.sync()
  await sender.whenIdle()
  assert.deepEqual(senderApplied, ["first", "from-server"])
  assert.equal(senderGets, 0)
  assert.equal(await pointerOf(dirs.client, subjectA), "3")

  const authors = logAuthors(dirs.server)
  assert.equal(authors[0], (await readFile(path.join(receiverDir, "client-id"), "utf8")).trim())
  assert.equal(authors[1], (await readFile(path.join(dirs.client, "client-id"), "utf8")).trim())
  assert.equal(authors[2], null)
})

test("e2e: a later append lands in a new incoming queue after the previous one becomes inflight", async (t) => {
  const dirs = await tempDirs(t)
  const gates = [createGate(), createGate(), createGate()]
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listenHeld(server, t, gates)
  server.init(subjectA, async () => {})
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async () => {})
  await client.appendChanges({ data: "original" })
  const syncing = client.sync()
  const digest = subjectDigest(subjectA)
  const root = path.join(dirs.client, "subjects", digest)

  await gates[0].started
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["original"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), [])
  await client.appendChanges({ data: "a1" })
  await client.appendChanges({ data: "a2" })
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["original"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), ["a1", "a2"])
  gates[0].release()

  await gates[1].started
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["a1", "a2"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), [])
  await client.appendChanges({ data: "b1" })
  await client.appendChanges({ data: "b2" })
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["a1", "a2"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), ["b1", "b2"])
  gates[1].release()

  await gates[2].started
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["b1", "b2"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), [])
  gates[2].release()

  const synced = await syncing
  await server.whenIdle()
  assert.equal(synced.status, "succeed")
  assert.deepEqual(logData(dirs.server), ["original", "a1", "a2", "b1", "b2"])
  assert.deepEqual(await client.outbox(), [])
})

test("e2e: a restart sends the remaining inflight row and every incoming change", async (t) => {
  const dirs = await tempDirs(t)
  let trap = null
  let entered
  const started = new Promise((resolve) => { entered = resolve })
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listenOnce(server, t, {
    capture(req) {
      trap = req
      entered()
    },
  })
  server.init(subjectA, async () => {})
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async () => {})
  await client.appendChanges({ data: "kept" })
  const syncing = client.sync()
  const digest = subjectDigest(subjectA)
  const root = path.join(dirs.client, "subjects", digest)
  await started
  await client.appendChanges({ data: "photo" })
  assert.deepEqual(await queueData(path.join(root, "inflight")), ["kept"])
  assert.deepEqual(await queueData(path.join(root, "incoming")), ["photo"])
  trap.destroy()
  await assert.rejects(syncing)

  const restarted = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await restarted.use(subjectA, async () => {})
  const synced = await restarted.sync()
  await server.whenIdle()
  assert.equal(synced.status, "succeed")
  assert.deepEqual(logData(dirs.server), ["kept", "photo"])
  assert.deepEqual(await restarted.outbox(), [])
})

function createGate() {
  let release
  let entered
  return {
    started: new Promise((resolve) => { entered = resolve }),
    wait() {
      entered()
      return new Promise((resolve) => { release = resolve })
    },
    release() {
      release()
    },
  }
}

async function listenHeld(syncServer, t, gates) {
  const pending = [...gates]
  const server = http.createServer(async (req, res) => {
    if (req.method === "POST" && pending.length > 0) await pending.shift().wait()
    syncServer.handle(req, res)
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()))
  }))
  return { url: `http://127.0.0.1:${server.address().port}/sync` }
}

async function listenOnce(syncServer, t, trap) {
  let captured = false
  const server = http.createServer((req, res) => {
    if (req.method === "POST" && !captured) {
      captured = true
      trap.capture(req)
      return
    }
    syncServer.handle(req, res)
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()))
  }))
  return { url: `http://127.0.0.1:${server.address().port}/sync` }
}

async function queueData(directory) {
  const names = (await readdir(directory)).filter((name) => name.endsWith(".json")).sort()
  const data = []
  for (const name of names) {
    data.push(JSON.parse(await readFile(path.join(directory, name), "utf8")).package.data)
  }
  return data
}

async function pointerOf(dataDir, subject) {
  return readFile(path.join(dataDir, "subjects", subjectDigest(subject), "pointer"), "utf8")
}

function logSubjects(serverDir) {
  const db = new Database(path.join(serverDir, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT subject FROM log_entries ORDER BY rowid").all()
  db.close()
  return rows.map((row) => row.subject)
}

function logAuthors(serverDir) {
  const db = new Database(path.join(serverDir, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT client_id FROM log_entries ORDER BY position").all()
  db.close()
  return rows.map((row) => row.client_id)
}

function logData(serverDir) {
  const db = new Database(path.join(serverDir, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT package_json FROM log_entries ORDER BY position").all()
  db.close()
  return rows.map((row) => JSON.parse(row.package_json).data)
}

async function tempDirs(t) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-e2e-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  return {
    server: path.join(root, "server"),
    client: path.join(root, "client"),
  }
}
