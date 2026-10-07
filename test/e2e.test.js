import { mkdtemp, readFile, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { randomId, sha256, subjectDigest } from "../src/contracts.js"
import { createFetchTransport } from "../src/fetch-transport.js"
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
  assert.deepEqual(first, { subject: subjectA })
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.equal(appliedA.length, 2)
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectA])

  live = subjectB
  const switched = await client.sync()
  assert.deepEqual(switched, { switched: true, subject: subjectB })
  assert.equal(await client.current(), subjectB)
  assert.equal(await pointerOf(dirs.client, subjectB), "0")
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.equal(appliedA.length, 2)
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectA])

  await client.use(subjectB, async (entry) => {
    appliedB.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "project", action: "add", id: "metro", name: "Metro Bridge" },
  })
  const syncedB = await client.sync()
  assert.deepEqual(syncedB, { subject: subjectB })
  assert.equal(await pointerOf(dirs.client, subjectB), "1")
  assert.deepEqual(appliedB, [
    { object: "project", action: "add", id: "metro", name: "Metro Bridge" },
  ])
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
  assert.equal(await pointerOf(dirs.client, subjectA), "1")

  live = subjectB
  assert.deepEqual(await client.sync(), { switched: true, subject: subjectB })
  await client.use(subjectB, async () => {})
  await client.appendChanges({
    data: { object: "project", action: "add", id: "metro", name: "Metro Bridge" },
  })
  await client.sync()
  assert.equal(await pointerOf(dirs.client, subjectB), "1")

  live = subjectA
  const back = await client.sync()
  assert.deepEqual(back, { switched: true, subject: subjectA })
  assert.equal(await client.current(), subjectA)
  assert.equal(await pointerOf(dirs.client, subjectA), "1")
  assert.equal(await pointerOf(dirs.client, subjectB), "1")
  assert.deepEqual(appliedA, [
    { object: "project", action: "add", id: "harbor", name: "Harbor Yard" },
  ])

  await client.use(subjectA, async (entry) => {
    appliedA.push(entry.package.data)
  })
  await client.appendChanges({
    data: { object: "inspection", action: "add", projectId: "harbor", id: "2026-10-07", title: "North wall" },
  })
  const continued = await client.sync()
  assert.deepEqual(continued, { subject: subjectA })
  assert.equal(await pointerOf(dirs.client, subjectA), "2")
  assert.deepEqual(logSubjects(dirs.server), [subjectA, subjectB, subjectA])
})

test("e2e: a Node client syncs a file onto the server and staging is deleted", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("node-photo")
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

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subjectA, async () => {}, { readAttachment: store.readAttachment })
  await client.appendChanges({
    data: { object: "photo", action: "add" },
    attachments: [{ id: "photo-1", path: "shot.jpg", contentType: "image/jpeg" }],
  })
  await assertNoClientStaging(dirs.client)
  await client.sync()
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
  assert.deepEqual(applied, [photo])
  await assertNoServerStaging(dirs.server)
  await assertNoClientStaging(dirs.client)
})

async function pointerOf(dataDir, subject) {
  return readFile(path.join(dataDir, "subjects", subjectDigest(subject), "pointer"), "utf8")
}

function logSubjects(serverDir) {
  const db = new Database(path.join(serverDir, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT subject FROM log_entries ORDER BY rowid").all()
  db.close()
  return rows.map((row) => row.subject)
}

async function tempDirs(t) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-e2e-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  return {
    server: path.join(root, "server"),
    client: path.join(root, "client"),
  }
}
