import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { randomId, sha256, subjectDigest } from "../src/contracts.js"
import { createHttpTransport } from "../src/http-transport.js"
import { createNodeClientStorage } from "../src/node-client-storage.js"
import { createNodeServerStorage } from "../src/node-server-storage.js"
import { createClient } from "../src/sync-client.js"
import { createSyncServer } from "../src/node.js"
import { assertNoServerStaging, createHostStore } from "../testing/host.js"
import { listen } from "../testing/listen.js"

const subject = "user.4.google.folder-a"

async function setup(t, files = []) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-batches-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const store = createHostStore(files)
  const applied = []
  const server = createSyncServer({ dataDir: path.join(root, "server"), currentSubject() { return subject } })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry, attachments) => {
    for (const manifest of entry.package.attachments) {
      const bytes = Buffer.from(await attachments.get(manifest.id).read())
      store.put(`server/${manifest.id}`, bytes)
    }
    applied.push(entry.package.data)
    return { attachments: entry.package.attachments.map((manifest) => ({ id: manifest.id, path: `server/${manifest.id}` })) }
  }, { readAttachment: store.readAttachment })
  return { root, store, applied, server, endpoints }
}

// A client whose transport counts each call; `put` may be made to fail.
function countingClient(dataDir, url, store, { failPut, sendBatch } = {}) {
  const real = createHttpTransport({ url })
  const calls = { post: [], put: 0, has: 0 }
  const client = createClient({
    storage: createNodeClientStorage(dataDir),
    transport: {
      post(body) { calls.post.push(body.changes.length); return real.post(body) },
      async put(body) { calls.put += 1; if (failPut?.(calls.put)) throw new Error("network dropped"); return real.put(body) },
      has(hash) { calls.has += 1; return real.has(hash) },
      get: (hash) => real.get(hash),
    },
    sha256,
    randomId,
    ...(sendBatch ? { sendBatch } : {}),
  })
  return { client, calls, use: () => client.use(subject, async () => {}, { readAttachment: store.readAttachment }) }
}

test("a long outbox goes out in batches, in order, and the server applies each as it arrives", async (t) => {
  const { root, store, applied, server, endpoints } = await setup(t)
  const { client, calls, use } = countingClient(path.join(root, "client"), endpoints.url, store, { sendBatch: 100 })
  await use()
  for (let index = 0; index < 250; index += 1) await client.appendChanges({ data: index })
  await client.sync()
  await server.whenIdle()
  assert.deepEqual(calls.post, [100, 100, 50])
  assert.deepEqual(applied, Array.from({ length: 250 }, (_, index) => index))
})

test("a stop part way through the uploads re-uploads nothing already sent", async (t) => {
  const files = Array.from({ length: 4 }, (_, index) => [`p${index}.jpg`, `photo-${index}`])
  const { root, store, applied, server, endpoints } = await setup(t, files)
  const dir = path.join(root, "client")
  const first = countingClient(dir, endpoints.url, store, { failPut: (call) => call === 3 })
  await first.use()
  for (let index = 0; index < 4; index += 1) {
    await first.client.appendChanges({ data: index, attachments: [{ id: `photo-${index}`, path: `p${index}.jpg`, contentType: "image/jpeg" }] })
  }
  const stopped = await first.client.sync()
  assert.equal(stopped.status, "failed")
  assert.equal(first.calls.put, 3)
  // The two photos that went up are recorded on their changes' inflight files.
  const inflight = path.join(dir, "subjects", subjectDigest(subject), "inflight")
  const rows = await Promise.all((await readdir(inflight)).sort().map(async (name) => JSON.parse(await readFile(path.join(inflight, name), "utf8"))))
  assert.deepEqual(rows.map((row) => Boolean(row.package.attachments[0].sha256)), [true, true, false, false])

  const second = countingClient(dir, endpoints.url, store)
  await second.use()
  await second.client.sync()
  await server.whenIdle()
  assert.equal(second.calls.put, 2)
  assert.deepEqual(applied, [0, 1, 2, 3])
  await assertNoServerStaging(path.join(root, "server"))
})

test("bytes the server already holds are not uploaded again, and the change applies from the stored copy", async (t) => {
  const photo = "the-same-photo"
  const { root, store, applied, server, endpoints } = await setup(t, [["a.jpg", photo], ["b.jpg", photo]])
  const a = countingClient(path.join(root, "a"), endpoints.url, store)
  await a.use()
  await a.client.appendChanges({ data: "from a", attachments: [{ id: "photo-a", path: "a.jpg", contentType: "image/jpeg" }] })
  await a.client.sync()
  await server.whenIdle()
  assert.equal(a.calls.put, 1)

  const b = countingClient(path.join(root, "b"), endpoints.url, store)
  await b.use()
  await b.client.appendChanges({ data: "from b", attachments: [{ id: "photo-b", path: "b.jpg", contentType: "image/jpeg" }] })
  await b.client.sync()
  await server.whenIdle()
  assert.equal(b.calls.put, 0, "the server held those bytes")
  assert.deepEqual(applied, ["from a", "from b"])
  assert.equal(store.files.get("server/photo-b").toString(), photo)
  await assertNoServerStaging(path.join(root, "server"))
})

test("HEAD /sync tells whether the host holds bytes", async (t) => {
  const { root, store, server, endpoints } = await setup(t, [["a.jpg", "held-bytes"]])
  const transport = createHttpTransport({ url: endpoints.url })
  const hash = sha256(Buffer.from("held-bytes"))
  assert.equal(await transport.has(hash), false)
  const a = countingClient(path.join(root, "a"), endpoints.url, store)
  await a.use()
  await a.client.appendChanges({ data: "a", attachments: [{ id: "photo-a", path: "a.jpg", contentType: "image/jpeg" }] })
  await a.client.sync()
  await server.whenIdle()
  assert.equal(await transport.has(hash), true)
  assert.equal(await transport.has(sha256(Buffer.from("other"))), false)
})

test("an existing log is indexed by its attachments when the server storage opens", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-index-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const hash = sha256(Buffer.from("x"))
  const first = createNodeServerStorage(root)
  first.appendEntry(subject, "e1", { data: 1, attachments: [{ id: "p", path: "server/p", sha256: hash, size: 1 }] })
  first.close()
  const db = new Database(path.join(root, "syncbridge.sqlite"))
  db.exec("DROP TABLE log_attachments")
  db.close()
  const reopened = createNodeServerStorage(root)
  t.after(() => reopened.close())
  assert.deepEqual(reopened.findAttachment(subject, hash), { id: "p", path: "server/p", sha256: hash, size: 1 })
})

test("a client far behind downloads in pages and reports its progress", async (t) => {
  const { root, store, server, endpoints } = await setup(t)
  const sender = countingClient(path.join(root, "sender"), endpoints.url, store)
  await sender.use()
  for (let index = 0; index < 450; index += 1) await sender.client.appendChanges({ data: index })
  await sender.client.sync()
  await server.whenIdle()

  const real = createHttpTransport({ url: endpoints.url })
  const pages = []
  const received = []
  const receiver = createClient({
    storage: createNodeClientStorage(path.join(root, "receiver")),
    transport: { post: async (body) => { const ack = await real.post(body); pages.push(ack.entries.length); return ack }, put: (body) => real.put(body), get: (hash) => real.get(hash) },
    sha256,
    randomId,
    receivePage: 200,
  })
  await receiver.use(subject, async (entry) => { received.push(entry.package.data) }, { readAttachment: store.readAttachment })
  const seen = []
  const stop = receiver.onProgress((progress) => seen.push(progress))
  const result = await receiver.sync()
  await receiver.whenIdle()
  stop()
  assert.equal(result.status, "succeed")
  assert.deepEqual(pages, [200, 200, 50])
  assert.deepEqual(received, Array.from({ length: 450 }, (_, index) => index))
  assert.deepEqual(seen.at(-1), { sent: 0, toSend: 0, received: 450, toReceive: 450, applying: 0 })
  assert.ok(seen.some((progress) => progress.received === 200 && progress.toReceive === 450))
})

test("a sender's progress counts its changes as they are acked", async (t) => {
  const { root, store, server, endpoints } = await setup(t)
  const sender = countingClient(path.join(root, "sender"), endpoints.url, store, { sendBatch: 100 })
  await sender.use()
  for (let index = 0; index < 250; index += 1) await sender.client.appendChanges({ data: index })
  const seen = []
  sender.client.onProgress((progress) => seen.push(`${progress.sent}/${progress.toSend}`))
  await sender.client.sync()
  await server.whenIdle()
  assert.deepEqual(seen.filter((value, index) => seen.indexOf(value) === index).slice(0, 4), ["0/250", "100/250", "200/250", "250/250"])
})

test("a change the server cannot apply shows in the sender's failed entries", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-failed-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const store = createHostStore()
  const server = createSyncServer({ dataDir: path.join(root, "server"), currentSubject() { return subject } })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry) => { if (entry.package.data === "bad") throw new Error("drive refused") }, { readAttachment: store.readAttachment })
  const sender = countingClient(path.join(root, "sender"), endpoints.url, store)
  await sender.use()
  await sender.client.appendChanges({ data: "good" })
  await sender.client.appendChanges({ data: "bad" })
  await sender.client.sync()
  await server.whenIdle()
  assert.deepEqual(await sender.client.failedEntries(), [])
  await sender.client.sync()
  const failed = await sender.client.failedEntries()
  assert.equal(failed.length, 1)
  assert.equal(failed[0].side, "server")
  assert.match(failed[0].error, /apply failed after 3 attempts: drive refused/)
  await sender.client.sync()
  assert.equal((await sender.client.failedEntries()).length, 1, "reported once")
})
