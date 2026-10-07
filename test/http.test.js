import { createHash } from "node:crypto"
import { mkdtemp, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import assert from "node:assert/strict"
import test from "node:test"
import { createSyncClient, createSyncServer } from "../src/node.js"
import { listen } from "../testing/listen.js"

const subject = "tenant.company-123"

test("handle stages a stream and applies the posted change on any host path", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("photo-bytes")
  const photoHash = sha256(photo)
  let applies = 0
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry, attachments) => {
    applies += 1
    assert.equal(entry.position, 1)
    assert.deepEqual(Buffer.from(await attachments.get("photo-1").read()), photo)
  })

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.init(subject, async () => {})
  const appended = await client.appendChanges({
    data: { poi: "north-wall" },
    attachments: [{ id: "photo-1", bytes: photo, contentType: "image/jpeg", name: "poi-1.jpg" }],
  })
  await client.sync()
  assert.equal(applies, 1)

  const duplicate = await fetch(endpoints.also, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      subject,
      pointer: 1,
      changes: [{
        id: appended.id,
        package: {
          data: { poi: "north-wall" },
          attachments: [{
            id: "photo-1",
            sha256: photoHash,
            size: photo.length,
            contentType: "image/jpeg",
            name: "poi-1.jpg",
          }],
        },
      }],
    }),
  })
  assert.equal(duplicate.status, 200)
  assert.deepEqual(await duplicate.json(), {
    acceptedIds: [appended.id],
    entries: [],
    attachmentHashes: [],
    subject,
  })
  assert.equal(applies, 1)

  const downloaded = await fetch(`${endpoints.also}?sha256=${photoHash}`)
  assert.equal(downloaded.status, 200)
  assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()), photo)

  const extra = Buffer.from("from-the-other-path")
  const extraHash = sha256(extra)
  const uploaded = await fetch(`${endpoints.also}?sha256=${extraHash}`, {
    method: "PUT",
    headers: { "content-type": "application/octet-stream", "content-length": String(extra.length) },
    body: extra,
  })
  assert.equal(uploaded.status, 204)
  const fromSync = await fetch(`${endpoints.url}?sha256=${extraHash}`)
  assert.deepEqual(Buffer.from(await fromSync.arrayBuffer()), extra)

  const mismatch = await fetch(`${endpoints.url}?sha256=${extraHash}`, {
    method: "PUT",
    headers: { "content-type": "application/octet-stream" },
    body: Buffer.from("nope"),
  })
  assert.equal(mismatch.status, 400)
  const kept = await fetch(`${endpoints.also}?sha256=${extraHash}`)
  assert.deepEqual(Buffer.from(await kept.arrayBuffer()), extra)
  const temps = await readdir(path.join(dirs.server, "tmp"))
  assert.deepEqual(temps, [])
})

test("POST echoes the live subject and refuses a stale one", async (t) => {
  const dirs = await tempDirs(t)
  const liveB = "user.4.google.folder-b"
  let live = subject
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() {
      return live
    },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})

  const ok = await fetch(endpoints.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ subject, pointer: 0, changes: [] }),
  })
  assert.equal(ok.status, 200)
  assert.equal((await ok.json()).subject, subject)

  live = liveB
  const stale = await fetch(endpoints.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      subject,
      pointer: 0,
      changes: [{ id: "change-a", package: { data: "nope", attachments: [] } }],
    }),
  })
  assert.equal(stale.status, 409)
  assert.deepEqual(await stale.json(), {
    subject: liveB,
    acceptedIds: [],
    entries: [],
    attachmentHashes: [],
  })
})

test("a throwing applyChange rolls the log tip back through the handler", async (t) => {
  const dirs = await tempDirs(t)
  let calls = 0
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {
    calls += 1
    if (calls === 1) throw new Error("server failed")
  })
  const positions = []
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.init(subject, async (entry) => {
    positions.push(entry.position)
  })
  await client.appendChanges({ data: "retry" })
  await assert.rejects(() => client.sync(), /server failed/)
  await client.sync()
  assert.equal(calls, 2)
  assert.deepEqual(positions, [1])
})

async function tempDirs(t) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-http-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  return {
    server: path.join(root, "server"),
    client: path.join(root, "client"),
  }
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}
