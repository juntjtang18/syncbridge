import { readdir, rm } from "node:fs/promises"
import path from "node:path"
import assert from "node:assert/strict"
import test from "node:test"
import { sha256 } from "../src/contracts.js"
import { createFetchTransport } from "../src/fetch-transport.js"
import { hostFiles, startClient, startServer, tempDir, uploadsLeft } from "./helpers.js"

const video = (id, file) => ({ data: id, attachments: [{ id: `${id}.mov`, path: file, contentType: "video/quicktime" }] })
const offline = () => Object.assign(new Error("network dropped"), { offline: true })
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// Counts each transport call by name; `fail(name, n)` answers that call's n-th time as a dropped network.
function counting(fail = () => false) {
  const calls = { put: 0, putPart: 0, join: 0 }
  const wrap = (real) => ({
    ...real,
    put: (bytes) => { calls.put += 1; return fail("put", calls.put) ? Promise.reject(offline()) : real.put(bytes) },
    putPart: (upload, index, bytes) => { calls.putPart += 1; return fail("putPart", calls.putPart) ? Promise.reject(offline()) : real.putPart(upload, index, bytes) },
    join: (upload, parts) => { calls.join += 1; return real.join(upload, parts) },
  })
  return { calls, wrap }
}

test("a file larger than a part goes up in parts, is joined whole, and its parts are gone", async (t) => {
  const root = await tempDir(t)
  const { server, url, host, applied } = await startServer(t, root)
  const { calls, wrap } = counting()
  const a = await startClient(root, "a", url, hostFiles({ "v.mov": "0123456789" }), { wrap, partSize: 4 })
  const b = await startClient(root, "b", url, hostFiles())
  await a.client.appendChanges(video("clip", "v.mov"))
  assert.deepEqual(await a.client.sync(), { status: "succeed" })
  await server.whenIdle()
  assert.deepEqual(calls, { put: 0, putPart: 3, join: 1 })
  assert.deepEqual(applied, ["clip"])
  assert.equal(host.files.get("server/clip.mov").toString(), "0123456789")
  assert.deepEqual(await uploadsLeft(root), [])
  await b.client.sync()
  assert.deepEqual(b.applied, [{ data: "clip", own: false, bytes: ["0123456789"] }])
})

test("a file no larger than a part goes up whole, as before parts", async (t) => {
  const root = await tempDir(t)
  const { server, url, applied } = await startServer(t, root)
  const { calls, wrap } = counting()
  const a = await startClient(root, "a", url, hostFiles({ "p.jpg": "1234" }), { wrap, partSize: 4 })
  await a.client.appendChanges(video("photo", "p.jpg"))
  await a.client.sync()
  await server.whenIdle()
  assert.deepEqual(calls, { put: 1, putPart: 0, join: 0 })
  assert.deepEqual(applied, ["photo"])
})

test("a stop part way through a file sends only the parts not yet up", async (t) => {
  const root = await tempDir(t)
  const { server, url, host, applied } = await startServer(t, root)
  const first = counting((name, n) => name === "putPart" && n === 3)
  const files = hostFiles({ "v.mov": "abcdefghijklmnopqrst" })
  const a = await startClient(root, "a", url, files, { wrap: first.wrap, partSize: 4 })
  await a.client.appendChanges(video("clip", "v.mov"))
  await assert.rejects(a.client.sync(), /network dropped/)
  // The app restarts: a new client over the same outbox.
  const again = counting()
  const restarted = await startClient(root, "a", url, files, { wrap: again.wrap, partSize: 4 })
  await restarted.client.sync()
  await server.whenIdle()
  assert.deepEqual(again.calls, { put: 0, putPart: 3, join: 1 })
  assert.deepEqual(applied, ["clip"])
  assert.equal(host.files.get("server/clip.mov").toString(), "abcdefghijklmnopqrst")
})

test("parts go a few at once when asked, and arrive whole", async (t) => {
  const root = await tempDir(t)
  const { server, url, host } = await startServer(t, root)
  let running = 0
  let most = 0
  const slow = (real) => ({
    ...real,
    putPart: async (upload, index, bytes) => {
      running += 1
      most = Math.max(most, running)
      await wait(20)
      try { return await real.putPart(upload, index, bytes) } finally { running -= 1 }
    },
  })
  const a = await startClient(root, "a", url, hostFiles({ "v.mov": "abcdefghijklmnopqrstuvwxyz" }), { wrap: slow, partSize: 2, partsAtOnce: 3 })
  await a.client.appendChanges(video("clip", "v.mov"))
  await a.client.sync()
  await server.whenIdle()
  assert.equal(most, 3)
  assert.equal(host.files.get("server/clip.mov").toString(), "abcdefghijklmnopqrstuvwxyz")
})

test("the server joins parts that came in any order, and a join asked again gives the same answer", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const transport = createFetchTransport({ url })
  const upload = "5a1c0de0-0000-4000-8000-000000000001"
  for (const [index, text] of [[2, "ef"], [0, "ab"], [1, "cd"]]) await transport.putPart(upload, index, Buffer.from(text))
  const joined = await transport.join(upload, 3)
  assert.deepEqual(joined, { upload, sha256: sha256(Buffer.from("abcdef")), size: 6 })
  assert.deepEqual(await transport.join(upload, 3), joined)
  assert.deepEqual(await uploadsLeft(root), [upload])
})

test("a join with a part missing is 409 with the missing list", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const transport = createFetchTransport({ url })
  const upload = "5a1c0de0-0000-4000-8000-000000000002"
  await transport.putPart(upload, 1, Buffer.from("cd"))
  await assert.rejects(transport.join(upload, 3), (error) => error.status === 409 && !error.offline)
  const answer = await fetch(`${url}?upload=${upload}&parts=3`, { method: "PUT" })
  assert.equal(answer.status, 409)
  assert.deepEqual((await answer.json()).missing, [0, 2])
})

test("parts the server lost: the join fails, and the next sync sends the file afresh", async (t) => {
  const root = await tempDir(t)
  const { server, url, host, applied } = await startServer(t, root)
  // The server loses the parts after they arrive, once.
  let lost = false
  const losing = (real) => ({
    ...real,
    join: async (upload, parts) => {
      if (!lost) {
        lost = true
        await rm(path.join(root, "server", "uploads", `${upload}.parts`), { recursive: true, force: true })
      }
      return real.join(upload, parts)
    },
  })
  const a = await startClient(root, "a", url, hostFiles({ "v.mov": "0123456789" }), { wrap: losing, partSize: 4 })
  await a.client.appendChanges(video("clip", "v.mov"))
  await assert.rejects(a.client.sync(), /arrived changed/)
  assert.deepEqual(await a.client.sync(), { status: "succeed" })
  await server.whenIdle()
  assert.deepEqual(applied, ["clip"])
  assert.equal(host.files.get("server/clip.mov").toString(), "0123456789")
})

test("parts begun for a file that then changed start over", async (t) => {
  const root = await tempDir(t)
  const { server, url, host } = await startServer(t, root)
  const files = hostFiles({ "v.mov": "0123456789" })
  const first = counting((name, n) => name === "putPart" && n === 2)
  const a = await startClient(root, "a", url, files, { wrap: first.wrap, partSize: 4 })
  await a.client.appendChanges(video("clip", "v.mov"))
  await assert.rejects(a.client.sync(), /network dropped/)
  files.files.set("v.mov", Buffer.from("ABCDEFGHIJ"))
  const again = counting()
  const restarted = await startClient(root, "a", url, files, { wrap: again.wrap, partSize: 4 })
  await restarted.client.sync()
  await server.whenIdle()
  assert.deepEqual(again.calls, { put: 0, putPart: 3, join: 1 })
  assert.equal(host.files.get("server/clip.mov").toString(), "ABCDEFGHIJ")
})

test("a part number out of range is refused", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const upload = "5a1c0de0-0000-4000-8000-000000000003"
  for (const query of [`upload=${upload}&part=-1`, `upload=${upload}&part=x`, `upload=${upload}&parts=0`, `upload=${upload}&parts=100001`]) {
    const answer = await fetch(`${url}?${query}`, { method: "PUT", body: "ab" })
    assert.equal(answer.status, 400, query)
  }
  assert.deepEqual((await readdir(path.join(root, "server", "uploads"))).filter((name) => !name.startsWith(".")), [])
})
