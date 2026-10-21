import { readFile, readdir } from "node:fs/promises"
import http from "node:http"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { sha256 } from "../src/contracts.js"
import { createFetchTransport } from "../src/fetch-transport.js"
import { hostFiles, startClient, startServer, subject, tempDir, uploadsLeft } from "./helpers.js"

const photo = (id, file) => ({ data: id, attachments: [{ id: `${id}.jpg`, path: file, contentType: "image/jpeg" }] })

test("a change and its photo reach the server and another phone; the sender gets it back without the photo", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url, host, applied } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  const b = await startClient(root, "b", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  assert.deepEqual(await a.client.sync(), { status: "succeed" })
  await server.whenIdle()
  assert.deepEqual(applied, ["one"])
  assert.equal(host.files.get("server/one.jpg").toString(), "photo-a")
  assert.deepEqual(await uploadsLeft(root), [])
  assert.deepEqual(a.applied, [{ data: "one", own: true, bytes: [] }])
  await b.client.sync()
  assert.deepEqual(b.applied, [{ data: "one", own: false, bytes: ["photo-a"] }])
})

test("a long queue goes out in batches of twenty, applied in order", async (t) => {
  const root = await tempDir(t)
  const { server, url, applied } = await startServer(t, root)
  const posts = []
  const a = await startClient(root, "a", url, hostFiles(), { wrap: (real) => ({ ...real, post: (body) => { posts.push(body.changes.length); return real.post(body) } }) })
  for (let index = 0; index < 45; index += 1) await a.client.appendChanges({ data: index })
  await a.client.sync()
  await server.whenIdle()
  assert.deepEqual(posts, [20, 20, 5, 0])
  assert.deepEqual(applied, Array.from({ length: 45 }, (_, index) => index))
})

test("a phone far behind catches up page by page, with progress", async (t) => {
  const root = await tempDir(t)
  const { server, url } = await startServer(t, root)
  const a = await startClient(root, "a", url, hostFiles())
  for (let index = 0; index < 450; index += 1) await a.client.appendChanges({ data: index })
  await a.client.sync()
  await server.whenIdle()
  const pages = []
  const b = await startClient(root, "b", url, hostFiles(), { wrap: (real) => ({ ...real, post: async (body) => { const answer = await real.post(body); pages.push(answer.entries.length); return answer } }) })
  const seen = []
  b.client.onProgress((progress) => seen.push(progress))
  await b.client.sync()
  assert.deepEqual(pages, [200, 200, 50])
  assert.deepEqual(b.applied.map((item) => item.data), Array.from({ length: 450 }, (_, index) => index))
  assert.deepEqual(seen.at(-1), { sent: 0, uploaded: 0, toSend: 0, received: 450, toReceive: 450 })
})

test("a stop part way through the uploads re-uploads nothing already sent", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "0.jpg": "p0", "1.jpg": "p1", "2.jpg": "p2", "3.jpg": "p3" })
  const { server, url, applied } = await startServer(t, root)
  let puts = 0
  const flaky = (real) => ({ ...real, put: (bytes) => { puts += 1; return puts === 3 ? Promise.reject(Object.assign(new Error("network dropped"), { offline: true })) : real.put(bytes) } })
  const a = await startClient(root, "a", url, files, { wrap: flaky })
  for (let index = 0; index < 4; index += 1) await a.client.appendChanges(photo(String(index), `${index}.jpg`))
  await assert.rejects(a.client.sync(), /network dropped/)
  const again = await startClient(root, "a", url, files, { wrap: (real) => ({ ...real, put: (bytes) => { puts += 1; return real.put(bytes) } }) })
  puts = 0
  await again.client.sync()
  await server.whenIdle()
  assert.equal(puts, 2)
  assert.deepEqual(applied, ["0", "1", "2", "3"])
})

test("an upload the server received differently is not sent", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const lying = (real) => ({ ...real, put: async (bytes) => ({ ...(await real.put(bytes)), sha256: "0".repeat(64) }) })
  const a = await startClient(root, "a", url, hostFiles({ "a.jpg": "photo" }), { wrap: lying })
  await a.client.appendChanges(photo("one", "a.jpg"))
  await assert.rejects(a.client.sync(), /arrived changed/)
})

test("a change the server refuses three times goes on the failure list, and the rest still go", async (t) => {
  const root = await tempDir(t)
  const { server, url, applied } = await startServer(t, root)
  // The bad photo's upload never reaches the server: it answers for bytes it does not keep.
  const losing = (real) => ({ ...real, put: async (bytes) => (Buffer.from(bytes).toString() === "bad" ? { upload: "00000000-0000-0000-0000-000000000000", sha256: sha256(bytes), size: bytes.byteLength } : real.put(bytes)) })
  const a = await startClient(root, "a", url, hostFiles({ "bad.jpg": "bad", "good.jpg": "good" }), { wrap: losing })
  await a.client.appendChanges(photo("bad", "bad.jpg"))
  await a.client.appendChanges(photo("good", "good.jpg"))
  await a.client.sync()
  await server.whenIdle()
  assert.deepEqual(applied, ["good"])
  const failures = await a.client.failedEntries()
  assert.equal(failures.length, 1)
  assert.match(failures[0].error, /send failed after 3 attempts: upload of attachment bad.jpg is missing/)
})

test("a change this phone cannot apply three times goes on its failure list, and the rest still apply", async (t) => {
  const root = await tempDir(t)
  const { server, url } = await startServer(t, root)
  const a = await startClient(root, "a", url, hostFiles())
  await a.client.appendChanges({ data: "fine" })
  await a.client.appendChanges({ data: "poison" })
  await a.client.appendChanges({ data: "after" })
  await a.client.sync()
  await server.whenIdle()
  const tries = []
  const b = await startClient(root, "b", url, hostFiles())
  await b.client.init(subject, async (entry) => {
    tries.push(entry.package.data)
    if (entry.package.data === "poison") throw new Error("cannot apply")
  })
  await b.client.sync()
  assert.deepEqual(tries, ["fine", "poison", "poison", "poison", "after"])
  const failures = await b.client.failedEntries()
  assert.deepEqual(failures.map((failure) => [failure.position, failure.error]), [[2, "apply failed after 3 attempts: cannot apply"]])
})

test("a network failure while receiving ends the sync, and the next sync resumes there", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  await a.client.sync()
  await server.whenIdle()
  let offline = true
  const b = await startClient(root, "b", url, files, { wrap: (real) => ({ ...real, get: (...args) => (offline ? Promise.reject(Object.assign(new Error("offline"), { offline: true })) : real.get(...args)) }) })
  await assert.rejects(b.client.sync(), /offline/)
  assert.deepEqual(await b.client.failedEntries(), [])
  offline = false
  await b.client.sync()
  assert.deepEqual(b.applied, [{ data: "one", own: false, bytes: ["photo-a"] }])
})

test("a server apply that fails three times is on the server's failure list and its upload is gone", async (t) => {
  const root = await tempDir(t)
  let tries = 0
  const { server, url } = await startServer(t, root, { apply: async () => { tries += 1; throw new Error("drive refused") } })
  const a = await startClient(root, "a", url, hostFiles({ "a.jpg": "photo" }))
  await a.client.appendChanges(photo("one", "a.jpg"))
  await a.client.sync()
  await server.whenIdle()
  assert.equal(tries, 3)
  assert.match(server.failedEntries(subject)[0].error, /apply failed after 3 attempts: drive refused/)
  assert.deepEqual(await uploadsLeft(root), [])
})

test("two phones uploading the same bytes each keep their own upload, and both apply", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "same", "b.jpg": "same" })
  const { server, url, applied } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  const b = await startClient(root, "b", url, files)
  await a.client.appendChanges(photo("from-a", "a.jpg"))
  await b.client.appendChanges(photo("from-b", "b.jpg"))
  await Promise.all([a.client.sync(), b.client.sync()])
  await server.whenIdle()
  assert.deepEqual(applied.sort(), ["from-a", "from-b"])
  assert.deepEqual(await uploadsLeft(root), [])
})

test("a change sent again after a lost answer is logged once", async (t) => {
  const root = await tempDir(t)
  const { server, url, applied } = await startServer(t, root)
  let first = true
  const lossy = (real) => ({ ...real, post: async (body) => { const answer = await real.post(body); if (first && body.changes.length) { first = false; throw Object.assign(new Error("answer lost"), { offline: true }) } return answer } })
  const a = await startClient(root, "a", url, hostFiles(), { wrap: lossy })
  await a.client.appendChanges({ data: "once" })
  await assert.rejects(a.client.sync(), /answer lost/)
  await a.client.sync()
  await server.whenIdle()
  assert.deepEqual(applied, ["once"])
  assert.deepEqual(server.entriesBetween(subject, 0, 99).map((entry) => entry.package.data), ["once"])
})

test("a phone signed in as someone else is told to sign in again", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const transport = createFetchTransport({ url })
  await assert.rejects(transport.post({ subject: "user.5", pointer: 0, changes: [] }), (error) => error.status === 409 && /sign in again/.test(error.message))
})

test("a host change with apply resolves once applied; without apply it is only logged", async (t) => {
  const root = await tempDir(t)
  const { server, applied } = await startServer(t, root)
  await server.appendChanges(subject, { data: "web edit" }, { apply: true })
  assert.deepEqual(applied, ["web edit"])
  await server.appendChanges(subject, { data: "backfill" })
  await server.whenIdle()
  assert.deepEqual(applied, ["web edit"])
  assert.deepEqual(server.entriesBetween(subject, 0, 99).map((entry) => entry.package.data), ["web edit", "backfill"])
})

test("a host change's attachment is its stored copy: every phone downloads the bytes current at its path", async (t) => {
  const root = await tempDir(t)
  const { server, url, host } = await startServer(t, root)
  host.files.set("p1/v1/report.pdf", Buffer.from("report-1"))
  await server.appendChanges(subject, { data: "report 1", attachments: [{ id: "report.pdf", path: "p1/v1/report.pdf" }] })
  const a = await startClient(root, "a", url, hostFiles())
  await a.client.sync()
  assert.deepEqual(a.applied, [{ data: "report 1", own: false, bytes: ["report-1"] }])
  host.files.set("p1/v1/report.pdf", Buffer.from("report-2, longer"))
  await server.appendChanges(subject, { data: "report 2", attachments: [{ id: "report.pdf", path: "p1/v1/report.pdf" }] })
  // A phone behind by a version gets the current copy for both changes, and nothing fails.
  const b = await startClient(root, "b", url, hostFiles())
  await b.client.sync()
  assert.deepEqual(b.applied.map((entry) => entry.bytes), [["report-2, longer"], ["report-2, longer"]])
  assert.deepEqual(await b.client.failedEntries(), [])
})

test("a change logged before uploads had ids is served from the host's stored copy", async (t) => {
  const root = await tempDir(t)
  const { server, url, host } = await startServer(t, root)
  host.files.set("server/old.jpg", Buffer.from("old-photo"))
  const db = new Database(path.join(root, "server", "syncbridge.sqlite"))
  db.prepare("INSERT INTO log_entries (subject, position, id, package_json) VALUES (?, 1, 'old', ?)")
    .run(subject, JSON.stringify({ data: "old", attachments: [{ id: "p", path: "server/old.jpg", sha256: sha256(Buffer.from("old-photo")), size: 9 }] }))
  db.prepare("INSERT INTO subject_counters (subject, next_position) VALUES (?, 2)").run(subject)
  db.close()
  const b = await startClient(root, "b", url, hostFiles())
  await b.client.sync()
  assert.deepEqual(b.applied, [{ data: "old", own: false, bytes: ["old-photo"] }])
  await server.whenIdle()
})

test("the queue is one file per change, and the pointer and failures sit beside it", async (t) => {
  const root = await tempDir(t)
  const { url } = await startServer(t, root)
  const a = await startClient(root, "a", url, hostFiles())
  await a.client.appendChanges({ data: 1 })
  await a.client.appendChanges({ data: 2 })
  const outbox = await readdir(path.join(root, "a", encodeURIComponent(subject), "outbox"))
  assert.equal(outbox.length, 2)
  assert.deepEqual(outbox.map((name) => name.split("-")[0]), ["000000000001", "000000000002"])
  await a.client.sync()
  assert.deepEqual(await readdir(path.join(root, "a", encodeURIComponent(subject), "outbox")), [])
  assert.equal(await readFile(path.join(root, "a", encodeURIComponent(subject), "pointer"), "utf8"), "2")
})

test("a change added while a sync waits on the network is kept, and goes with the next sync", async (t) => {
  const root = await tempDir(t)
  const { server, url, applied } = await startServer(t, root)
  let release
  const stalled = new Promise((resolve) => { release = resolve })
  let first = true
  const slow = (real) => ({ ...real, post: async (body) => { if (first) { first = false; await stalled; throw Object.assign(new Error("dropped"), { offline: true }) } return real.post(body) } })
  const a = await startClient(root, "a", url, hostFiles(), { wrap: slow })
  await a.client.appendChanges({ data: "first" })
  const sending = a.client.sync().catch((error) => error)
  await a.client.appendChanges({ data: "second" })
  release()
  assert.match((await sending).message, /dropped/)
  await a.client.sync()
  await server.whenIdle()
  assert.deepEqual(applied, ["first", "second"])
})

test("a phone sends its whole queue before it receives, and downloads a few attachments at once", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles(Object.fromEntries(Array.from({ length: 12 }, (_, index) => [`${index}.jpg`, `photo-${index}`])))
  const { server, url } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  for (let index = 0; index < 12; index += 1) await a.client.appendChanges(photo(`a${index}`, `${index}.jpg`))
  await a.client.sync()
  await server.whenIdle()
  const pages = []
  let active = 0
  let most = 0
  const watch = (real) => ({
    ...real,
    post: async (body) => { const answer = await real.post(body); pages.push([body.changes.length, body.page, answer.entries.length]); return answer },
    get: async (...args) => { active += 1; most = Math.max(most, active); await new Promise((resolve) => setTimeout(resolve, 20)); try { return await real.get(...args) } finally { active -= 1 } },
  })
  const b = await startClient(root, "b", url, files, { wrap: watch, sendBatch: 2 })
  for (let index = 0; index < 3; index += 1) await b.client.appendChanges({ data: `b${index}` })
  await b.client.sync()
  assert.deepEqual(pages, [[2, 0, 0], [1, 0, 0], [0, 200, 15]])
  assert.equal(most, 4)
  assert.deepEqual(b.applied.map((item) => item.data), [...Array.from({ length: 12 }, (_, index) => `a${index}`), "b0", "b1", "b2"])
})

test("a server briefly unavailable while receiving ends the sync without giving up any change", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  await a.client.sync()
  await server.whenIdle()
  let restarting = true
  const proxy = (real) => ({ ...real, get: (...args) => (restarting ? Promise.reject(Object.assign(new Error("503 Service Unavailable"), { status: 503, offline: true })) : real.get(...args)) })
  const b = await startClient(root, "b", url, files, { wrap: proxy })
  await assert.rejects(b.client.sync(), /503/)
  assert.deepEqual(await b.client.failedEntries(), [])
  restarting = false
  await b.client.sync()
  assert.deepEqual(b.applied, [{ data: "one", own: false, bytes: ["photo-a"] }])
})

test("the transport marks a 5xx answer offline and a 404 not, and keeps a busy server's Retry-After", async () => {
  const answer = (status, retry) => async () => ({ ok: false, status, headers: new Headers(retry ? { "retry-after": retry } : {}), text: async () => "{\"error\":\"x\"}" })
  const { createFetchTransport } = await import("../src/fetch-transport.js")
  await assert.rejects(createFetchTransport({ url: "http://x/sync", fetch: answer(503) }).post({}), (error) => error.offline === true && error.status === 503 && error.retryAfter === undefined)
  await assert.rejects(createFetchTransport({ url: "http://x/sync", fetch: answer(503, "2") }).post({}), (error) => error.offline === true && error.retryAfter === 2)
  await assert.rejects(createFetchTransport({ url: "http://x/sync", fetch: answer(404) }).post({}), (error) => error.offline === false && error.status === 404)
})

test("an attachment missing from the host is a try, and after three the change is on the failure list", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url, host } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  await a.client.sync()
  await server.whenIdle()
  host.files.delete("server/one.jpg")
  const b = await startClient(root, "b", url, files)
  await b.client.sync()
  assert.match((await b.client.failedEntries())[0].error, /apply failed after 3 attempts: attachment not found/)
})

test("a stored copy that changed on the host is a try for the receiver, and after three the change is on its failure list", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url, host } = await startServer(t, root)
  const a = await startClient(root, "a", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  await a.client.sync()
  await server.whenIdle()
  host.files.set("server/one.jpg", Buffer.from("something else"))
  const b = await startClient(root, "b", url, files)
  await b.client.sync()
  assert.match((await b.client.failedEntries())[0].error, /apply failed after 3 attempts: attachment one\.jpg arrived changed/)
})

test("a server at capacity answers busy at once; the sync keeps its change and it goes once there is room", async (t) => {
  const root = await tempDir(t)
  const files = hostFiles({ "a.jpg": "photo-a" })
  const { server, url, applied } = await startServer(t, root, { maxRequests: 1, busyRetryAfter: 3 })
  // Another client's upload holds the server's one place: its body has not finished arriving.
  const target = new URL(url)
  const held = http.request({ hostname: target.hostname, port: target.port, path: target.pathname, method: "PUT", headers: { "content-length": "10" } })
  held.on("error", () => {})
  held.write("12345")
  await new Promise((resolve) => setTimeout(resolve, 50))
  const a = await startClient(root, "a", url, files)
  await a.client.appendChanges(photo("one", "a.jpg"))
  const started = Date.now()
  await assert.rejects(a.client.sync(), (error) => error.status === 503 && error.offline === true && error.retryAfter === 3)
  assert.ok(Date.now() - started < 1000, "a busy answer comes at once")
  assert.deepEqual(await a.client.failedEntries(), [], "a busy answer uses none of the change's tries")
  held.end("67890")
  await new Promise((resolve) => held.on("response", (response) => { response.resume(); response.on("end", resolve) }))
  assert.deepEqual(await a.client.sync(), { status: "succeed" })
  await server.whenIdle()
  assert.deepEqual(applied, ["one"])
})
