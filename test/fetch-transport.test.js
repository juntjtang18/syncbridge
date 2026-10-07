import { readFile } from "node:fs/promises"
import assert from "node:assert/strict"
import test from "node:test"
import { createFetchTransport } from "../src/fetch-transport.js"

test("the Expo client graph does not import Node builtins", async () => {
  const files = [
    "expo.js",
    "expo-client-storage.js",
    "fetch-transport.js",
    "sync-client.js",
    "client-contracts.js",
  ]
  for (const file of files) {
    const text = await readFile(new URL(`../src/${file}`, import.meta.url), "utf8")
    assert.doesNotMatch(text, /from ["']node:/, file)
  }
})


const hash = "a".repeat(64)
const bytes = new Uint8Array([137, 80, 78, 71])

test("put and get stream raw file bytes over fetch", async (t) => {
  const originalFetch = globalThis.fetch
  t.after(() => { globalThis.fetch = originalFetch })

  const requests = []
  globalThis.fetch = async (url, init) => {
    requests.push({ url, init })
    if (!init || init.method === "GET") {
      return {
        ok: true,
        status: 200,
        body: readableOf(bytes),
        text: async () => "",
      }
    }
    return { ok: true, status: 200, text: async () => JSON.stringify({ sha256: hash, size: 4 }) }
  }

  const written = []
  const transport = createFetchTransport({ url: "https://inspect.example/sync" })

  const uploaded = await transport.put(bytes)
  await transport.get(hash, writableTo(written, "file:///tmp/download.bin"))

  assert.deepEqual(uploaded, { sha256: hash, size: 4 })
  assert.deepEqual(requests[0].init.body, bytes)
  assert.equal(requests[0].init.duplex, "half")
  assert.equal(requests[0].init.headers["content-type"], "application/octet-stream")
  assert.equal(requests[0].init.headers["content-length"], "4")
  assert.equal(requests[0].url, "https://inspect.example/sync")
  assert.deepEqual(written, [{ fileRef: "file:///tmp/download.bin", body: bytes }])
})

test("post sends JSON and returns the parsed body", async (t) => {
  const originalFetch = globalThis.fetch
  t.after(() => { globalThis.fetch = originalFetch })

  let captured
  globalThis.fetch = async (url, init) => {
    captured = { url, init }
    return { ok: true, status: 200, text: async () => JSON.stringify({ acceptedIds: [] }) }
  }

  const transport = createFetchTransport({ url: "https://inspect.example/sync" })
  const response = await transport.post({ subject: "user.1", pointer: 0, changes: [] })

  assert.deepEqual(response, { acceptedIds: [] })
  assert.equal(captured.init.body, JSON.stringify({ subject: "user.1", pointer: 0, changes: [] }))
  assert.equal(captured.init.headers["content-type"], "application/json")
})

function readableOf(chunk) {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(chunk)
      controller.close()
    },
  })
}

function writableTo(written, fileRef) {
  const chunks = []
  return new WritableStream({
    write(chunk) { chunks.push(chunk) },
    close() { written.push({ fileRef, body: concat(chunks) }) },
  })
}

async function readAll(stream) {
  const reader = stream.getReader()
  const chunks = []
  for (;;) {
    const { done, value } = await reader.read()
    if (done) return concat(chunks)
    chunks.push(value)
  }
}

function concat(chunks) {
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
  const bytes = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.byteLength
  }
  return bytes
}
