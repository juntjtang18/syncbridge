import { mkdtemp, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { randomId, sha256 } from "../src/contracts.js"
import { createClientStorage } from "../src/client-storage.js"
import { createFetchTransport } from "../src/fetch-transport.js"
import { nodeFs } from "../src/node-fs.js"
import { createSyncServer } from "../src/node.js"
import { createClient } from "../src/sync-client.js"
import { listen } from "../testing/listen.js"

export const subject = "user.4"

export async function tempDir(t) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  return root
}

// Files a host keeps: path → bytes. readAttachment reads from it; an apply writes into it.
export function hostFiles(initial = {}) {
  const files = new Map(Object.entries(initial).map(([name, text]) => [name, Buffer.from(text)]))
  return {
    files,
    readAttachment(manifest) {
      const bytes = files.get(manifest.path)
      if (!bytes) return null
      return new Uint8Array(bytes)
    },
  }
}

// A server on a real HTTP port whose apply records each change and stores its attachments.
export async function startServer(t, root, { apply, applyConcurrency, applyKey, maxRequests, busyRetryAfter } = {}) {
  const host = hostFiles()
  const applied = []
  const server = createSyncServer({ dataDir: path.join(root, "server"), currentSubject: () => subject, applyConcurrency, maxRequests, busyRetryAfter })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, apply ?? (async (entry, attachments) => {
    const moved = []
    for (const manifest of entry.package.attachments) {
      host.files.set(`server/${manifest.id}`, Buffer.from(await attachments.get(manifest.id).read()))
      moved.push({ id: manifest.id, path: `server/${manifest.id}` })
    }
    applied.push(entry.package.data)
    return { attachments: moved }
  }), { readAttachment: host.readAttachment, applyKey })
  return { server, url: endpoints.url, host, applied }
}

// A client whose transport can be wrapped (to count, delay or fail calls).
export async function startClient(root, name, url, files, { wrap = (transport) => transport, sendBatch, receivePage } = {}) {
  const applied = []
  const client = createClient({
    storage: createClientStorage(nodeFs, path.join(root, name), randomId),
    transport: wrap(createFetchTransport({ url })),
    sha256,
    randomId,
    ...(sendBatch ? { sendBatch } : {}),
    ...(receivePage ? { receivePage } : {}),
  })
  await client.init(subject, async (entry, attachments) => {
    const bytes = await Promise.all(entry.package.attachments.map(async (manifest) => Buffer.from(await attachments.get(manifest.id).read()).toString()))
    applied.push({ data: entry.package.data, own: Boolean(entry.own), bytes })
  }, { readAttachment: files.readAttachment })
  return { client, applied }
}

export async function uploadsLeft(root) {
  return (await readdir(path.join(root, "server", "uploads"))).filter((name) => !name.endsWith(".tmp"))
}
