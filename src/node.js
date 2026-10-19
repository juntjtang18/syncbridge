import { requireDataDir, requireUrl } from "./client-contracts.js"
import { createClientStorage } from "./client-storage.js"
import { randomId, sha256 } from "./contracts.js"
import { createFetchTransport } from "./fetch-transport.js"
import { nodeFs } from "./node-fs.js"
import { createNodeServerStorage } from "./node-server-storage.js"
import { createClient } from "./sync-client.js"
import { createServer } from "./sync-server.js"

export function createSyncClient({ dataDir, url, headers, log, sendBatch, receivePage }) {
  return createClient({
    storage: createClientStorage(nodeFs, requireDataDir(dataDir), randomId),
    transport: createFetchTransport({ url: requireUrl(url), headers }),
    sha256,
    randomId,
    log,
    ...(sendBatch ? { sendBatch } : {}),
    ...(receivePage ? { receivePage } : {}),
  })
}

export function createSyncServer({ dataDir, currentSubject, log, applyConcurrency, maxRequests, busyRetryAfter }) {
  return createServer({
    storage: createNodeServerStorage(requireDataDir(dataDir)),
    currentSubject,
    log,
    ...(applyConcurrency ? { applyConcurrency } : {}),
    ...(maxRequests ? { maxRequests } : {}),
    ...(busyRetryAfter ? { busyRetryAfter } : {}),
  })
}
