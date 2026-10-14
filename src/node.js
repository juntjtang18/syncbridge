import { randomId, requireDataDir, requireUrl, sha256 } from "./contracts.js"
import { createHttpTransport } from "./http-transport.js"
import { createNodeClientStorage } from "./node-client-storage.js"
import { createNodeServerStorage } from "./node-server-storage.js"
import { createClient } from "./sync-client.js"
import { createServer } from "./sync-server.js"

export function createSyncClient({ dataDir, url, headers, log }) {
  requireDataDir(dataDir)
  requireUrl(url)
  if (headers !== undefined && typeof headers !== "function") {
    throw new TypeError("headers must be a function")
  }
  return createClient({
    storage: createNodeClientStorage(dataDir),
    transport: createHttpTransport({ url, headers }),
    sha256,
    randomId,
    log,
  })
}

export function createSyncServer({ dataDir, currentSubject, log }) {
  requireDataDir(dataDir)
  return createServer({ storage: createNodeServerStorage(dataDir), currentSubject, log })
}
