import * as Crypto from "expo-crypto"
import { fetch } from "expo/fetch"

import { requireDataDir, requireUrl } from "./client-contracts.js"
import { createClientStorage } from "./client-storage.js"
import { createExpoFs } from "./expo-fs.js"
import { createFetchTransport } from "./fetch-transport.js"
import { createClient } from "./sync-client.js"

export function createSyncClient({ dataDir, url, headers, log }) {
  return createClient({
    storage: createClientStorage(createExpoFs(randomId), requireDataDir(dataDir), randomId),
    transport: createFetchTransport({ url: requireUrl(url), headers, fetch }),
    sha256,
    randomId,
    log,
  })
}

async function sha256(bytes) {
  const digest = await Crypto.digest(Crypto.CryptoDigestAlgorithm.SHA256, bytes)
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function randomId() {
  return Crypto.randomUUID()
}
