import * as Crypto from "expo-crypto"
import { fetch } from "expo/fetch"

import { createExpoClientStorage } from "./expo-client-storage.js"
import { createFetchTransport } from "./fetch-transport.js"
import { createClient } from "./sync-client.js"

export function createSyncClient({ dataDir, url, headers }) {
  requireString(dataDir, "dataDir")
  requireHttpUrl(url)
  if (headers !== undefined && typeof headers !== "function") {
    throw new TypeError("headers must be a function")
  }

  return createClient({
    storage: createExpoClientStorage(dataDir, { sha256, subjectDigest, randomId }),
    transport: createFetchTransport({ url, headers, fetch }),
    sha256,
    randomId,
  })
}

export { createAttachmentReader, preparePackage, requireApplyChange, requireSubject } from "./client-contracts.js"

async function sha256(bytes) {
  const digest = await Crypto.digest(Crypto.CryptoDigestAlgorithm.SHA256, bytes)
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function subjectDigest(subject) {
  return Crypto.digestStringAsync(Crypto.CryptoDigestAlgorithm.SHA256, subject)
}

function randomId() {
  return Crypto.randomUUID()
}

function requireString(value, label) {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${label} must be a non-empty string`)
}

function requireHttpUrl(url) {
  requireString(url, "url")
  const parsed = new URL(url)
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new TypeError("url must be an http(s) URL")
  }
}
