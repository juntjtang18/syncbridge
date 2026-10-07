import * as Crypto from "expo-crypto"
import { fetch } from "expo/fetch"
import { File } from "expo-file-system"

import { requireApplyChange, requireSubject, preparePackage, createAttachmentReader } from "./contracts.js"
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
    transport: createFetchTransport({
      url,
      headers,
      fetch,
      openRead: (fileUri) => new File(fileUri).readableStream(),
      openWrite: (fileUri) => new File(fileUri).writableStream(),
      size: (fileUri) => new File(fileUri).size,
    }),
  })
}

export { createAttachmentReader, preparePackage, requireApplyChange, requireSubject }

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
