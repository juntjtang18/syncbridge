import { createHash, randomUUID } from "node:crypto"

export function requireSubject(subject) {
  if (typeof subject !== "string" || subject.length === 0) {
    throw new TypeError("subject must be a non-empty string")
  }
  return subject
}

export function requireApplyChange(applyChange) {
  if (typeof applyChange !== "function") {
    throw new TypeError("applyChange must be a function")
  }
  return applyChange
}

export function requireUrl(url) {
  if (typeof url !== "string" || url.length === 0) {
    throw new TypeError("url must be a non-empty string")
  }
  const parsed = new URL(url)
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new TypeError("url must be an http(s) URL")
  }
  return url
}

export function requireDataDir(dataDir) {
  if (typeof dataDir !== "string" || dataDir.length === 0) {
    throw new TypeError("dataDir must be a non-empty string")
  }
  return dataDir
}

export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}

export function subjectDigest(subject) {
  return sha256(Buffer.from(requireSubject(subject), "utf8"))
}

export function randomId() {
  return randomUUID()
}

export function asBytes(value, label) {
  if (value instanceof Uint8Array) return Buffer.from(value)
  throw new TypeError(`${label} must be bytes`)
}

export function preparePackage(input) {
  if (input === null || typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("package must be an object")
  }
  if (!Object.hasOwn(input, "data")) throw new TypeError("package.data is required")
  const data = jsonData(input.data)
  const attachments = input.attachments ?? []
  if (!Array.isArray(attachments)) throw new TypeError("package.attachments must be an array")

  const ids = new Set()
  const prepared = []
  for (const attachment of attachments) {
    if (attachment === null || typeof attachment !== "object" || Array.isArray(attachment)) {
      throw new TypeError("attachment must be an object")
    }
    if (typeof attachment.id !== "string" || attachment.id.length === 0) {
      throw new TypeError("attachment.id must be a non-empty string")
    }
    if (ids.has(attachment.id)) throw new TypeError(`duplicate attachment id ${attachment.id}`)
    ids.add(attachment.id)
    if (!Object.hasOwn(attachment, "bytes")) throw new TypeError("attachment.bytes is required")
    const bytes = asBytes(attachment.bytes, "attachment.bytes")
    const digest = sha256(bytes)
    prepared.push({
      manifest: manifestOf(attachment, digest, bytes.length),
      bytes,
    })
  }

  return {
    package: { data, attachments: prepared.map((item) => item.manifest) },
    blobs: prepared.map((item) => ({ sha256: item.manifest.sha256, bytes: item.bytes })),
  }
}

export function persistedPackage(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("package must be an object")
  }
  const data = jsonData(value.data)
  if (!Array.isArray(value.attachments)) throw new TypeError("package.attachments must be an array")
  const ids = new Set()
  const attachments = value.attachments.map((attachment) => {
    if (attachment === null || typeof attachment !== "object" || Array.isArray(attachment)) {
      throw new TypeError("attachment must be an object")
    }
    if (typeof attachment.id !== "string" || attachment.id.length === 0) {
      throw new TypeError("attachment.id must be a non-empty string")
    }
    if (ids.has(attachment.id)) throw new TypeError(`duplicate attachment id ${attachment.id}`)
    ids.add(attachment.id)
    if (!/^[0-9a-f]{64}$/.test(attachment.sha256)) throw new TypeError("attachment.sha256 is invalid")
    if (!Number.isInteger(attachment.size) || attachment.size < 0) {
      throw new TypeError("attachment.size must be a non-negative integer")
    }
    return manifestOf(attachment, attachment.sha256, attachment.size)
  })
  return { data, attachments }
}

export function requirePointer(pointer) {
  if (!Number.isInteger(pointer) || pointer < 0) {
    throw new TypeError("pointer must be a non-negative integer")
  }
  return pointer
}

export function createAttachmentReader(manifests, readBytes) {
  const byId = new Map(manifests.map((manifest) => [manifest.id, manifest]))
  return {
    get(id) {
      const manifest = byId.get(id)
      if (!manifest) throw new Error(`unknown attachment ${id}`)
      return {
        id: manifest.id,
        sha256: manifest.sha256,
        size: manifest.size,
        contentType: manifest.contentType,
        name: manifest.name,
        async read() {
          const bytes = await readBytes(manifest.sha256)
          if (sha256(bytes) !== manifest.sha256 || bytes.length !== manifest.size) {
            throw new Error(`attachment verification failed for ${id}`)
          }
          return new Uint8Array(bytes)
        },
      }
    },
  }
}

function jsonData(data) {
  let encoded
  try {
    encoded = JSON.stringify(data)
  } catch {
    throw new TypeError("package.data must be JSON")
  }
  if (encoded === undefined) throw new TypeError("package.data must be JSON")
  const parsed = JSON.parse(encoded)
  if (JSON.stringify(parsed) !== JSON.stringify(data)) throw new TypeError("package.data must be JSON")
  return parsed
}

function manifestOf(attachment, digest, size) {
  const manifest = { id: attachment.id, sha256: digest, size }
  if (attachment.contentType !== undefined) {
    if (typeof attachment.contentType !== "string") throw new TypeError("contentType must be a string")
    manifest.contentType = attachment.contentType
  }
  if (attachment.name !== undefined) {
    if (typeof attachment.name !== "string") throw new TypeError("name must be a string")
    manifest.name = attachment.name
  }
  return manifest
}
