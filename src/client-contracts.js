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

export function requireReadAttachment(readAttachment) {
  if (readAttachment !== undefined && typeof readAttachment !== "function") {
    throw new TypeError("readAttachment must be a function")
  }
  return readAttachment
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

export function requirePointer(pointer) {
  if (!Number.isInteger(pointer) || pointer < 0) {
    throw new TypeError("pointer must be a non-negative integer")
  }
  return pointer
}

export function asBytes(value, label) {
  if (value instanceof Uint8Array) return value
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
  const manifests = []
  for (const attachment of attachments) {
    if (attachment === null || typeof attachment !== "object" || Array.isArray(attachment)) {
      throw new TypeError("attachment must be an object")
    }
    if (typeof attachment.id !== "string" || attachment.id.length === 0) {
      throw new TypeError("attachment.id must be a non-empty string")
    }
    if (ids.has(attachment.id)) throw new TypeError(`duplicate attachment id ${attachment.id}`)
    ids.add(attachment.id)
    if (typeof attachment.path !== "string" || attachment.path.length === 0) {
      throw new TypeError("attachment.path must be a non-empty string")
    }
    manifests.push(queuedManifest(attachment))
  }

  return { package: { data, attachments: manifests } }
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
    if (typeof attachment.path !== "string" || attachment.path.length === 0) {
      throw new TypeError("attachment.path must be a non-empty string")
    }
    if (!/^[0-9a-f]{64}$/.test(attachment.sha256)) throw new TypeError("attachment.sha256 is invalid")
    if (!Number.isInteger(attachment.size) || attachment.size < 0) {
      throw new TypeError("attachment.size must be a non-negative integer")
    }
    return loggedManifest(attachment)
  })
  return { data, attachments }
}

export function createAttachmentReader(manifests, readBytes, digest) {
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
        path: manifest.path,
        async read() {
          const bytes = asBytes(await readBytes(manifest), "attachment")
          if (manifest.sha256 && typeof digest === "function") {
            if (await digest(bytes) !== manifest.sha256 || bytes.byteLength !== manifest.size) {
              throw new Error(`attachment verification failed for ${id}`)
            }
          }
          return bytes
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

function queuedManifest(attachment) {
  const manifest = { id: attachment.id, path: attachment.path }
  if (attachment.contentType !== undefined) {
    if (typeof attachment.contentType !== "string") throw new TypeError("contentType must be a string")
    manifest.contentType = attachment.contentType
  }
  return manifest
}

function loggedManifest(attachment) {
  const manifest = {
    id: attachment.id,
    path: attachment.path,
    sha256: attachment.sha256,
    size: attachment.size,
  }
  if (attachment.contentType !== undefined) {
    if (typeof attachment.contentType !== "string") throw new TypeError("contentType must be a string")
    manifest.contentType = attachment.contentType
  }
  return manifest
}
