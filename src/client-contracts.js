// Shared rules for both platforms. No Node-only imports: the Expo client loads this file too.

export const ATTEMPTS = 3

export function requireSubject(subject) {
  if (typeof subject !== "string" || subject.length === 0) throw new TypeError("subject must be a non-empty string")
  return subject
}

export function requireFunction(value, label) {
  if (typeof value !== "function") throw new TypeError(`${label} must be a function`)
  return value
}

export function requireUrl(url) {
  if (typeof url !== "string" || !/^https?:\/\//.test(url)) throw new TypeError("url must be an http(s) URL")
  return url
}

export function requireDataDir(dataDir) {
  if (typeof dataDir !== "string" || dataDir.length === 0) throw new TypeError("dataDir must be a non-empty string")
  return dataDir
}

export function asBytes(value, label) {
  if (value instanceof Uint8Array) return value
  throw new TypeError(`${label} must be bytes`)
}

// A change as the app hands it over: `data` (any JSON) and attachments { id, path, contentType? }.
export function preparePackage(input) {
  if (input === null || typeof input !== "object" || !Object.hasOwn(input, "data")) throw new TypeError("package.data is required")
  const attachments = input.attachments ?? []
  if (!Array.isArray(attachments)) throw new TypeError("package.attachments must be an array")
  const ids = new Set()
  for (const attachment of attachments) {
    if (typeof attachment?.id !== "string" || !attachment.id || ids.has(attachment.id)) throw new TypeError("attachment.id must be a unique non-empty string")
    if (typeof attachment.path !== "string" || !attachment.path) throw new TypeError("attachment.path must be a non-empty string")
    ids.add(attachment.id)
  }
  return {
    data: JSON.parse(JSON.stringify(input.data)),
    attachments: attachments.map(({ id, path, contentType }) => ({ id, path, ...(contentType ? { contentType } : {}) })),
  }
}

// A change as it is logged: each attachment names its own upload and the hash of its bytes.
export function loggedPackage(value) {
  if (value === null || typeof value !== "object" || !Array.isArray(value.attachments)) throw new TypeError("package is invalid")
  for (const manifest of value.attachments) {
    if (typeof manifest?.id !== "string" || typeof manifest.path !== "string") throw new TypeError("attachment is invalid")
    if (typeof manifest.upload !== "string" || !/^[0-9a-f]{64}$/.test(manifest.sha256) || !Number.isInteger(manifest.size)) {
      throw new TypeError("attachment upload is invalid")
    }
  }
  return { data: value.data, attachments: value.attachments }
}

// attachments.get(id).read() for an apply, over bytes already read and verified.
export function attachmentReader(manifests, readBytes) {
  const byId = new Map(manifests.map((manifest) => [manifest.id, manifest]))
  return {
    get(id) {
      const manifest = byId.get(id)
      if (!manifest) throw new Error(`unknown attachment ${id}`)
      return { ...manifest, read: () => readBytes(manifest) }
    },
  }
}

// Runs `work` up to ATTEMPTS times back to back. Resolves to null on success, or to the last error.
// A network failure is not a try: it ends the sync, which resumes here next time.
export async function tryThrice(work) {
  let failure = null
  for (let attempt = 0; attempt < ATTEMPTS; attempt += 1) {
    try {
      await work()
      return null
    } catch (error) {
      if (error?.offline) throw error
      failure = error
    }
  }
  return failure
}

export function messageOf(error) {
  return error instanceof Error && error.message ? error.message : String(error)
}
