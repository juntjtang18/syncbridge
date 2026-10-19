export function createFetchTransport({ url, headers, fetch }) {
  const request = fetch ?? globalThis.fetch.bind(globalThis)

  return {
    async put(body) {
      const extra = { "content-type": "application/octet-stream" }
      if (body instanceof Uint8Array) extra["content-length"] = String(body.byteLength)
      const response = await request(url, {
        method: "PUT",
        credentials: "include",
        headers: await requestHeaders(headers, extra),
        duplex: "half",
        body,
      })
      const text = await requireText(response)
      const parsed = text ? JSON.parse(text) : {}
      return { upload: parsed.upload, sha256: parsed.sha256, size: parsed.size }
    },

    async post(body) {
      const payload = JSON.stringify(body)
      const response = await request(url, {
        method: "POST",
        credentials: "include",
        headers: await requestHeaders(headers, {
          "content-type": "application/json",
          "content-length": String(new TextEncoder().encode(payload).byteLength),
        }),
        body: payload,
      })
      const text = await response.text()
      let parsed = {}
      if (text) {
        try {
          parsed = JSON.parse(text)
        } catch {
          parsed = {}
        }
      }
      if (response.status === 409 && parsed && typeof parsed.subject === "string") return parsed
      if (!response.ok) throw errorFromText(text, parsed, response.status)
      return parsed
    },

    // One attachment of one logged change: GET /sync?subject=&entry=&attachment=.
    async get(subject, entry, attachment) {
      const response = await request(attachmentUrl(url, subject, entry, attachment), {
        method: "GET",
        credentials: "include",
        headers: await requestHeaders(headers),
      })
      if (!response.ok) throw await errorFrom(response)
      return new Uint8Array(await response.arrayBuffer())
    },
  }
}

function attachmentUrl(url, subject, entry, attachment) {
  const target = new URL(url)
  target.searchParams.set("subject", subject)
  target.searchParams.set("entry", entry)
  target.searchParams.set("attachment", attachment)
  return target.toString()
}

async function requestHeaders(headers, extra = {}) {
  const provided = typeof headers === "function" ? await headers() : {}
  if (provided === null || typeof provided !== "object" || Array.isArray(provided)) {
    throw new TypeError("headers must return an object")
  }
  return { ...provided, ...extra }
}

async function requireText(response) {
  if (response.ok) return response.status === 204 ? "" : response.text()
  throw await errorFrom(response)
}

async function errorFrom(response) {
  const text = await response.text()
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch {
    parsed = null
  }
  return errorFromText(text, parsed, response.status)
}

function errorFromText(text, parsed, status) {
  let message
  if (parsed && typeof parsed.error === "string") message = parsed.error
  return new Error(message || text || `request failed with status ${status}`)
}
