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
      return { sha256: parsed.sha256, size: parsed.size }
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
      return JSON.parse(await requireText(response))
    },

    async get(hash, dest) {
      const response = await request(withHash(url, hash), {
        method: "GET",
        credentials: "include",
        headers: await requestHeaders(headers),
      })
      if (!response.ok) throw await errorFrom(response)
      if (dest && typeof dest.getWriter === "function") {
        if (response.body == null || typeof response.body.pipeTo !== "function") {
          throw new TypeError("fetch response does not expose a readable body stream")
        }
        await response.body.pipeTo(dest)
        return
      }
      return new Uint8Array(await response.arrayBuffer())
    },
  }
}

function withHash(url, hash) {
  const target = new URL(url)
  target.searchParams.set("sha256", hash)
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
  let message
  try {
    const body = JSON.parse(text)
    if (body && typeof body.error === "string") message = body.error
  } catch {
    // Fall back to the response body when the host does not return JSON.
  }
  return new Error(message || text || `request failed with status ${response.status}`)
}
