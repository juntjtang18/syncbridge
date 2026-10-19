// The client's HTTP calls, over fetch (Node 22 and Expo both have it). A non-2xx answer throws an
// error carrying `status`. A request that got no answer, or a 5xx (the server restarting or busy),
// throws one marked `offline`: the sync stops and resumes next time, using none of a change's tries.
// A busy server's 503 also carries `retryAfter`, the seconds it asked the client to wait.
export function createFetchTransport({ url, headers, fetch: fetchImpl }) {
  const request = fetchImpl ?? globalThis.fetch.bind(globalThis)

  async function call(target, init) {
    const extra = typeof headers === "function" ? await headers() : {}
    const response = await request(target, { credentials: "include", ...init, headers: { ...extra, ...init.headers } })
      .catch((error) => { throw Object.assign(error instanceof Error ? error : new Error(String(error)), { offline: true }) })
    if (response.ok) return response
    const text = await response.text().catch(() => "")
    let message = text || `HTTP ${response.status}`
    try { message = JSON.parse(text).error || message } catch { /* not JSON */ }
    const retryAfter = Number(response.headers?.get?.("retry-after"))
    throw Object.assign(new Error(message), {
      status: response.status,
      offline: response.status >= 500,
      ...(Number.isFinite(retryAfter) && retryAfter > 0 ? { retryAfter } : {}),
    })
  }

  return {
    // Uploads bytes; the server answers { upload, sha256, size } for this one upload.
    async put(bytes) {
      const response = await call(url, {
        method: "PUT",
        headers: { "content-type": "application/octet-stream", "content-length": String(bytes.byteLength) },
        body: bytes,
      })
      return response.json()
    },

    async post(body) {
      const response = await call(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })
      return response.json()
    },

    // One attachment of one logged change.
    async get(subject, entry, attachment) {
      const target = new URL(url)
      target.searchParams.set("subject", subject)
      target.searchParams.set("entry", entry)
      target.searchParams.set("attachment", attachment)
      const response = await call(target.toString(), { method: "GET", headers: {} })
      return new Uint8Array(await response.arrayBuffer())
    },
  }
}
