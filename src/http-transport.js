import { createReadStream, createWriteStream } from "node:fs"
import http from "node:http"
import https from "node:https"
import { stat } from "node:fs/promises"
import { pipeline } from "node:stream/promises"

export function createHttpTransport({ url, headers }) {
  return {
    async put(hash, filePath) {
      const info = await stat(filePath)
      await send({
        url: withHash(url, hash),
        method: "PUT",
        headers: await requestHeaders(headers, {
          "content-type": "application/octet-stream",
          "content-length": String(info.size),
        }),
        body: createReadStream(filePath),
      })
    },

    async post(body) {
      const payload = Buffer.from(JSON.stringify(body))
      const { status, text } = await send({
        url,
        method: "POST",
        headers: await requestHeaders(headers, {
          "content-type": "application/json",
          "content-length": String(payload.length),
        }),
        body: payload,
        withStatus: true,
      })
      const parsed = text ? JSON.parse(text) : {}
      if (status === 409 && parsed && typeof parsed.subject === "string") return parsed
      if (status < 200 || status >= 300) throw errorFrom(Buffer.from(text || ""))
      return parsed
    },

    async get(hash, filePath) {
      await send({
        url: withHash(url, hash),
        method: "GET",
        headers: await requestHeaders(headers),
        writeTo: filePath,
      })
    },
  }
}

function withHash(url, hash) {
  const target = new URL(url)
  target.searchParams.set("sha256", hash)
  return target
}

async function requestHeaders(headers, extra = {}) {
  const provided = typeof headers === "function" ? await headers() : {}
  if (provided === null || typeof provided !== "object" || Array.isArray(provided)) {
    throw new TypeError("headers must return an object")
  }
  return { ...provided, ...extra }
}

function send({ url, method, headers, body, writeTo, withStatus }) {
  const target = url instanceof URL ? url : new URL(url)
  const lib = target.protocol === "https:" ? https : http
  return new Promise((resolve, reject) => {
    const req = lib.request(target, { method, headers }, (res) => {
      if (!withStatus && (res.statusCode < 200 || res.statusCode >= 300)) {
        const chunks = []
        res.on("data", (chunk) => chunks.push(chunk))
        res.on("end", () => reject(errorFrom(Buffer.concat(chunks))))
        res.on("error", reject)
        return
      }
      if (writeTo) {
        pipeline(res, createWriteStream(writeTo)).then(() => resolve(), reject)
        return
      }
      const chunks = []
      res.on("data", (chunk) => chunks.push(chunk))
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8")
        resolve(withStatus ? { status: res.statusCode, text } : text)
      })
      res.on("error", reject)
    })
    req.on("error", reject)
    if (body && typeof body.pipe === "function") {
      pipeline(body, req).catch(reject)
      return
    }
    req.end(body)
  })
}

function errorFrom(buffer) {
  const text = buffer.toString("utf8")
  try {
    const parsed = JSON.parse(text)
    if (parsed && typeof parsed.error === "string") return new Error(parsed.error)
  } catch {
    // The status body is not JSON.
  }
  return new Error(text || "request failed")
}
