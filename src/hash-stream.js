import { createHash } from "node:crypto"
import { createReadStream, createWriteStream } from "node:fs"
import { Transform } from "node:stream"
import { pipeline } from "node:stream/promises"

export async function hashFile(file) {
  const hash = createHash("sha256")
  await pipeline(createReadStream(file), hash)
  return hash.digest("hex")
}

export async function writeHashed(source, file) {
  const hash = createHash("sha256")
  let size = 0
  const counter = new Transform({
    transform(chunk, encoding, callback) {
      hash.update(chunk)
      size += chunk.length
      callback(null, chunk)
    },
  })
  await pipeline(source, counter, createWriteStream(file))
  return { hash: hash.digest("hex"), size }
}
