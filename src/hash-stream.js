import { createHash } from "node:crypto"
import { createWriteStream } from "node:fs"
import { Transform } from "node:stream"
import { pipeline } from "node:stream/promises"

// Streams an upload to `file`, hashing it on the way.
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
