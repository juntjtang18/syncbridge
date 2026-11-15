import { createHash } from "node:crypto"
import { createReadStream, createWriteStream } from "node:fs"
import { Transform } from "node:stream"
import { pipeline } from "node:stream/promises"

// The files read one after another, as one stream.
export async function* filesInTurn(files) {
  for (const file of files) yield* createReadStream(file)
}

// The hash and size of a stream's bytes.
export async function hashOf(source) {
  const hash = createHash("sha256")
  let size = 0
  for await (const chunk of source) {
    hash.update(chunk)
    size += chunk.length
  }
  return { hash: hash.digest("hex"), size }
}

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
