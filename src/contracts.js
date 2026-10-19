import { createHash, randomUUID } from "node:crypto"

// Node's hash and ids, for the Node client and the server.
export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}

export function randomId() {
  return randomUUID()
}
