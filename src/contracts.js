import { createHash, randomUUID } from "node:crypto"
import { requireSubject } from "./client-contracts.js"

export {
  asBytes,
  createAttachmentReader,
  persistedPackage,
  preparePackage,
  requireApplyChange,
  requireDataDir,
  requirePointer,
  requireReadAttachment,
  requireSubject,
  requireUrl,
} from "./client-contracts.js"

export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}

export function subjectDigest(subject) {
  return sha256(Buffer.from(requireSubject(subject), "utf8"))
}

export function randomId() {
  return randomUUID()
}
