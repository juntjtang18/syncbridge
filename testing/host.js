import { readdir } from "node:fs/promises"
import path from "node:path"
import assert from "node:assert/strict"

export function createHostStore(initial = []) {
  const files = new Map(initial.map(([filePath, bytes]) => [filePath, Buffer.from(bytes)]))
  return {
    files,
    put(filePath, bytes) {
      files.set(filePath, Buffer.from(bytes))
    },
    readAttachment(manifest) {
      const bytes = files.get(manifest.path)
      if (!bytes) throw new Error(`missing attachment ${manifest.path}`)
      return bytes
    },
  }
}

export async function assertNoServerStaging(dataDir) {
  for (const name of ["blobs", "attachments", "tmp"]) {
    await assertEmptyDir(path.join(dataDir, name))
  }
}

export async function assertNoClientStaging(dataDir) {
  let subjects
  try {
    subjects = await readdir(path.join(dataDir, "subjects"))
  } catch (error) {
    if (error.code === "ENOENT") return
    throw error
  }
  for (const name of subjects) {
    await assertEmptyDir(path.join(dataDir, "subjects", name, "attachments"))
  }
}

async function assertEmptyDir(directory) {
  try {
    assert.deepEqual(await readdir(directory), [])
  } catch (error) {
    if (error.code !== "ENOENT") throw error
  }
}
