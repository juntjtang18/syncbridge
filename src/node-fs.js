import { randomUUID } from "node:crypto"
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"

// The client's file adapter on Node (see client-storage.js).
export const nodeFs = {
  async read(file) {
    try {
      return new Uint8Array(await readFile(file))
    } catch (error) {
      if (error.code === "ENOENT") return null
      throw error
    }
  },

  async write(file, contents) {
    await mkdir(path.dirname(file), { recursive: true })
    const temporary = `${file}.${randomUUID()}.tmp`
    await writeFile(temporary, contents)
    await rename(temporary, file)
  },

  async list(directory) {
    try {
      return (await readdir(directory)).filter((name) => !name.endsWith(".tmp")).sort()
    } catch (error) {
      if (error.code === "ENOENT") return []
      throw error
    }
  },

  remove(file) {
    return rm(file, { force: true })
  },
}
