import { Directory, File } from "expo-file-system"

// The client's file adapter on Expo (see client-storage.js). Paths are file:// URIs.
export function createExpoFs(randomId) {
  return {
    async read(path) {
      const file = new File(path)
      return file.exists ? file.bytes() : null
    },

    async write(path, contents) {
      const temporary = new File(`${path}.${randomId()}.tmp`)
      temporary.create({ intermediates: true, overwrite: true })
      temporary.write(contents)
      const destination = new File(path)
      if (destination.exists) destination.delete()
      temporary.move(destination)
    },

    async list(path) {
      const directory = new Directory(path)
      if (!directory.exists) return []
      return directory.list().map((entry) => entry.name).filter((name) => !name.endsWith(".tmp")).sort()
    },

    async remove(path) {
      const file = new File(path)
      if (file.exists) file.delete()
    },
  }
}
