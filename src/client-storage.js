// The client's files, for one subject, over a small file adapter `fs`:
//   read(path) → bytes | null · write(path, text | bytes) (atomic) · list(dir) → names · remove(path)
//
//   <dataDir>/client-id                       this install's id
//   <dataDir>/<subject>/outbox/<seq>-<id>.json one queued change each, oldest first by name
//   <dataDir>/<subject>/pointer               the last log position applied here
//   <dataDir>/<subject>/failures.json         changes given up after three tries
export function createClientStorage(fs, dataDir, randomId) {
  const root = dataDir.replace(/\/+$/, "")
  let base = ""
  let seq = 0

  const text = async (path) => {
    const bytes = await fs.read(path)
    return bytes ? new TextDecoder().decode(bytes) : null
  }
  const json = async (path, fallback) => JSON.parse((await text(path)) ?? JSON.stringify(fallback))

  return {
    async open(subject) {
      base = `${root}/${encodeURIComponent(subject)}`
      const names = await fs.list(`${base}/outbox`)
      seq = names.length ? Number(names.at(-1).split("-")[0]) : 0
    },

    async clientId() {
      const saved = await text(`${root}/client-id`)
      if (saved) return saved.trim()
      const id = randomId()
      await fs.write(`${root}/client-id`, id)
      return id
    },

    async append(row) {
      seq += 1
      await fs.write(`${base}/outbox/${String(seq).padStart(12, "0")}-${row.id}.json`, JSON.stringify(row))
    },

    // The oldest `limit` queued changes, each with the file name it is saved under.
    async outbox(limit = Infinity) {
      const names = (await fs.list(`${base}/outbox`)).slice(0, limit)
      return Promise.all(names.map(async (name) => ({ name, row: await json(`${base}/outbox/${name}`, null) })))
    },

    async queued() {
      return (await fs.list(`${base}/outbox`)).length
    },

    save(name, row) {
      return fs.write(`${base}/outbox/${name}`, JSON.stringify(row))
    },

    remove(name) {
      return fs.remove(`${base}/outbox/${name}`)
    },

    async pointer() {
      return Number((await text(`${base}/pointer`)) ?? 0)
    },

    setPointer(position) {
      return fs.write(`${base}/pointer`, String(position))
    },

    failures() {
      return json(`${base}/failures.json`, [])
    },

    async addFailure(failure) {
      await fs.write(`${base}/failures.json`, JSON.stringify([...await json(`${base}/failures.json`, []), failure]))
    },
  }
}
