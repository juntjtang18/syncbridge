// The load check: N clients, each with its own data folder, against one server in another process.
//   npm run load -- <clients> <subjects> <changes each> <photo KB>
// Clients share the subjects round robin. Each queues its changes, each with a photo, then syncs
// until it holds its subject's whole log, waiting the Retry-After a busy server gives. It prints how
// long that took, how the server held up, and every failure by message. PROF=<dir> writes the
// server's CPU profile there.
import { spawn } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { createSyncClient } from "../src/node.js"

const [clients = 50, subjects = 1, changesEach = 2, kb = 200] = process.argv.slice(2).map(Number)
const profile = process.env.PROF ? ["--cpu-prof", `--cpu-prof-dir=${process.env.PROF}`] : []
const serverProcess = spawn(process.execPath, [...profile, new URL("./load-server.mjs", import.meta.url).pathname, String(subjects)], {
  stdio: ["pipe", "pipe", "inherit"],
  env: { ...process.env, KB: String(kb) },
})
const url = (await once(serverProcess.stdout, "data"))[0].toString().trim()
const root = await mkdtemp(path.join(tmpdir(), "syncbridge-load-"))
const photo = new Uint8Array(kb * 1024).map((_, index) => index % 251)
const perSubject = Math.ceil(clients / subjects) * changesEach

const phones = []
for (let index = 0; index < clients; index += 1) {
  const subject = `user.${index % subjects}`
  const received = new Set()
  const client = createSyncClient({ dataDir: path.join(root, `client-${index}`), url, headers: () => ({ "x-subject": subject }) })
  await client.init(subject, async (entry) => { received.add(entry.id) }, { readAttachment: () => photo })
  for (let change = 0; change < changesEach; change += 1) {
    await client.appendChanges({ data: { client: index, change }, attachments: [{ id: "photo", path: `photo-${index}-${change}` }] })
  }
  phones.push({ client, received })
}

const errors = new Map()
let syncs = 0
let failedSyncs = 0
let busyAnswers = 0
const wait = (seconds) => new Promise((resolve) => setTimeout(resolve, seconds * 1000))

// Syncs as an app would: after a busy answer it waits the Retry-After it was given, after another
// failure a second, and after a sync that came back before a partner sent its changes, a moment.
async function untilComplete({ client, received }) {
  for (let tries = 0; tries < 60 && received.size < perSubject; tries += 1) {
    syncs += 1
    try {
      await client.sync()
      if (received.size < perSubject) await wait(0.25 + Math.random() / 4)
    } catch (error) {
      failedSyncs += 1
      if (error.status === 503) busyAnswers += 1
      const message = `${error.message} ${error.cause?.code || ""}`.trim().slice(0, 70)
      errors.set(message, (errors.get(message) || 0) + 1)
      await wait((error.retryAfter || 1) + Math.random())
    }
  }
}

const started = Date.now()
await Promise.all(phones.map(untilComplete))
const seconds = (Date.now() - started) / 1000
const complete = phones.filter(({ received }) => received.size === perSubject).length
serverProcess.stdin.write("done\n")
const server = JSON.parse((await once(serverProcess.stdout, "data"))[0].toString())
console.log(JSON.stringify({ clients, subjects, seconds, complete: `${complete}/${clients}`, syncs, failedSyncs, busyAnswers, ...server, errors: Object.fromEntries(errors) }))
await rm(root, { recursive: true, force: true })
process.exit(complete === clients ? 0 : 1)
