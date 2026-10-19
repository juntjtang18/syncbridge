// The load check's server, alone in its own process: a SyncBridge server whose host stores nothing.
//   node testing/load-server.mjs <subjects>
// It prints its URL. On a line from stdin it prints its peak memory, CPU, longest event-loop stall,
// and the connection errors Node saw, then exits.
import http from "node:http"
import { mkdtemp } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { createSyncServer } from "../src/node.js"

const subjects = Number(process.argv[2] || 1)
const photo = new Uint8Array(Number(process.env.KB || 200) * 1024).map((_, index) => index % 251)
const root = await mkdtemp(path.join(tmpdir(), "syncbridge-load-server-"))
const server = createSyncServer({ dataDir: path.join(root, "server"), currentSubject: (req) => req.headers["x-subject"] })

// The host stores nothing: every photo in the check is the same bytes, so a later read makes them again.
for (let index = 0; index < subjects; index += 1) {
  server.init(`user.${index}`, async (entry, attachments) => {
    for (const manifest of entry.package.attachments) await attachments.get(manifest.id).read()
    await new Promise((resolve) => setTimeout(resolve, 5))
    return { attachments: entry.package.attachments.map((manifest) => ({ id: manifest.id, path: `stored/${manifest.id}` })) }
  }, { readAttachment: () => photo })
}

const web = http.createServer((req, res) => server.handle(req, res))
const connectionErrors = new Map()
web.on("clientError", (error, socket) => {
  const name = error.code || error.message
  connectionErrors.set(name, (connectionErrors.get(name) || 0) + 1)
  socket.destroy()
})
await new Promise((resolve) => web.listen(0, "127.0.0.1", resolve))

let peak = 0
setInterval(() => { peak = Math.max(peak, process.memoryUsage().rss) }, 50).unref()
let stall = 0
let last = performance.now()
setInterval(() => {
  const now = performance.now()
  stall = Math.max(stall, now - last - 20)
  last = now
}, 20).unref()

console.log(`http://127.0.0.1:${web.address().port}/sync`)
process.stdin.once("data", async () => {
  await server.whenIdle()
  const cpu = process.cpuUsage()
  console.log(JSON.stringify({
    serverPeakRssMB: Math.round(peak / 1e6),
    serverCpuSec: Math.round((cpu.user + cpu.system) / 1e5) / 10,
    maxEventLoopLagMs: Math.round(stall),
    connectionErrors: Object.fromEntries(connectionErrors),
  }))
  process.exit(0)
})
