import { once } from "node:events"
import http from "node:http"

export async function listen(syncServer, t) {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1")
    if (url.pathname === "/sync" || url.pathname === "/also") {
      syncServer.handle(req, res)
      return
    }
    res.writeHead(404)
    res.end()
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()))
  }))
  const { port } = server.address()
  return {
    url: `http://127.0.0.1:${port}/sync`,
    also: `http://127.0.0.1:${port}/also`,
  }
}
