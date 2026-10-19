// Process-wide sync sizes, in one place. A host may override them when it creates a client or server.
export const SYNC_LIMITS = {
  // Changes in one POST /sync. A batch's photos upload, then the batch is sent and the server starts
  // applying it while the next uploads: 20 camera photos (~60 MB) take about a minute on a slow
  // network, so new data reaches the server within that, and a stop loses one batch at most.
  sendBatch: 20,
  // Entries in one ack. A client far behind (a new phone) catches up page by page, with progress.
  receivePage: 200,
  // Server applies in flight at once. An apply mostly waits on the host's storage (a Drive upload is
  // about a second), so a few at once multiply throughput on one thread. Drive allows ~3 writes/s.
  applyConcurrency: 3,
}
