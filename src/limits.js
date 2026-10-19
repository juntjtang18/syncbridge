// Process-wide sync sizes, in one place. A host may override them when it creates a client or server.
export const SYNC_LIMITS = {
  // Changes in one POST /sync. A batch's photos upload, then the batch is sent and the server starts
  // applying it while the next uploads: 20 camera photos (~60 MB) take about a minute on a slow
  // network, so new data reaches the server within that, and a stop loses one batch at most.
  sendBatch: 20,
  // Entries in one ack. A client far behind (a new phone) catches up page by page, with progress.
  receivePage: 200,
  // Attachments downloaded at once while receiving; changes are still applied one by one in order.
  downloads: 4,
  // Server applies in flight at once. An apply mostly waits on the host's storage (a Drive upload is
  // about a second), so a few at once multiply throughput on one thread. Drive allows ~3 writes/s.
  applyConcurrency: 3,
  // Requests the server works on at once, from every client together. One past it is answered 503
  // at once with Retry-After (busyRetryAfter seconds): the client's sync stops without using a try
  // and comes back, instead of waiting in a queue until it times out. A request holds at most one
  // attachment or one page in memory, so this also bounds the server's memory.
  maxRequests: 128,
  busyRetryAfter: 2,
}
