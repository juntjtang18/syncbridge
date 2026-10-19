// Process-wide sync sizes, in one place. A host may override them when it creates a client or server.
export const SYNC_LIMITS = {
  // Changes in one POST /sync. A batch's photos upload, then the batch is sent and the server starts
  // applying it while the next batch uploads, so data appears early and a stop loses one batch at most.
  sendBatch: 100,
  // Seconds a batch may spend uploading before it is sent with the changes whose bytes are up, so
  // the server starts on new data within about half a minute on any network. At least one change goes.
  sendSeconds: 30,
  // Entries in one ack. A client far behind (a new phone) catches up page by page, with progress.
  receivePage: 200,
}
