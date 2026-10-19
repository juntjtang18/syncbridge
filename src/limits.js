// Process-wide sync sizes, in one place. A host may override them when it creates a client or server.
export const SYNC_LIMITS = {
  // Changes in one POST /sync. A batch's photos upload, then the batch is sent and the server starts
  // applying it while the next batch uploads, so data appears early and a stop loses one batch at most.
  sendBatch: 100,
}
