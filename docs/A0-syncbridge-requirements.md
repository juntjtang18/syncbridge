# SyncBridge — Requirements

**Status:** 2026-10-19, the minimal design. It replaces the earlier multi-subject client, retry
counters on the wire, five ack statuses and shared attachment staging.
**Client send:** [B1](./B1-syncbridge-client-send.md)

SyncBridge keeps one ordered change log per subject on a server and gives every client the same
log, in the same order. Applications apply each change; the last one logged wins.

## 1. Log and subjects

1. One server and any number of clients. A `subject` is an opaque string naming one log (e.g. `user.4`).
2. A log entry is `{ id, position, package }`. The server assigns `position`. `id` makes a resend a no-op.
3. A package is `{ data, attachments }`. `data` is any JSON. An attachment is `{ id, path, contentType? }`
   when queued, and `{ id, path, contentType?, upload, sha256, size }` once logged. `path` is the
   host's pointer; SyncBridge never parses it.
4. The hash of an attachment only checks that bytes arrived as sent: client to server on upload,
   server to client on download. It never links one entry to another. There is no dedupe.

## 2. Client

5. `createSyncClient({ dataDir, url, headers?, log? })` from `syncbridge/node` or `syncbridge/expo`.
   API: `init(subject, applyChange, { readAttachment })`, `appendChanges(package)`, `sync()`,
   `failedEntries()`, `onProgress(listener)`.
6. Files, per subject under `dataDir/<subject>/`: `outbox/<seq>-<id>.json` (one queued change each),
   `pointer` (last position applied here), `failures.json`; and `dataDir/client-id`.
7. `appendChanges` writes one outbox file and returns `{ id }`, offline, even while a sync runs.
8. `sync()` runs only when called, one at a time. It repeats until the outbox is empty and no page
   is left:
   - take the oldest `sendBatch` (20) outbox changes;
   - upload each attachment not yet uploaded: hash the bytes, `PUT` them, check the answer's
     `sha256` and `size`, and keep `upload`, `sha256`, `size` on the change's outbox file at once
     (a stop re-uploads nothing already up);
   - `POST` the batch with the pointer; delete the accepted changes from the outbox;
   - while changes are left to send, ask for no entries (`page: 0`): send first, then receive;
   - apply each returned entry in order, then set the pointer to its position. Attachments are
     downloaded a few at a time (`downloads`, 4) ahead of the applies.
9. Applying an entry: download each attachment (`GET`), check its hash and size, call
   `applyChange(entry, attachments)`. `entry.own` is true for this client's own changes, which come
   back in log order without attachments.
10. One rule for failures: an item is tried up to 3 times back to back. A change the server refuses
    is resent (its attachments uploaded afresh) up to 3 times; an entry that does not download intact
    or apply is retried 3 times. After the third failure it goes on `failures.json` and the sync moves
    on (for an entry, the pointer still moves past it). A network failure, or a 5xx answer (the server restarting
    or busy), is not a try: `sync()` throws and the next sync resumes where it stopped. A busy server's
    503 carries `Retry-After`; the thrown error has it as `retryAfter` (seconds), so the app can wait
    that long before its next sync.
11. `onProgress` reports `{ sent, uploaded, toSend, received, toReceive }`.

## 3. Server

12. `createSyncServer({ dataDir, currentSubject?, log?, applyConcurrency?, maxRequests?, busyRetryAfter? })` from `syncbridge/node`.
    API: `init(subject, applyChange, { readAttachment, applyKey? })`, `handle(req, res)`,
    `appendChanges(subject, { data }, { apply? })`, `entriesBetween(subject, after, before)`,
    `failedEntries(subject)`, `pendingSubjects()`, `whenIdle()`, `close()`.
13. State under `dataDir`: `syncbridge.sqlite` (the logs, the changes waiting to be applied, given-up
    applies) and `uploads/<upload>` (one file per `PUT`, until its change is applied).
14. HTTP on one path:
    - `PUT` bytes → `{ upload, sha256, size }`. Every upload is its own file.
    - `POST { subject, pointer, clientId, changes, page }` → `{ acceptedIds, refused?, entries, more, remaining }`.
      Changes are accepted in order; the first that cannot be (its upload missing or not the logged
      size, or malformed) is named in `refused: { id, error }` and the rest wait for the next post.
      `entries` are at most `page` (200) entries after `pointer`, read with SQL `LIMIT`.
      A subject that is not the signed-in one (`currentSubject(req)`) is HTTP 409.
    - `GET ?subject=&entry=&attachment=` → one attachment of one logged change: its upload while the
      change waits to be applied, then the host's stored copy through `readAttachment`, which returns
      `null` for a file that is not there (HTTP 404) and throws on a failure to read it (HTTP 500).
      The server does not hash it again; the client checks it against the logged hash (item 9).
    - Any other error is a non-2xx `{ error }`. Requests are not serialized.
    - At most `maxRequests` requests, from all clients together, are worked on at once. One more is
      answered at once with HTTP 503, `Retry-After: busyRetryAfter` and `Connection: close`, its body
      unread, instead of waiting in a queue until the client times out.
15. Accepted changes are applied through `applyChange(entry, attachments)`. Each subject is scheduled
    on its own, so one subject's backlog never holds up another's, and when one of its applies ends
    only that subject's waiting changes are read again: up to `applyConcurrency` (3) of a
    subject's changes at once, the same `applyKey(entry)` one at a time in log order, different keys
    side by side, no key alone (it waits for that subject's changes before it). An apply is tried 3 times; after the third
    failure it goes on the server's failure list. Either way its uploads are deleted. `applyChange`
    may return `{ attachments: [{ id, path }] }` to point later downloads at the stored copy.
16. `appendChanges(subject, { data })` logs a host change for every client. With `{ apply: true }`
    it is also applied, in log order with the clients' changes, and resolves once applied.
17. A subject not initialized yet keeps its changes waiting; `pendingSubjects()` names them so a
    restarted host can `init` them.

## 4. Limits

18. `SYNC_LIMITS` in `src/limits.js`: `sendBatch` 20, `receivePage` 200, `downloads` 4, `applyConcurrency` 3,
    `maxRequests` 128, `busyRetryAfter` 2 (seconds).
19. Accepted limitation: a server apply that fails 3 times leaves the host's storage without that
    change while clients have it, until someone fixes the cause.

## 5. Scale

20. One server serves any number of clients, as many as it has the capacity for. It keeps nothing per
    client: a client is a `clientId` on the log rows it sent. A request's work is bounded by the batch,
    the page, or one attachment, never by how many clients or subjects there are, and past
    `maxRequests` the server answers busy rather than slowing every client down.
21. `npm run load -- <clients> <subjects> <changes each> <photo KB>` runs the load check: that many
    clients, each with its own data folder, against one server in its own process, until every client
    holds its subject's whole log. On one laptop (2026-10-19, 2 changes each with a 200 KB photo):

    | clients / subjects | time | server peak memory | longest stall | failures |
    |---|---|---|---|---|
    | 2,000 / 1,000 | 8 s | 233 MB | 33 ms | busy answers only |
    | 4,000 / 2,000 | 12 s | 242 MB | 95 ms | busy answers only |
    | 8,000 / 4,000 | 28 s | 299 MB | 290 ms | busy answers only |

    Before item 15's per-subject reads and item 14's limit, the same 4,000 clients took 108 s, with a
    38 s stall, 1.5 GB, and 16,620 timeouts and resets.
22. Clients that share one subject each download every other client's attachments, so that work
    grows with the square of their number: it is the cost of the fan-out, not of the server.
