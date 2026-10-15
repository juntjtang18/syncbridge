# SyncBridge — Requirements

**Status:** 2026-10-06  
**Design:** [A1](./A1-syncbridge-highlevel-design.md)  
**Changing subject:** [A1-1](./A1-1-syncbridge-changing-subject.md)  
**Attachment pointer:** [A1-2](./A1-2-attachment-internal-uri.md)  
**Client send:** [B1](./B1-syncbridge-client-send.md)

SyncBridge is client-server middleware for synchronizing opaque application changes.

## 1. Nodes and subjects

1. A sync system has one server and one or more clients. Two phones and a server are the first case.
2. Any node can make a change.
3. A `subject` is one opaque string that names one independent ordered log. Nodes using the same subject see the same log.
4. SyncBridge does not parse subject semantics. An application may use `"default"`, `"tenant.company-123"`, `"tenant.company-123.seat.mail%40example%2Ecom"`, or `"user.{id}.{driver}.{root}"` when the configured root can change.
5. The server stores no pointer for any client. Each client persists a pointer (and outbox) for each subject it has seen. A subject this client has never seen starts at pointer `0`. Returning to a known subject reuses that pointer. Changing current does not reset another subject's pointer.
6. The transport host authenticates callers, authorizes their use of subjects, and names the **live** subject. SyncBridge treats a subject only as a log name. One subject is one `changelog0`.

## 2. Package and attachments

7. A log entry is `{ id, position, package }`. `id` prevents a retry from being appended twice; SyncServer assigns `position`.
8. A package is `{ data: JSONValue, attachments: [] }`. `data` can be a string or any JSON value.
9. An attachment persisted in a log package is a manifest `{ id, sha256, size, contentType, path }`. At `appendChanges`, the application supplies `{ id, path, contentType }`. `path` is a host-internal pointer. SyncBridge does not parse it. `sha256` and `size` are filled when the PUT stream is hashed.
10. SyncBridge does not inspect, run, or evaluate `data`. SQL and scripts are only data unless an application callback chooses to execute them.

## 3. Client

11. The platform creates a client with a persistent `dataDir` and a transport `send` function. One client holds many subjects and one current subject.
12. The client API is `use(subject, applyChange, { readAttachment })`, `current()`, `subjects()`, `sync()`, `appendChanges(package)`, `failedEntries()`, and `whenIdle()`. `use` creates the subject if new, reuses its pointer if known, and sets current. `sync` and `appendChanges` apply to current only. `readAttachment(manifest)` is the host callback that streams bytes for an outbox `path` when sending.
13. SyncBridge owns, per subject, the client's pointer, outbox, unprocessed queue, and failure list inside its `dataDir`. It also persists which subject is current and a stable `clientId`. Attachment temps live in `dataDir` only until a successful local apply deletes them.
14. `appendChanges` queues a local change on the current subject without requiring a network connection. It returns when the change is stored in that subject's `incoming/` directory, including while `sync()` is still running. Unsent work stays in `incoming/` or `inflight/` if current moves away. The handoff is [B1](./B1-syncbridge-client-send.md).
15. `sync()` happens only when the application calls it; SyncBridge does not poll.
16. `sync()` blocks until the `POST /sync` ack and until each downloaded entry and its attachment bytes are saved locally. It does not wait for server `applyChange` or for the client's own `applyChange`. The pointer means received through here. It moves when the entry is saved, including an entry the sender gave up after 3 failure acks. Apply does not move it again.
16a. Every `POST /sync` response includes the host's live `subject` and a `status`. The client compares `subject` to the one it sent. If they differ, the client `use`s the live subject and returns `{ status: "switched", switched: true, subject }`. `PUT /sync` streams bytes and returns `{ sha256, size }`. A known write or hash problem returns `{ status: "failed", error }`. An unexpected PUT throw returns `{ status: "error", error }`. `GET /sync?sha256=` reads the in-flight PUT temp. After that temp is deleted, the same GET streams the live subject's `readAttachment` for the log manifest with that hash. A missing blob is HTTP 404. The changelog stores `path`, not a URL.
16b. If the request subject is not the live subject, the host does not append to that log. It answers HTTP 409 with `{ status: "switched", subject }` and still returns the live subject.
16c. Predicted POST statuses, all HTTP 200 except `switched`: `succeed` (every change in the batch was saved), `partial` (`acceptedIds` were saved and the server stopped at the first change it could not store), `failed` (nothing new was saved), `switched` (request subject is not live). An unexpected throw is `{ status: "error", error }` with HTTP 200. The process stays up. A `failed` or `partial` receive stores nothing for the refused change and writes no failure-list row.

## 4. applyChange

17. SyncBridge calls `await applyChange(entry, attachments)` for one received entry. On the server, during the upload that just arrived, `attachments.get(id).read()` reads the in-flight PUT temp. On the client, `attachments.get(id).read()` for a received entry calls `GET /sync?sha256=`. The client's `readAttachment` is only for sending.
18. `attachments.get(id).read()` returns verified bytes for an attachment named in that entry.
19. The callback writes business data and copies or moves attachment bytes to the application's final destination. It throws on failure.
20. Apply runs on the node that saved the log, after the ack, on that node's unprocessed queue. The first two throws leave the entry at the head. The third moves it to that node's failure list with `apply failed after 3 attempts: …`, keeps the attachment temps, and the worker continues. The pointer is not rewound.

## 5. Server

21. The server creates SyncServer with a persistent `dataDir`; SyncServer owns the ordered log, duplicate IDs, and a short-lived PUT temp. `init(subject, applyChange, { readAttachment })` registers the host reader for that subject.
22. SyncServer persists `changelog0` in its SQLite file under `dataDir`, indexed by subject and position. A successful receive appends there and enqueues the server unprocessed queue. Attachment temps are deleted after `applyChange` succeeds. `applyChange` may return `{ attachments: [{ id, path }] }` to update `path` on that log row. A later `GET /sync?sha256=` calls `readAttachment` with that manifest. `failedEntries(subject)` and `whenIdle()` expose the failure list and the drained unprocessed queue.
23. A client sends `{ subject, pointer, changes, clientId }`. When that subject is live, SyncServer appends each new client change once, acks, and returns entries after `pointer` plus the live `subject`. It does not call `applyChange` on the request. The server does not store the client's pointer. It stores per-client send attempts for `(subject, clientId, id)`.
24. A BizServer local change is appended to `changelog0` and is not enqueued for apply: BizServer already wrote its own business storage. `appendChanges` returns while a client apply may still be running.
25. Upload and download use the same sender and receiver roles. The receiver tries to save the log entry and the attachment temps. If that fails it stores nothing, acks `failed`, and does not count. If it succeeds it acks `succeed`. The sender counts a `failed`, `partial`, or `error` ack. Attempts 1 and 2 keep the entry and send nothing after it. Attempt 3 moves it to the sender's failure list with `send failed after 3 attempts: …` and the same `sync()` sends the next entry. On a download, the client posts `receiveFailed` for the id it could not save. At 3, the server skips that entry for that `clientId` only. Other clients still receive it.

## 6. Reuse

26. SyncBridge is its own project. Site Inspect and later applications can use it through Node and Expo platform entries without placing business schemas, business storage, or subject semantics in the library.
