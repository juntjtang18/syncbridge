# SyncBridge — Requirements

**Status:** 2026-10-06  
**Design:** [A1](./A1-syncbridge-highlevel-design.md)  
**Changing subject:** [A1-1](./A1-1-syncbridge-changing-subject.md)  
**Attachment pointer:** [A1-2](./A1-2-attachment-internal-uri.md)

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
12. The client API is `use(subject, applyChange, { readAttachment })`, `current()`, `subjects()`, `sync()`, and `appendChanges(package)`. `use` creates the subject if new, reuses its pointer if known, and sets current. `sync` and `appendChanges` apply to current only. `readAttachment(manifest)` is the host callback that streams bytes for `path`.
13. SyncBridge owns, per subject, the client's pointer and outgoing retry queue inside its `dataDir`. It also persists which subject is current. It does not copy attachment bytes into `dataDir`.
14. `appendChanges` queues a local change on the current subject without requiring a network connection. Unsent work stays on that subject's outbox if current moves away.
15. `sync()` happens only when the application calls it; SyncBridge does not poll.
16. `sync()` sends the current subject's queued changes, receives server entries after that subject's pointer, and advances that pointer only after `applyChange` succeeds.
16a. Every `POST /sync` response includes the host's live `subject`. The client compares it to the subject it sent. If they differ, the client `use`s the live subject and does not apply the stale body. `PUT /sync` streams bytes and returns `{ sha256, size }`. `GET /sync?sha256=` reads the in-flight PUT temp only.
16b. If the request subject is not the live subject, the host does not append to that log. It rejects and still returns the live subject.

## 4. applyChange

17. SyncBridge calls `await applyChange(entry, attachments)` for one received entry. `attachments.get(id).read()` calls `readAttachment` (client) or reads the in-flight PUT temp (server).
18. `attachments.get(id).read()` returns verified bytes for an attachment named in that entry.
19. The callback writes business data and copies or moves attachment bytes to the application's final destination. It throws on failure.
20. If the callback or an attachment transfer fails, the pointer stays unchanged and the entry is retried on the next sync.

## 5. Server

21. The server creates SyncServer with a persistent `dataDir`; SyncServer owns the ordered log, duplicate IDs, and a short-lived PUT temp.
22. SyncServer persists entries in its SQLite file under `dataDir`, indexed by subject and position. Attachment bytes are not kept after `applyChange` succeeds. `applyChange` may return `{ attachments: [{ id, path }] }` to update `path` on that log row.
23. A client sends `{ subject, pointer, changes }`. When that subject is live, SyncServer appends each new client change once, calls the server `applyChange` for each accepted client change, and returns entries after `pointer` plus the live `subject`.
24. A BizServer local change is appended to the log but is not applied again: BizServer already wrote its own business storage.

## 6. Reuse

25. SyncBridge is its own project. Site Inspect and later applications can use it through Node and Expo platform entries without placing business schemas, business storage, or subject semantics in the library.
