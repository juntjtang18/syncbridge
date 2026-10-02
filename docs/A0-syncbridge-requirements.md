# SyncBridge — Requirements

**Status:** 2026-10-02  
**Design:** [A1](./A1-syncbridge-highlevel-design.md)

SyncBridge is client-server middleware for synchronizing opaque application changes.

## 1. Nodes and subjects

1. A sync system has one server and one or more clients. Two phones and a server are the first case.
2. Any node can make a change.
3. A `subject` is one opaque string that names one independent ordered log. Nodes using the same subject see the same log.
4. SyncBridge does not parse subject semantics. An application may use `"default"`, `"tenant.company-123"`, or `"tenant.company-123.seat.mail%40example%2Ecom"`.
5. The server stores no pointer for any client. Each client holds its own pointer for each subject.
6. The transport host authenticates callers and authorizes their use of subjects. SyncBridge treats a subject only as a log name.

## 2. Package and attachments

7. A log entry is `{ id, position, package }`. `id` prevents a retry from being appended twice; SyncServer assigns `position`.
8. A package is `{ data: JSONValue, attachments: [] }`. `data` can be a string or any JSON value.
9. An attachment persisted in a log package is a manifest `{ id, sha256, size, contentType, name }`. At `appendChanges`, the application supplies `{ id, bytes, contentType, name }`; SyncBridge hashes and persists the manifest while storing the bytes separately.
10. SyncBridge does not inspect, run, or evaluate `data`. SQL and scripts are only data unless an application callback chooses to execute them.

## 3. Client

11. The platform creates a client with a persistent `dataDir` and a transport `send` function.
12. The client API is `init(subject, applyChange)`, `sync()`, and `appendChanges(package)`.
13. SyncBridge owns the client's pointer, outgoing retry queue, and one content-addressed attachment blob cache per subject inside its `dataDir`. Outgoing and received copies of the same SHA-256 share one cache file.
14. `appendChanges` queues a local change without requiring a network connection.
15. `sync()` happens only when the application calls it; SyncBridge does not poll.
16. `sync()` sends queued changes, receives server entries after the local pointer, and advances that pointer only after `applyChange` succeeds.

## 4. applyChange

17. SyncBridge calls `await applyChange(entry, attachments)` for one received entry only after all referenced attachment bytes are present and verified locally.
18. `attachments.get(id).read()` returns verified bytes for an attachment named in that entry.
19. The callback writes business data and copies or moves attachment bytes to the application's final destination. It throws on failure.
20. If the callback or an attachment transfer fails, the pointer stays unchanged and the entry is retried on the next sync.

## 5. Server

21. The server creates SyncServer with a persistent `dataDir`; SyncServer owns the ordered log, duplicate IDs, attachment storage, and replay.
22. SyncServer persists entries in its SQLite file under `dataDir`, indexed by subject and position. It stores attachment bytes separately by subject and SHA-256.
23. A client sends `{ subject, pointer, changes }`. SyncServer appends each new client change once, calls the server `applyChange` for each accepted client change, and returns entries after `pointer`.
24. A BizServer local change is appended to the log but is not applied again: BizServer already wrote its own business storage.

## 6. Reuse

25. SyncBridge is its own project. Site Inspect and later applications can use it through Node and Expo platform entries without placing business schemas, business storage, or subject semantics in the library.
