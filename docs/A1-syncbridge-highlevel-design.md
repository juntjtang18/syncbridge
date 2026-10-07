# SyncBridge — High-level design

**Status:** 2026-10-06  
**Requirements:** [A0](./A0-syncbridge-requirements.md)  
**Changing subject:** [A1-1](./A1-1-syncbridge-changing-subject.md)

SyncBridge is middleware for client-server synchronization. It moves opaque change packages. The application decides what a package means and supplies `applyChange` to write that package into its business storage.

The server owns the ordered history. A client owns only its last server position and a retry queue for its own unsent changes. SyncBridge owns both of those client files under an app-supplied `dataDir`.

```mermaid
flowchart LR
  subgraph phoneA [Phone A]
    bizA[BizClient]
    localA[(dataDir subject files)]
    clientA[SyncClient]
  end
  subgraph phoneB [Phone B]
    bizB[BizClient]
    localB[(dataDir subject files)]
    clientB[SyncClient]
  end
  subgraph serverHost [Server]
    bizServer[BizServer]
    syncServer[SyncServer]
    serverData[(dataDir subject changelog0)]
  end

  bizA -->|appendChanges| clientA
  clientA -->|applyChange| bizA
  clientA --> localA
  bizB -->|appendChanges| clientB
  clientB -->|applyChange| bizB
  clientB --> localB
  clientA <-->|subject pointer changes| syncServer
  clientB <-->|subject pointer changes| syncServer
  syncServer --> serverData
  syncServer -->|applyChange| bizServer
  bizServer -->|appendChanges| syncServer
```

Diagram 1: SyncBridge client-server synchronization

## 1. Subject

A `subject` is the name of one independent ordered log. It is one opaque string. SyncBridge does not parse or interpret it.

Nodes that use the same subject see the same log, like a publisher and subscriber that use the same queue subject. Different subjects have separate histories and separate positions.

The application can compose a readable subject from dot-separated components, escaping each component before joining:

```js
"default"
"tenant." + encode(companyId)
"tenant." + encode(tenantId) + ".seat." + encode(email)
"user." + encode(userId) + "." + encode(driver) + "." + encode(root)
```

For example, `user.4.google.1AbCfolderId` is one subject: that user, that driver, that configured root folder. `tenant.company-123.seat.mail%40example%2Ecom` is another. The application escapes components so values such as email addresses or folder ids cannot change the subject shape. SyncBridge stores and compares the final string only.

Each subject has its own server `changelog0`. Positions start at `1` only inside that subject. A pointer is a location in **that** subject's log. Position `3` on folder A is not position `3` on folder B.

A phone can use several subjects. One is **current** (the live root). The others stay on disk so a later return to that root reuses the same log and the same pointer. They remain separate logs.

## 2. Change log

Each record in `changelog0` is:

```js
{ id, position, package }
```

`id` identifies a change so a retry is not appended twice. `position` is assigned by SyncServer, starts at `1`, and orders entries only inside that subject.

`package` has one generic JSON envelope:

```js
{
  data: JSONValue,
  attachments: [
    { id, sha256, size, contentType, name }
  ],
}
```

`data` can be a string, number, boolean, `null`, JSON array, or JSON object. A plain string package is therefore just:

```js
{ data: "plain text", attachments: [] }
```

`attachments` is a sibling of `data`, not part of an application's business schema. It is the generic file mechanism. Each entry is a manifest, not the file bytes. `id` is an application-selected reference, `sha256` identifies the exact byte content, `size` verifies its length, and `contentType` and `name` are optional metadata. Business data can refer to `attachments[].id`, but SyncBridge does not inspect that relationship.

SyncBridge serializes the package as JSON, but never evaluates or runs its `data`. A SQL or script string is only data to SyncBridge; it becomes dangerous only if an application's `applyChange` chooses to execute it.

The package passed to `appendChanges` has the same shape, except every attachment supplies `bytes` instead of `sha256` and `size`. SyncBridge hashes those bytes, saves them, and writes the resulting manifest into the queued package and server log.

SyncServer owns the log mechanics: assigning positions, keeping order, and returning records after a position. It does not store a pointer for any client.

## 3. Attachments

A file path alone cannot be in a package: that path exists only on the node that made the change. Putting every file byte directly into `changelog0` would duplicate large files in every replay.

SyncBridge stores attachment bytes separately, content-addressed by their SHA-256 hash:

```text
<dataDir>/<encoded-subject>/attachments/<sha256>
```

The same bytes are stored once per subject. The log stores only the attachment manifest. A sender gives file bytes to `appendChanges`; SyncBridge hashes and saves the bytes, then writes the hash into the queued package.

Attachment bytes have two server/client lifetimes:

- The server's subject attachment store is the durable source for replaying a log entry to later clients.
- A client has one immutable content-addressed blob cache per subject. Outgoing and received attachments use the same `<sha256>` file.

During `sync()`:

```text
sender         upload attachment bytes missing on server
server         verify hash and size, save attachment bytes
server         append the package manifest to changelog0
receiver       download referenced attachment bytes it does not have
receiver       verify hash and size, save bytes in blob cache
receiver       call applyChange(entry, attachments)
receiver       advance pointer
```

SyncBridge never calls `applyChange` until all attachments referenced by that entry are present and verified locally. If a hash is already in the client blob cache, it does not download another copy. New bytes are written to a temporary file, verified, then atomically moved to the hash path. It passes an attachment reader with the entry. `applyChange` moves or copies each attachment into the application's final destination. SyncBridge does not know that destination or whether it is a database, OneDrive, a local file, or another store.

The application returns success from `applyChange` only after its final business data and attachment destination are complete. Then SyncBridge advances the pointer. It retains the cache file, because it may be used by a queued outgoing package or a later received entry. A later garbage collector can remove unreferenced cache files. If attachment transfer or `applyChange` fails, the pointer stays unchanged and the next `sync()` retries the entry.

The callback contract is:

```js
async function applyChange(entry, attachments) {
  // entry = { id, position, package: { data, attachments: manifests } }

  const photo = attachments.get("photo-1")
  const bytes = await photo.read()

  // Write entry.package.data and bytes into application storage.
  // Throw if either write fails.
}
```

`attachments` is scoped to this entry. `get(id)` returns the verified attachment named by `entry.package.attachments[].id`:

```js
{
  id,
  sha256,
  size,
  contentType,
  name,
  async read(), // Promise<Uint8Array>
}
```

The contract deliberately does not expose a staging path. Node and Expo can keep different temporary-file implementations while every application reads the same attachment bytes. A later streaming reader can be added without changing attachment IDs or package schema.

For Site Inspect, `data` can be the PoI and PoI-entry descriptions; every photo becomes an attachment:

```js
{
  data: {
    poi,
    description: poiDescription,
    poiEntries: [
      { description: "Close-up", photo: { attachmentId: "photo-1" } },
    ],
  },
  attachments: [
    { id: "photo-1", bytes: photoBytes, contentType: "image/jpeg", name: "poi-1.jpg" },
  ],
}
```

## 4. Client

The platform entry creates a client once. That client holds **many subjects** and one **current** subject:

```js
const client = createSyncClient({ dataDir, send })
await client.use(subject, applyChange)
```

`dataDir` is a persistent app directory. The Node entry uses Node file APIs; the Expo entry uses Expo file APIs. SyncBridge persists the current subject and, for every subject this device has seen, that subject's pointer, outbox, and attachment cache:

```text
<dataDir>/current
<dataDir>/subjects/<encoded-subject>/pointer
<dataDir>/subjects/<encoded-subject>/outbox.jsonl
<dataDir>/subjects/<encoded-subject>/attachments/<sha256>
```

That directory set is the persisted list `{ subject, pointer }`. There is no second index. A subject this client has never seen gets `pointer` `0`: every server log entry is new. A subject this client has seen keeps its file. Changing current does not rewrite another subject's pointer.

The public client API is:

```text
use(subject, applyChange)   // create if new, reuse pointer if known, set current
current() → subject | null
subjects() → [subject]
appendChanges(changeLog)    // queues on current (offline ok)
async sync() → { subject } | { switched: true, subject }
```

`applyChange` is per subject. Two roots are two local trees. Do not reuse one callback that always writes the same directory.

`appendChanges(changeLog)` adds a local change to the **current** subject's outgoing queue. It does not require a network connection. Unsent work stays in that subject's outbox if current moves away. It flushes only when that subject is current again and `sync()` runs.

`sync()` happens only when the app calls it; SyncBridge does not poll:

```text
sync:
  send current subject, that subject's pointer, and that subject's queued changes
  remove each change the server accepts

  receive the live subject on the response (see §4.1)
  if the live subject differs from current:
    persist current + first-seen pointer 0
    return { switched: true, subject: live } (do not apply the stale body)

  receive entries after the saved pointer
  for each entry:
    applyChange(entry)
    save entry.position as the new pointer for this subject
```

The pointer is saved only after `applyChange` succeeds. If applying an entry fails, the pointer remains unchanged and the next `sync()` receives that entry again.

### 4.1 Changing the live subject (server folder)

The host application owns the live root (the configured driver folder). SyncBridge does not compose `user.{id}.{driver}.{root}` and does not read the driver spec.

On every `POST /sync` — the call that already names a subject — the **host** puts the live subject on the response:

```js
{ acceptedIds, entries, attachmentHashes, subject }
```

`subject` is always the live root, not “the one the client sent.” The extra string is cheap next to attachments. `PUT` and `GET` of raw attachment bytes do not carry a subject.

The client compares. If `response.subject` differs from the subject it sent, that is the folder change. It calls `use(response.subject, applyChange)` and makes that string current. First time this device sees it: pointer `0`. Seen before: reuse that pointer.

If the request subject is not the live subject, the host must **not** append to the old `changelog0`. It rejects (409, or an empty accept list) and still returns the live `subject`. A late sync after a folder change must not mix history into the previous log.

```text
client POST { subject: A, pointer, changes }
host live subject is B
  do not write A's changelog0
  respond { subject: B, acceptedIds: [], entries: [], … }
client use(B) → current is B
next sync() is B (pointer 0 if B is new, else B's saved pointer)
```

Leave A, go to B, return to A: A’s changelog0 and A’s pointer are reused. Resetting A to `0` on every current change would replay A’s whole history onto a tree that already applied it.

The host supplies the live subject (a `currentSubject(user)` callback, or a wrapper around `handle`). SyncBridge still only opens the log named in the request, and only if the host says that name is current.

## 5. Server

The server entry creates SyncServer once:

```js
const syncServer = createSyncServer({ dataDir })
syncServer.init(subject, applyChange)
```

SyncServer owns `<dataDir>/syncbridge.sqlite`. It stores `log_entries` indexed by `(subject, position)` and `(subject, id)`, so it can return entries after a pointer without scanning a text file. BizServer does not open the database or implement log storage methods.

When a client sends `{ subject, pointer, changes }`, and the host says that subject is current, SyncServer:

1. Opens that subject's log (`changelog0` for that subject only).
2. Appends each new client change once and assigns its position.
3. Calls the server `applyChange(entry)` for each accepted client change.
4. Returns records after `pointer`, plus the live `subject` the host provided.

When BizServer calls `appendChanges(subject, changeLog)`, SyncServer writes that change to that subject's `changelog0` but does not apply it again: BizServer already wrote its own business storage.

The transport host is responsible for authenticating callers, authorizing which subjects they may use, and naming the live subject on every `POST /sync`. SyncBridge treats a subject as a log name only.

## 6. Library boundary

```text
BizClient    applyChange(entry, attachments), appendChanges; asks host for live subject
SyncClient   use, current, subjects, sync, appendChanges
SyncServer   init, receive, appendChanges
BizServer    applyChange(entry, attachments), appendChanges
Host         live subject on POST /sync; reject a request subject that is not live
```

SyncBridge is not built on a normal queue. The client has an outgoing retry queue, but each server subject is a durable replayable log: multiple clients can independently read entries after their own pointers.
