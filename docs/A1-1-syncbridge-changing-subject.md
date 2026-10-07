# A1-1 — Changing subject (server folder)

**Status:** 2026-10-06  
**Amends:** [A1](./A1-syncbridge-highlevel-design.md) · **Requirements:** [A0](./A0-syncbridge-requirements.md)

A1 assumed one `init(subject)` per client process and no live-root change. The host can now retarget the configured store (another Google folder, another driver). This spec is that delta. SyncBridge still does not parse the subject string.

## 1. What did not change

- A subject is one opaque string and one independent ordered log.
- The server owns that subject's `changelog0`. Positions start at `1` only inside that subject.
- The server stores no client pointer.
- Package and `applyChange` stay as in A1 / A1-2. Attachment `path` is a host pointer.
- `PUT /sync` and `GET /sync?sha256=` do not carry a subject.

## 2. Subject names the root

When the application’s configured root can change, it composes:

```text
user.{id}.{driver}.{root}
```

Examples: `user.4.local.4`, `user.4.google.1AbCfolderId`. Escape each component before joining.

| Piece | Meaning |
|---|---|
| `{id}` | Site-inspect user |
| `{driver}` | Spec provider: `local`, `google`, `onedrive`, … |
| `{root}` | That driver’s configured root (local user folder, picked Drive folder id, …) |

New root → new subject → new `changelog0`. Same root → same subject → same `changelog0`. SyncBridge only compares the final string.

## 3. Client: many subjects, one current

Replace one-shot `init` with a client that persists every subject it has seen and which one is live.

```text
use(subject, applyChange)   // create if new, reuse pointer if known, set current
current() → subject | null
subjects() → [subject]
appendChanges(package)      // current only; offline ok
sync() → { subject } | { switched: true, subject }
```

On disk:

```text
<dataDir>/current
<dataDir>/subjects/<encoded-subject>/pointer
<dataDir>/subjects/<encoded-subject>/outbox.jsonl
<dataDir>/subjects/<encoded-subject>/attachments/
```

That directory set is the list `{ subject, pointer }`. No second index.

`applyChange` is per subject. Two roots are two local trees.

`appendChanges` queues on **current**. If current moves away, unsent work stays in the old outbox until that subject is current again.

## 4. Pointer rule

A pointer is a position in **that** subject’s `changelog0`. Do not copy it across subjects.

| Situation | Pointer |
|---|---|
| This device has never seen the subject | `0` — the whole server log is new |
| This device has seen it (including return to a previous root) | Keep that subject’s file |
| Current moves from A to B | A’s pointer stays; B is `0` if new, else B’s file |

Do not reset to `0` on every current change. That would replay a known log onto a tree that already applied it.

## 5. Detect a live-subject change on every POST

The host owns the live root. On every `POST /sync` (the call that already names a subject) the response includes that string:

```js
{ acceptedIds, entries, attachmentHashes, subject }
```

`subject` is the **live** root, not the one the client sent. The extra field is cheap next to attachments.

```text
client POST { subject: A, pointer, changes }
response.subject is B
  do not apply the stale body
  use(B, applyChange)
  next sync() is B
```

If the request subject is not live, the host **does not** append to that `changelog0`. It rejects (409, or empty `acceptedIds` / `entries`) and still returns the live `subject`. A late sync after a folder change must not mix history into the previous log.

The host supplies the live subject (`currentSubject(user)`, or a wrapper around `handle`). SyncBridge opens the named log only when the host says that name is current.

## 6. Situations

| Host action | Subject | Client |
|---|---|---|
| Pick Google folder A once | `user.4.google.A` | One current; pointer from 0 |
| Change root to folder B | `user.4.google.B` | New current; B pointer 0 if new; A kept |
| Switch driver to local | `user.4.local.4` | Same rule |
| Change back to A | `user.4.google.A` | Reuse A’s `changelog0` and A’s pointer |

## 7. Not in this spec

- Site-inspect composing `user.{id}.{driver}.{root}` from the driver spec (host work).
- Implementing `use` / `current` / `subjects` and the `subject` field on `POST` (library work; A1 API is the target).
- Parsing driver or folder meaning inside SyncBridge.
