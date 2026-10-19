# B1 — Client send

**Status:** component design · 2026-10-15  
**Requirements:** [A0](./A0-syncbridge-requirements.md) §3  
**System:** [A1](./A1-syncbridge-highlevel-design.md)

This component is the phone's upload buffer. It owns client `appendChanges` and the send half of client `sync()`. Server `appendChanges` is a different call: it writes `changelog0` and does not use this queue.

A change is durable once `appendChanges` returns. That return does not wait for a `sync()` that is already on the network. `sync()` never writes the directory `appendChanges` writes, so the two calls do not share a lock.

```mermaid
flowchart LR
  appendChanges[appendChanges] -->|temp then rename| incoming[incoming]
  incoming -->|rename each file when inflight is empty| inflight[inflight]
  inflight -->|hash attempts remove| syncNode[sync]
```

## 1. Files

Each subject has two directories. One JSON file is one change. The name is `<seq>-<id>.json`. `<seq>` is zero-padded so directory order is append order. The client increments it before any `await`. The first value is one past the highest seq already on disk.

```text
<dataDir>/subjects/<digest>/incoming/<seq>-<id>.json
<dataDir>/subjects/<digest>/inflight/<seq>-<id>.json
```

`incoming/` receives new rows. `appendChanges` writes a temp file beside the directory and renames it into `incoming/`. `inflight/` is the batch `sync()` is sending. Hash updates, send attempts, and deletes happen on those files only.

An older `outbox.jsonl` is read once. Each line becomes an `inflight` file, and the jsonl is deleted only after those files exist. A crash in the middle writes the same ids again, then deletes the jsonl.

## 2. appendChanges

`appendChanges` is not on the queue that runs `sync()`. It snapshots the current subject, writes one `incoming` file, and returns `{ id }`. A `sync()` in progress does not delay that write. A throw from a missing subject or a bad package rejects the returned promise.

If the app exits after that return, the file is in `incoming/` or `sync()` has already renamed it into `inflight/`. The next launch sends it. A change that has not reached `appendChanges` is outside this component.

## 3. sync

Two `sync()` calls still run one after another. Each pass claims `inflight`:

- If `inflight/` has files, that list is the pass. `incoming/` is left alone.
- If `inflight/` is empty, each `incoming/` file is renamed into `inflight/`. A file that appears after that listing stays in `incoming/` until the next pass.

`selectBatch` takes at most `sendBatch` rows (20), oldest first. A row with `attempts > 0` blocks later `inflight` rows. Those later rows stay in `inflight/`. New photos stay in `incoming/` until `inflight/` is empty.

Each change's upload id, hash and size are written onto its claimed `inflight` file as soon as that change's attachments are up, before the next change uploads, so a stop part way re-uploads nothing already sent. These writes touch claimed `inflight` files only. Accepted ids delete those files. A send failure updates or deletes that one `inflight` file. None of these rewrite `incoming/`.

The loop claims again after a pass. A photo saved while this `sync()` is open goes out before `sync()` returns when the app stays up.

The POST body is `{ subject, pointer, changes, clientId }`. `clientId` is a random id for this install. The first `sync()` writes it to `<dataDir>/client-id` when that file is missing or empty, and later syncs read the same file. One id covers every subject on this `dataDir`. SyncBridge creates it. The host does not pass it in.

The server stores that id on each log row this install appends. Those rows come back as entries in log order with `own: true` and no attachments: this client downloads nothing for them and calls `applyChange` like for any other entry, so it replays the same log as every other copy. Another install has a different `client-id` file, so it receives the rows with their attachments. The same id is how the server counts a failed download for this install only: three failures skip that entry here, and other installs still receive it.

## 4. Host queue

`outbox()` reads `inflight/` then `incoming/` and does not wait for `sync()`. `setOutbox` waits until the current `sync()` finishes, then replaces both directories with the given records, in order, as `incoming/` files.

## 5. Situations

1. Incoming becomes inflight, then a new incoming takes the next batch. `sync()` is blocked on a POST of the current `inflight` row. Two `appendChanges` calls write only under `incoming/`. After the claim, those files are gone from `incoming/` and are the only files in `inflight/`, and `incoming/` is empty. Two more `appendChanges` calls, while that POST is blocked, land only in the new `incoming/`. The same `sync()` sends the original row, then the first pair, then the second pair.

2. Exit, then restart. `sync()` is blocked with one row in `inflight/`. `appendChanges` writes `incoming/`. The client is dropped before `sync()` finishes. A new client on the same `dataDir` sends the remaining `inflight` row first, then every file that was still in `incoming/`.
