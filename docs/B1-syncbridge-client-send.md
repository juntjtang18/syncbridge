# B1 — Client send

**Status:** 2026-10-19 · **Requirements:** [A0](./A0-syncbridge-requirements.md) §2

The client's queue is a directory, `dataDir/<subject>/outbox/`, with one JSON file per change named
`<seq>-<id>.json`. `<seq>` is zero-padded, so the directory order is the append order. It comes from
a counter set from the last file name when `init` opens the subject.

- `appendChanges` only creates files. It never waits for a running `sync()`.
- `sync()` only rewrites or deletes the oldest files it read. The two never touch the same file.

A send pass:

1. Read the oldest 20 files.
2. For each change, for each attachment without an `upload`: read the bytes through `readAttachment`,
   hash them, `PUT` them, check the answer's hash and size, and write `upload`, `sha256`, `size` into
   that change's file before the next upload.
3. `POST` the 20, asking for no entries back (`page: 0`): a phone sends its whole queue first, so the
   server gets new data as fast as the phone can upload. Delete each accepted file.
4. If the server named a refused change, add one to its `attempts` and clear its uploads, so the next
   pass uploads its bytes afresh; at 3 attempts it moves to `failures.json` and its file is deleted.
5. Once the queue is empty, post with `page: 200` and apply each returned page, moving the pointer
   after each entry; attachments download four at a time ahead of the applies.

A stop at any point loses nothing: the files still queued are sent by the next `sync()`, without
re-uploading attachments that already have an `upload`.
