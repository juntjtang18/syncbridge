export function createLog(log) {
  if (log == null) return { write() {} }
  if (typeof log.write !== "function") throw new TypeError("log.write must be a function")
  return {
    write(event) {
      try {
        log.write(event)
      } catch {
        // A host log must not change the sync result.
      }
    },
  }
}

export function trace(log, event) {
  const entry = { at: new Date().toISOString() }
  for (const [key, value] of Object.entries(event)) {
    if (value !== undefined) entry[key] = value
  }
  log.write(entry)
}
