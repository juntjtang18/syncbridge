import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import * as fs from 'node:fs/promises';
import { createServer } from '../src/sync-server.js';
import { createNodeServerStorage } from '../src/node-server-storage.js';
import { tempDir } from './helpers.js';

for (const outcome of ['failure', 'success', 'third-success', 'hook-rejects', 'no-hook']) {
  test(`failure hook ordering: ${outcome}`, async t => {
    const root = await tempDir(t), storage = createNodeServerStorage(root);
    const upload = '11111111-1111-1111-1111-111111111111', file = path.join(root, 'uploads', upload);
    await fs.writeFile(file, 'original');
    const value = { data: { original: true }, attachments: [{ id: 'photo', upload, size: 8 }] };
    storage.appendEntry('user.28', 'entry-1', value, 'phone-1'); storage.enqueue('user.28', 1, 'entry-1', value);
    const events = []; let attempts = 0, hookCalls = 0;
    const server = createServer({ storage, currentSubject: () => 'user.28', log: { write: e => events.push(e) } });
    server.init('user.28', async entry => {
      attempts++; entry.package.data.original = false;
      if (outcome === 'success' || (outcome === 'third-success' && attempts === 3)) return;
      throw new Error('drive failed');
    }, outcome === 'no-hook' ? {} : { onApplyFailure: async info => {
      hookCalls++;
      assert.equal(attempts, 3); assert.equal(info.attempts, 3); assert.equal(info.entry.clientId, 'phone-1');
      assert.equal(info.entry.package.data.original, true); assert.equal(info.error.message, 'drive failed');
      assert.equal(await fs.readFile(file, 'utf8'), 'original');
      await new Promise(r => setTimeout(r, 5));
      assert.equal(storage.hasUnprocessed(), true);
      if (outcome === 'hook-rejects') throw new Error('disk full');
      await fs.rename(file, path.join(root, 'saved'));
    } });
    await server.whenIdle();
    assert.equal(storage.hasUnprocessed(), false);
    assert.equal(hookCalls, ['failure', 'hook-rejects'].includes(outcome) ? 1 : 0);
    await assert.rejects(fs.stat(file), { code: 'ENOENT' });
    if (outcome === 'failure') assert.equal(await fs.readFile(path.join(root, 'saved'), 'utf8'), 'original');
    assert.equal(storage.failedEntries('user.28').length, ['success', 'third-success'].includes(outcome) ? 0 : 1);
    assert.equal(events.some(e => e.step === 'failure-archive-failed'), outcome === 'hook-rejects');
    await server.close();
  });
}
