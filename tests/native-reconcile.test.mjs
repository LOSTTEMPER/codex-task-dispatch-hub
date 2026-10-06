import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const entry = new Function('tools', 'operation', 'payload', readFileSync(new URL('../native_call.js', import.meta.url), 'utf8'));
const payload = { delivery_id: 'old', expected_thread_id: 'art', expected_turn_id: 'old-turn' };
const wrap = value => ({ content: [{ type: 'text', text: JSON.stringify(value) }] });
function fixture(mode, target = 'art') {
  const calls = [], sent = [], commits = [];
  const latest = { id: 'new-turn', status: 'completed', error: null, startedAt: 20, completedAt: 30 };
  let polls = 0;
  const tools = {
    async exec_command({ cmd }) {
      const op = cmd.match(/ call '([^']+)' --json /)?.[1];
      calls.push(op);
      let result;
      if (op === 'delivery_reconcile_native_prepare') result = { thread_id: target,
        turn_id: 'old-turn', token: 'fixture', mode: 'native-turn' };
      else if (op === 'delivery_reconcile_native_commit') {
        const args = JSON.parse(cmd.match(/ --json '(.*)'$/)[1]);
        commits.push(args);
        result = { state: 'completed', thread_id: target };
      }
      else if (op === 'delivery_candidates') result = { calling_thread_id: 'manager', deliveries: [
        { delivery_id: 'next', thread_id: target }, { delivery_id: 'untouched-i', thread_id: 'integration' }] };
      else if (op === 'delivery_claim_native') result = { thread_id: target, message: 'registered exact message' };
      else if (op === 'delivery_receipt_native') result = { state: 'delivered' };
      else throw Error(`unexpected ${op}`);
      return { output: JSON.stringify({ ok: true, result }) };
    },
    async mcp__codex_app__wait_threads() {
      polls++;
      if (mode === 'unknown') return wrap({ polls: [] });
      return wrap({ polls: [{ thread: { id: target, status: { type: mode === 'active' || polls === 4 ? 'active' : 'idle' } }, latestTurn: latest }] });
    },
    async mcp__codex_app__read_thread() {
      if (mode === 'read-error') return { isError: true, content: [{ type: 'text', text: 'unavailable' }] };
      const items = [];
      return wrap({ thread: { id: target, status: { type: 'idle' } }, page: { order: 'newest_first', hasMore: false },
        turns: [{ ...latest, items }, { id: 'old-turn', status: 'completed', error: null, startedAt: 10, completedAt: 15 }] });
    },
    async mcp__codex_app__send_message_to_thread(args) {
      sent.push(args);
      return wrap({ threadId: args.threadId });
    }
  };
  return { tools, calls, sent, commits };
}

test('completed evidence leads to commit then existing native drain only for art', async () => {
  const f = fixture('completed');
  const result = await entry(f.tools, 'delivery_reconcile_native', payload);
  assert.equal(result.result.state, 'completed');
  assert.deepEqual(f.calls, ['delivery_reconcile_native_prepare', 'delivery_reconcile_native_commit',
    'delivery_candidates', 'delivery_claim_native', 'delivery_receipt_native']);
  assert.deepEqual(f.sent, [{ threadId: 'art', prompt: 'registered exact message' }]);
});
for (const mode of ['active', 'unknown', 'read-error']) {
  test(`${mode} refuses without commit, claim or send`, async () => {
    const f = fixture(mode);
    const result = await entry(f.tools, 'delivery_reconcile_native', payload);
    assert.equal(result.result.reconciled, false);
    assert.equal(result.result.commitAttempted, false);
    assert.deepEqual(f.calls, ['delivery_reconcile_native_prepare']);
    assert.equal(f.sent.length, 0);
  });
}
test('caller-supplied proof and internal steps cannot enter public native path', async () => {
  const f = fixture('completed');
  await assert.rejects(entry(f.tools, 'delivery_reconcile_native', { ...payload, proof: {} }));
  await assert.rejects(entry(f.tools, 'delivery_reconcile_native_commit', payload));
  assert.deepEqual(f.calls, []);
});

