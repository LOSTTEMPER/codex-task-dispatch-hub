"""Read-only Codex metadata collector; never stores prompts, messages or credentials."""
from __future__ import annotations
from collections import deque
import hashlib
import json
from pathlib import Path
import sqlite3
from budget import stamp, encode

MAX_COUNTER = 2**63 - 1


def counter(value):
    if type(value) is not int or not 0 <= value <= MAX_COUNTER:
        raise ValueError('token counter must be a nonnegative signed 64-bit integer')
    return value


class UsageCollector:
    def __init__(self, hub, catalog=None, cancel=None):
        self.cancel = cancel
        self.h = hub
        self.db = hub.db
        self.catalog = Path(catalog or Path.home() / '.codex/state_5.sqlite')

    def collect(self):
        if not self.db.execute('SELECT 1 FROM budgets LIMIT 1').fetchone():
            return
        catalog = None
        try:
            catalog = sqlite3.connect(self.catalog.as_uri() + '?mode=ro', uri=True)
            catalog.row_factory = sqlite3.Row
            roots = [r[0] for r in self.db.execute('SELECT thread_id FROM members')]
            # Only inspect registered threads and their recursively enumerated children.
            pending = deque((r, None) for r in roots); seen = set()
            while pending:
                if self.cancel is not None and self.cancel.is_set():
                    return
                tid, parent = pending.popleft()
                if tid in seen:
                    continue
                seen.add(tid)
                meta = catalog.execute('SELECT id,rollout_path,created_at_ms,model_provider FROM threads WHERE id=?', (tid,)).fetchone()
                with self.h.transaction():
                    self.scan(tid, parent, meta)
                pending.extend((r[0], tid) for r in catalog.execute('SELECT child_thread_id FROM thread_spawn_edges WHERE parent_thread_id=?', (tid,)))
            with self.h.transaction():
                self.h.set_meta('budget_collector', {'state': 'running', 'checked_at': stamp(), 'threads': len(seen)})
                self.h.budgets.evaluate()
                self.h.budgets.reconcile()
        except (OSError, sqlite3.Error, ValueError) as e:
            with self.h.transaction():
                self.h.set_meta('budget_collector', {'state': 'incomplete', 'checked_at': stamp(), 'error': str(e)[:200]})
        finally:
            if catalog is not None:
                catalog.close()

    def scan(self, tid, parent, meta):
        source = self.db.execute('SELECT * FROM budget_sources WHERE thread_id=?', (tid,)).fetchone()
        fresh = source is None
        if fresh:
            self.db.execute('INSERT INTO budget_sources(thread_id,parent_thread_id,created) VALUES(?,?,?)', (tid, parent, stamp()))
            source = self.db.execute('SELECT * FROM budget_sources WHERE thread_id=?', (tid,)).fetchone()
        s = dict(source)
        initialized = s['path'] is not None and s['inode'] is not None
        if not initialized:
            s.update(quality='pending', issue=None)
        self.inherit(s)
        try:
            if not meta or not meta['rollout_path']:
                raise ValueError('thread metadata unavailable')
            if meta['model_provider'] != 'openai':
                raise ValueError('unverified provider counter')
            path = Path(meta['rollout_path']); st = path.stat(); inode = str(st.st_ino)
            if initialized and (s['path'] != str(path) or s['inode'] != inode or st.st_size < s['offset']):
                # Do not guess/replay after replacement; expose the accounting gap.
                raise ValueError('source replaced or truncated; reconciliation required')
            s.update(path=str(path), inode=inode)
            with path.open('rb') as stream:
                stream.seek(s['offset'])
                while True:
                    if self.cancel is not None and self.cancel.is_set():
                        break
                    offset = stream.tell(); line = stream.readline()
                    if not line or not line.endswith(b'\n'):
                        break  # Partial record is retried at exactly the same offset.
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        raise ValueError('record must be an object at offset ' + str(offset))
                    if obj.get('type') == 'event_msg':
                        self.event(s, obj, offset, prime=not initialized and parent is None)
                    s['offset'] = stream.tell()
            # A discovered descendant without a binding is retained for diagnosis;
            # historic descendants are not retroactively charged to a new task.
            if s['quality'] != 'gap':
                s.update(quality='ok', issue=None)
        except (OSError, ValueError) as e:
            s.update(quality='gap', issue=str(e)[:200])
        s['last_seen'] = stamp()
        fields = ['path','offset','inode','total','turn_id','turn_started','turn_ended','inherited_budget','quality','issue','last_seen','epoch','root_turn_id']
        self.db.execute('UPDATE budget_sources SET '+','.join(k+'=?' for k in fields)+' WHERE thread_id=?', [s[k] for k in fields]+[tid])

    def inherit(self, s):
        if not s['parent_thread_id'] or s['inherited_budget']:
            return
        ancestor = self.db.execute('SELECT budget_id FROM budget_turns WHERE turn_id=? AND budget_id IS NOT NULL', (s['root_turn_id'],)).fetchone()
        parent = self.db.execute('SELECT inherited_budget FROM budget_sources WHERE thread_id=?', (s['parent_thread_id'],)).fetchone()
        inherited = (parent[0] if parent else None) or (ancestor[0] if ancestor else None)
        if inherited:
            s['inherited_budget'] = inherited
            b = self.h.budgets.account(inherited)
            # Repair late native receipts without replaying counters.
            turns = {r[0] for r in self.db.execute('SELECT DISTINCT turn_id FROM budget_usage WHERE thread_id=?', (s['thread_id'],))}
            if s['turn_id']:
                turns.add(s['turn_id'])
            for turn in turns:
                if turn:
                    self.h.budgets.bind_turn(s['thread_id'],turn,inherited,b['version'],started=s['turn_started'])

    def event(self, s, obj, offset, prime=False):
        p = obj.get('payload', {})
        if not isinstance(p, dict):
            raise ValueError('event payload must be an object')
        kind = p.get('type'); tid = s['thread_id']; observed = obj.get('timestamp', stamp())
        if not isinstance(observed, str):
            raise ValueError('invalid event timestamp')
        for key in ('turn_id', 'root_turn_id'):
            if p.get(key) is not None and not isinstance(p[key], str):
                raise ValueError('invalid ' + key)
        if kind == 'task_started':
            s.update(turn_id=p.get('turn_id'), turn_started=observed, turn_ended=None,root_turn_id=p.get('root_turn_id'))
            self.inherit(s)
            if not s['turn_id']:
                s.update(quality='gap', issue='task_started missing turn id'); return
            binding = self.db.execute('SELECT * FROM budget_turns WHERE thread_id=? AND turn_id=?', (tid, s['turn_id'])).fetchone()
            # A root_turn_id comes from Codex, and avoids guessing child ownership
            # from titles, working directories or temporal proximity.
            inherited = s['inherited_budget']
            if s['parent_thread_id'] and not inherited:
                ancestor = self.db.execute('SELECT budget_id FROM budget_turns WHERE turn_id=? AND budget_id IS NOT NULL', (p.get('root_turn_id'),)).fetchone()
                parent = self.db.execute('SELECT inherited_budget FROM budget_sources WHERE thread_id=?', (s['parent_thread_id'],)).fetchone()
                inherited = (parent[0] if parent else None) or (ancestor[0] if ancestor else None)
                s['inherited_budget'] = inherited
            # An assignment is intent, not evidence that this native turn was
            # sent by it. receipt_turn or explicit begin binds actual root work.
            if not prime and not binding and inherited:
                b = self.h.budgets.account(inherited)
                self.h.budgets.bind_turn(tid, s['turn_id'], inherited, b['version'], started=observed)
            elif binding:
                self.db.execute('UPDATE budget_turns SET started=COALESCE(started,?) WHERE thread_id=? AND turn_id=?', (observed,tid,s['turn_id']))
        elif kind == 'task_complete':
            if not p.get('turn_id') or p['turn_id'] == s['turn_id']:
                s['turn_ended'] = observed
                self.db.execute('UPDATE budget_turns SET ended=? WHERE thread_id=? AND turn_id=?', (observed,tid,s['turn_id']))
        elif kind == 'token_count' and p.get('info') is not None:
            info = p['info']
            if not isinstance(info, dict) or not isinstance(info.get('total_token_usage', {}), dict) or not isinstance(info.get('last_token_usage', {}), dict):
                raise ValueError('invalid token usage shape')
            totals = info.get('total_token_usage', {}); total = totals.get('total_tokens')
            last = info.get('last_token_usage', {}).get('total_tokens')
            counter(total)
            if last is not None:
                counter(last)
                if last > total:
                    raise ValueError('last token usage exceeds cumulative counter')
            names = ('total_tokens','input_tokens','cached_input_tokens','cache_write_input_tokens','output_tokens','reasoning_output_tokens')
            for counters in (totals, info.get('last_token_usage', {})):
                for key in names:
                    if key in counters: counter(counters[key])
            previous = s['total']
            if previous is not None: counter(previous)
            if previous is not None and total < previous: counter(s['epoch'] + 1)
            binding = self.db.execute('SELECT * FROM budget_turns WHERE thread_id=? AND turn_id=?', (tid,s['turn_id'])).fetchone()
            if prime and not binding:
                assignment = self.db.execute('SELECT created FROM budget_assignments WHERE thread_id=?', (tid,)).fetchone()
                # Retain post-assignment counters as UNMANAGED evidence so a
                # later actual turn receipt can backfill them. Time never binds.
                if not assignment or not s['turn_started'] or s['turn_started'] < assignment['created']:
                    s['total'] = total
                    return
            if previous is None:
                # Fork history can seed a cumulative total; only its first new
                # model response is chargeable. Last usage is not added twice.
                delta = last if isinstance(last, int) and 0 <= last <= total else None
            elif total < previous:
                delta = last if isinstance(last, int) and 0 <= last <= total else None
            else:
                delta = total - previous
            if delta is None:
                s['total'] = total
                s.update(quality='gap', issue='initial usage unavailable'); return
            binding = self.db.execute('SELECT * FROM budget_turns WHERE thread_id=? AND turn_id=?', (tid,s['turn_id'])).fetchone()
            event = hashlib.sha256(encode([tid,s['path'],s['inode'],offset]).encode()).hexdigest()
            if delta and not self.db.execute('SELECT 1 FROM budget_usage WHERE event_id=?', (event,)).fetchone():
                # Keep every SQLite SUM used by account/version views in range.
                # Reject the source record before changing its safe baseline.
                if binding and binding['budget_id']:
                    counter(self.h.budgets.used(binding['budget_id']) + delta)
                if binding and binding['version']:
                    used = self.db.execute('SELECT COALESCE(SUM(tokens),0) FROM budget_usage WHERE version=?', (binding['version'],)).fetchone()[0]
                    counter(used + delta)
            s['total'] = total
            if previous is not None and total < previous:
                s['epoch'] += 1
                s.update(quality='gap', issue='counter reset; first response only, gap retained')
            if delta == 0:
                return
            counters = {k:v for k,v in totals.items() if k in names}
            self.db.execute('INSERT OR IGNORE INTO budget_usage VALUES(?,?,?,?,?,?,?,?,?,?)',
                (event,tid,s['turn_id'],binding['budget_id'] if binding else None,binding['version'] if binding else None,binding['kind'] if binding else 'unmanaged',delta,encode(counters),observed,stamp()))
