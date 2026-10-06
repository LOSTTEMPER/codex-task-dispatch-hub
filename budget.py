"""Soft task budgets. No model execution, interruption, or provider billing controls."""
from __future__ import annotations

import datetime as dt
import json
import uuid


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds')


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def positive(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(name + '须为正整数')
    return value


def text(value, name, limit=6000):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(name + '须为非空有界文本')
    return value.strip()


ACTIVE_NOTICES = {'budget_warning', 'budget_limit'}
BUDGET_NOTICES = ACTIVE_NOTICES | {'budget_review', 'budget_resume', 'budget_clarify', 'budget_stop'}


class Budgets:
    def __init__(self, hub):
        self.h = hub
        self.db = hub.db
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS budgets(
          id TEXT PRIMARY KEY,version TEXT NOT NULL,owner TEXT NOT NULL,thread_id TEXT NOT NULL,
          token_limit INTEGER NOT NULL,warning_tokens INTEGER NOT NULL,revision INTEGER NOT NULL DEFAULT 1,
          state TEXT NOT NULL DEFAULT 'active',created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS budget_requests(
          request_id TEXT PRIMARY KEY,budget_id TEXT NOT NULL REFERENCES budgets(id));
        CREATE TABLE IF NOT EXISTS budget_runs(
          run_id TEXT PRIMARY KEY,budget_id TEXT NOT NULL REFERENCES budgets(id),estimate TEXT);
        CREATE TABLE IF NOT EXISTS budget_sources(
          thread_id TEXT PRIMARY KEY,path TEXT,offset INTEGER NOT NULL DEFAULT 0,inode TEXT,
          total INTEGER,turn_id TEXT,turn_started TEXT,turn_ended TEXT,
          parent_thread_id TEXT,inherited_budget TEXT,created TEXT,quality TEXT NOT NULL DEFAULT 'pending',
          issue TEXT,last_seen TEXT,epoch INTEGER NOT NULL DEFAULT 0,root_turn_id TEXT);
        CREATE TABLE IF NOT EXISTS budget_turns(
          thread_id TEXT NOT NULL,turn_id TEXT NOT NULL,budget_id TEXT,
          version TEXT,kind TEXT NOT NULL DEFAULT 'work',started TEXT,ended TEXT,
          PRIMARY KEY(thread_id,turn_id));
        CREATE TABLE IF NOT EXISTS budget_assignments(
          thread_id TEXT PRIMARY KEY,budget_id TEXT,version TEXT,kind TEXT NOT NULL DEFAULT 'work',
          delivery_id TEXT NOT NULL,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS budget_usage(
          event_id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,turn_id TEXT,budget_id TEXT,
          version TEXT,kind TEXT NOT NULL DEFAULT 'work',tokens INTEGER NOT NULL,
          counters TEXT NOT NULL,observed TEXT NOT NULL,recorded TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS budget_usage_account ON budget_usage(budget_id);
        CREATE INDEX IF NOT EXISTS budget_usage_turn ON budget_usage(thread_id,turn_id);
        CREATE TABLE IF NOT EXISTS budget_reviews(
          id TEXT PRIMARY KEY,budget_id TEXT NOT NULL REFERENCES budgets(id),state TEXT NOT NULL,
          revision INTEGER NOT NULL DEFAULT 1,report TEXT NOT NULL,decision TEXT,
          created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_open_budget_review ON budget_reviews(budget_id)
          WHERE state IN ('open','clarification');
        CREATE TABLE IF NOT EXISTS budget_review_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,review_id TEXT NOT NULL,actor TEXT NOT NULL,
          action TEXT NOT NULL,payload TEXT NOT NULL,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS budget_notice_receipts(
          delivery_id TEXT PRIMARY KEY,budget_id TEXT,thread_id TEXT NOT NULL,turn_id TEXT,
          state TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS budget_version_limits(version TEXT PRIMARY KEY,token_limit INTEGER NOT NULL);
        ''')
        if 'root_turn_id' not in {r[1] for r in self.db.execute('PRAGMA table_info(budget_sources)')}:
            self.db.execute('ALTER TABLE budget_sources ADD COLUMN root_turn_id TEXT')

    def account(self, identifier):
        row = self.db.execute('SELECT * FROM budgets WHERE id=?', (identifier,)).fetchone()
        if not row:
            raise ValueError('预算不存在')
        return row

    def owned(self, role, identifier):
        b = self.account(identifier)
        if b['owner'] != role:
            raise PermissionError('只能报告本执行者的预算；总管通过裁定操作处理')
        return b

    def used(self, identifier):
        return self.db.execute('SELECT COALESCE(SUM(tokens),0) FROM budget_usage WHERE budget_id=?', (identifier,)).fetchone()[0]

    def cap(self, version, extra):
        cap = self.db.execute('SELECT token_limit FROM budget_version_limits WHERE version=?', (version,)).fetchone()
        reserved = self.db.execute('SELECT COALESCE(SUM(token_limit),0) FROM budgets WHERE version=?', (version,)).fetchone()[0]
        if cap and reserved + extra > cap[0]:
            raise ValueError('超过已设置项目分配总额；须先由总管明确调整项目额度')

    def create(self, role, args, owner=None):
        self.h.require_manager(role)
        identifier = args.get('id') or 'budget-' + uuid.uuid4().hex
        if not isinstance(identifier, str) or not identifier or len(identifier) > 100:
            raise ValueError('预算标识无效')
        existing = self.db.execute('SELECT * FROM budgets WHERE id=?', (identifier,)).fetchone()
        if existing:
            raise ValueError('预算标识已存在；不可借创建操作重置用量')
        owner = owner or args.get('owner')
        member = self.h.member(owner)
        version = self.h.version(args.get('version'))['id']
        limit = positive(args.get('token_limit'), '总额度')
        warning = positive(args.get('warning_tokens'), '预警额度')
        if warning >= limit:
            raise ValueError('预警额度须小于总额度')
        self.cap(version, limit)
        self.db.execute('INSERT INTO budgets(id,version,owner,thread_id,token_limit,warning_tokens,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                        (identifier, version, owner, member['thread_id'], limit, warning, stamp(), stamp()))
        self.h.event(role, 'budget_created', f'{identifier} 分配 {limit} Token（含全部后代）', version=version)
        return identifier

    def attach_request(self, role, args, request_id, recipient):
        identifier = args.get('budget_id')
        if args.get('budget') is not None:
            if identifier:
                raise ValueError('budget与budget_id不可同时指定')
            spec = dict(args['budget'], version=args.get('version') or self.h.current())
            identifier = self.create(role, spec, recipient)
        if identifier:
            b = self.account(identifier)
            if b['owner'] != recipient or b['version'] != (args.get('version') or self.h.current()):
                raise ValueError('请求预算须属于同版本接收方')
            if b['state'] in ('stopped', 'closed'):
                raise ValueError('已停止或关闭的预算不能接新任务')
            self.db.execute('INSERT INTO budget_requests VALUES(?,?)', (request_id, identifier))
        elif self.h.meta('budget_enabled', False) and not args.get('unmanaged_reason'):
            raise ValueError('新请求须明确预算；仅行政协调可写unmanaged_reason说明未纳管原因')
        if not identifier and args.get('unmanaged_reason'):
            self.h.event(role, 'budget_unmanaged', text(args['unmanaged_reason'], '未纳管原因', 500), request_id)

    def request_budget(self, identifier):
        row = self.db.execute('SELECT budget_id FROM budget_requests WHERE request_id=?', (identifier,)).fetchone()
        return row[0] if row else None

    def prepare_delivery(self, row, active_turn=False):
        """Called under the same claim transaction, before the model sees the assignment."""
        if active_turn and row['kind'] in ACTIVE_NOTICES:
            # A notice about an old child task must never steal the attribution
            # of the parent's already-running, possibly unrelated native turn.
            return
        payload = json.loads(row['payload'])
        identifier = payload.get('budget_id') if row['kind'] in BUDGET_NOTICES else None
        kind = 'work'
        if row['kind'] == 'request':
            identifier = self.request_budget(row['request_id'])
        elif row['kind'] == 'budget_review':
            identifier = None
            kind = 'review'
        elif row['kind'] not in BUDGET_NOTICES:
            ids = payload.get('request_ids', []) + ([payload['request_id']] if payload.get('request_id') else [])
            found = set()
            for rid in ids:
                req = self.h.get_request(rid)
                if req['recipient'] == row['recipient']:
                    value = self.request_budget(rid)
                else:
                    run = self.db.execute('SELECT budget_id FROM budget_runs WHERE run_id=?', (req['run_id'],)).fetchone()
                    value = run[0] if run else None
                if value:
                    found.add(value)
            if len(found) > 1:
                raise ValueError('一次唤醒混合多个预算；需拆分投递或明确共用预算')
            identifier = next(iter(found), None)
        if identifier or kind == 'review':
            tid = self.h.member(row['recipient'])['thread_id']
            self.db.execute('INSERT INTO budget_assignments VALUES(?,?,?,?,?,?) ON CONFLICT(thread_id) DO UPDATE SET budget_id=excluded.budget_id,version=excluded.version,kind=excluded.kind,delivery_id=excluded.delivery_id,created=excluded.created',
                            (tid, identifier, row['version'], kind, row['id'], stamp()))
        else:
            self.db.execute('DELETE FROM budget_assignments WHERE thread_id=?', (self.h.member(row['recipient'])['thread_id'],))

    def bind_turn(self, thread, turn, identifier, version, kind='work', started=None):
        old = self.db.execute('SELECT * FROM budget_turns WHERE thread_id=? AND turn_id=?', (thread, turn)).fetchone()
        if old and old['budget_id'] and old['budget_id'] != identifier:
            raise ValueError('一个原生轮次不能计入两个预算')
        self.db.execute('INSERT INTO budget_turns VALUES(?,?,?,?,?,?,NULL) ON CONFLICT(thread_id,turn_id) DO UPDATE SET budget_id=excluded.budget_id,version=excluded.version,kind=excluded.kind,started=COALESCE(budget_turns.started,excluded.started)',
                        (thread, turn, identifier, version, kind, started))
        self.db.execute('UPDATE budget_usage SET budget_id=?,version=?,kind=? WHERE thread_id=? AND turn_id=? AND budget_id IS NULL',
                        (identifier, version, kind, thread, turn))

    def receipt_turn(self, row, turn_id):
        if not turn_id:
            return
        tid = self.h.member(row['recipient'])['thread_id']
        a = self.db.execute('SELECT * FROM budget_assignments WHERE thread_id=? AND delivery_id=?', (tid, row['id'])).fetchone()
        if a:
            self.bind_turn(tid, turn_id, a['budget_id'], a['version'], a['kind'])
            self.db.execute('DELETE FROM budget_assignments WHERE thread_id=?', (tid,))

    def begin(self, role, args, run_id):
        identifiers = {self.request_budget(r) for r in args.get('request_ids', [])}
        identifiers.discard(None)
        if args.get('budget_id'):
            identifiers.add(args['budget_id'])
        if len(identifiers) > 1:
            raise ValueError('同一原生轮次多个请求须显式共用一个预算')
        tid = self.h.member(role)['thread_id']
        source = self.db.execute('SELECT * FROM budget_sources WHERE thread_id=?', (tid,)).fetchone()
        if not identifiers and source and source['turn_id'] and not source['turn_ended']:
            t = self.db.execute('SELECT budget_id FROM budget_turns WHERE thread_id=? AND turn_id=?', (tid, source['turn_id'])).fetchone()
            if t and t[0]:
                identifiers.add(t[0])
        if not identifiers:
            return {'managed': False, 'note': '本轮未纳管；不会追溯扣除旧任务用量'}
        identifier = next(iter(identifiers))
        self.owned(role, identifier)
        if source and source['turn_id'] and not source['turn_ended']:
            self.bind_turn(tid, source['turn_id'], identifier, self.account(identifier)['version'], started=source['turn_started'])
        self.db.execute('INSERT INTO budget_runs(run_id,budget_id) VALUES(?,?) ON CONFLICT(run_id) DO NOTHING', (run_id, identifier))
        return dict(self.view(identifier), estimate_required=True,
                    instruction='先调用budget_estimate给预计范围、完成路径和收束余量；到额收束并budget_report，不能自批额度。')

    def view(self, identifier):
        result = dict(self.account(identifier))
        result['used_tokens'] = self.used(identifier)
        result['remaining_tokens'] = result['token_limit'] - result['used_tokens']
        sources = self.db.execute('SELECT thread_id,parent_thread_id,quality,issue,last_seen FROM budget_sources WHERE inherited_budget=? OR thread_id=?', (identifier, result['thread_id'])).fetchall()
        result['sources'] = [dict(x) for x in sources]
        unknown = self.db.execute('''WITH RECURSIVE descendants(thread_id) AS (
            SELECT thread_id FROM budget_sources WHERE parent_thread_id=?
            UNION SELECT s.thread_id FROM budget_sources s JOIN descendants d ON s.parent_thread_id=d.thread_id)
            SELECT s.thread_id,s.issue FROM budget_sources s JOIN descendants d ON s.thread_id=d.thread_id
            WHERE s.inherited_budget IS NULL AND s.quality!='ok' ''', (result['thread_id'],)).fetchall()
        result['unresolved_descendants'] = [dict(x) for x in unknown]
        collector = self.h.meta('budget_collector', {})
        result['coverage'] = 'complete' if sources and not unknown and all(x['quality'] == 'ok' for x in sources) and collector.get('state') == 'running' else 'incomplete'
        result['collector'] = collector
        row = self.db.execute("SELECT id,state,revision FROM budget_reviews WHERE budget_id=? AND state IN ('open','clarification')", (identifier,)).fetchone()
        result['review'] = dict(row) if row else None
        result['managed'] = True
        result['usage_by_thread'] = [dict(x) for x in self.db.execute('SELECT thread_id,SUM(tokens) tokens FROM budget_usage WHERE budget_id=? GROUP BY thread_id', (identifier,))]
        result['notice_policy'] = '程序不取消工具或封锁对话；收束与汇报消耗仍累计，允许可解释超额。'
        return result

    def emit(self, b, kind, key, payload, recipient=None):
        self.h.enqueue(recipient or b['owner'], kind, key,
                       dict(payload, budget_id=b['id'], budget_revision=b['revision']),
                       priority=1, version=b['version'])

    def evaluate(self):
        for b in self.db.execute("SELECT * FROM budgets WHERE state NOT IN ('closed','stopped')").fetchall():
            used = self.used(b['id'])
            case = self.db.execute("SELECT id FROM budget_reviews WHERE budget_id=? AND state IN ('open','clarification')", (b['id'],)).fetchone()
            if case:
                continue  # Live usage is read into the existing review, never another wake-up.
            level = 'limit' if used >= b['token_limit'] else 'warning' if used >= b['warning_tokens'] else None
            if not level:
                continue
            self.emit(b, 'budget_' + level, f"budget:{b['id']}:{b['revision']}:{level}", {'level': level})
            if level == 'limit':
                self.db.execute("UPDATE budgets SET state='report_due',updated=? WHERE id=?", (stamp(), b['id']))
                for row in self.db.execute("SELECT * FROM outbox WHERE kind='budget_warning' AND state='pending'").fetchall():
                    if json.loads(row['payload']).get('budget_id') == b['id']:
                        self.h.archive_wakeup(row, 'superseded_by_limit')

    def reconcile(self):
        for row in self.db.execute("SELECT * FROM outbox WHERE kind LIKE 'budget_%' AND state='pending'").fetchall():
            p = json.loads(row['payload']);b = self.account(p['budget_id'])
            stale = p.get('budget_revision') != b['revision']
            if row['kind'] in ACTIVE_NOTICES:
                stale = stale or b['state'] in ('closed','stopped','awaiting_decision')
            if row['kind'] == 'budget_review':
                c = self.db.execute('SELECT state,revision FROM budget_reviews WHERE id=?', (p['review_id'],)).fetchone()
                stale = not c or c['state'] != 'open' or c['revision'] != p['review_revision']
            if stale:
                self.h.archive_wakeup(row, 'budget_policy_or_review_changed')

    def notice(self, row):
        p = json.loads(row['payload']);v = self.view(p['budget_id'])
        result = {'event_id': row['id'], 'kind': row['kind'], 'budget': v,
                  'source': 'task-dispatch-hub actual local Codex token events; not account quota or billing',
                  'observed_at': stamp()}
        if p.get('review_id'):
            c = self.db.execute('SELECT * FROM budget_reviews WHERE id=?', (p['review_id'],)).fetchone()
            result['review'] = dict(c) if c else None
        result['instruction'] = {
            'budget_warning': '已接近任务预算（含全部子Agent）。检查剩余路径并预留收束余量；必要时提前budget_report。只调用budget_ack登记处理，不另发确认消息。',
            'budget_limit': '已达软预算（含全部子Agent）。停止新增实质开发，必要收束/保存后budget_report交代成果、证据、剩余工作与追加建议，再end进入预算评议等待。不得自批额度；程序没有取消工具或封锁对话。',
            'budget_review': '执行者需要预算裁定。读取budget_review_get，以成果价值和最小剩余工作决定追加、换路、分期、停止或澄清；budget_decide持久化。此评议独立于原业务等待。',
            'budget_resume': '总管裁定已持久化；读取budget_view及评议决定，按新范围继续原请求。先begin，保留原业务依赖；此恢复只执行一次。',
            'budget_clarify': '总管需补充说明。budget_report使用同一review_id补充，收束/澄清用量仍计入；不要新建普通wait或递归预算申请。',
            'budget_stop': '总管已决定停止此任务的新开发。保留成果、未完成需求去向和必要收束，不删除数据；记录结案，程序不强制中断运行。',
        }[row['kind']]
        result['entry'] = str(self.h.root / 'native_call.js')
        return result

    def report(self, role, args):
        b = self.owned(role, args['budget_id'])
        if b['state'] in ('stopped','closed'):
            raise ValueError('已终止预算不能重新申请；需总管明确重新安排')
        report = {key: text(args.get(key), key) for key in ('completed','evidence','remaining','recommendation')}
        additional = args.get('additional_tokens', 0)
        if isinstance(additional, bool) or not isinstance(additional, int) or additional < 0:
            raise ValueError('追加建议须为非负整数')
        report.update(additional_tokens=additional, used_tokens=self.used(b['id']), reported_at=stamp())
        row = self.db.execute("SELECT * FROM budget_reviews WHERE budget_id=? AND state IN ('open','clarification')", (b['id'],)).fetchone()
        if args.get('review_id') and (not row or row['id'] != args['review_id']):
            raise ValueError('评议编号不匹配')
        should_wake = row is None or row['state'] == 'clarification'
        if row:
            identifier = row['id'];revision = row['revision'] + int(should_wake)
            self.db.execute("UPDATE budget_reviews SET state='open',revision=?,report=?,updated=? WHERE id=?", (revision, encode(report), stamp(), identifier))
        else:
            identifier = 'budget-review-' + uuid.uuid4().hex;revision = 1
            self.db.execute('INSERT INTO budget_reviews VALUES(?,?,?,?,?,?,?,?)', (identifier, b['id'], 'open', revision, encode(report), None, stamp(), stamp()))
        self.db.execute('INSERT INTO budget_review_history(review_id,actor,action,payload,created) VALUES(?,?,?,?,?)', (identifier, role, 'report', encode(report), stamp()))
        self.db.execute("UPDATE budgets SET state='awaiting_decision',updated=? WHERE id=?", (stamp(), b['id']))
        if should_wake:
            self.emit(b, 'budget_review', f'budget-review:{identifier}:{revision}', {'review_id': identifier, 'review_revision': revision}, 'manager')
        self.ack_pending(role, b['id'])
        self.reconcile()
        return {'review_id': identifier, 'state': 'awaiting_decision', 'end_with': {'state': 'waiting', 'budget_review_id': identifier}}

    def decide(self, role, args):
        self.h.require_manager(role)
        c = self.db.execute('SELECT * FROM budget_reviews WHERE id=?', (args['review_id'],)).fetchone()
        if not c:
            raise ValueError('评议不存在')
        b = self.account(c['budget_id'])
        if b['owner'] == role:
            raise PermissionError('执行者不能自批自己的额度；总管自身任务需用户裁定，不向自己循环申请')
        if c['state'] != 'open' or args.get('expected_revision') != c['revision']:
            raise ValueError('评议已处理或修订不匹配')
        action = args.get('decision')
        if action not in ('increase','replan','phase','stop','clarify'):
            raise ValueError('裁定须为increase/replan/phase/stop/clarify')
        decision = {key: text(args.get(key), key) for key in ('reason','next_action','acceptance','remaining_scope')}
        decision['decision'] = action
        extra = args.get('additional_tokens', 0)
        if isinstance(extra, bool) or not isinstance(extra, int) or extra < 0:
            raise ValueError('追加须为非负整数')
        if action == 'increase' and not extra:
            raise ValueError('追加裁定需明确增加量')
        if action in ('stop','clarify') and extra:
            raise ValueError('停止或澄清不同时追加')
        self.cap(b['version'], extra)
        limit = b['token_limit'] + extra
        warning = args.get('warning_tokens', b['warning_tokens'] if not extra else max(b['warning_tokens'], limit * 4 // 5))
        positive(warning, '预警额度')
        if warning >= limit:
            raise ValueError('预警额度须小于总额度')
        decision.update(additional_tokens=extra, used_tokens=self.used(b['id']), decided_at=stamp())
        if action in ('increase','replan','phase') and limit <= self.used(b['id']):
            raise ValueError('当前额度已用尽，不能恢复新开发；请追加、停止或澄清')
        revision = b['revision'] + int(action != 'clarify')
        state = 'stopped' if action == 'stop' else 'awaiting_decision' if action == 'clarify' else 'active'
        self.db.execute('UPDATE budgets SET token_limit=?,warning_tokens=?,revision=?,state=?,updated=? WHERE id=?', (limit, warning, revision, state, stamp(), b['id']))
        self.db.execute('UPDATE budget_reviews SET state=?,decision=?,updated=? WHERE id=?', ('clarification' if action == 'clarify' else 'decided', encode(decision), stamp(), c['id']))
        self.db.execute('INSERT INTO budget_review_history(review_id,actor,action,payload,created) VALUES(?,?,?,?,?)', (c['id'], role, action, encode(decision), stamp()))
        fresh = self.account(b['id'])
        kind = 'budget_stop' if action == 'stop' else 'budget_clarify' if action == 'clarify' else 'budget_resume'
        self.emit(fresh, kind, f"budget-decision:{c['id']}:{c['revision']}", {'review_id': c['id']})
        self.reconcile()
        return {'review_id': c['id'], 'budget': self.view(b['id'])}

    def ack_pending(self, role, identifier, delivery_id=None):
        self.owned(role, identifier)
        matched = 0
        for row in self.db.execute("SELECT * FROM outbox WHERE recipient=? AND kind IN ('budget_warning','budget_limit','budget_resume','budget_clarify','budget_stop') AND state IN ('sending','delivered','uncertain','completed')", (role,)).fetchall():
            if json.loads(row['payload']).get('budget_id') != identifier or (delivery_id and row['id'] != delivery_id):
                continue
            matched += 1
            self.db.execute("INSERT INTO budget_notice_receipts VALUES(?,?,?,?,?,?) ON CONFLICT(delivery_id) DO UPDATE SET state='processed',updated=excluded.updated",
                (row['id'],identifier,self.h.member(role)['thread_id'],row['turn_id'],'processed',stamp()))
            self.db.execute("UPDATE outbox SET state='completed',updated=? WHERE id=?", (stamp(), row['id']))
        if delivery_id and not matched:
            raise ValueError('未找到属于本预算执行者的可确认投递；不能返回虚假的processed')
        return matched

    def check_wait(self, role, args):
        identifier = args.get('budget_review_id')
        if not identifier:
            return False
        c = self.db.execute('SELECT * FROM budget_reviews WHERE id=?', (identifier,)).fetchone()
        if not c or c['state'] not in ('open','clarification'):
            raise ValueError('预算评议不存在或已裁定')
        self.owned(role, c['budget_id'])
        if args.get('state') != 'waiting':
            raise ValueError('预算评议等待须使用waiting')
        return True

    def call(self, role, op, args):
        if op == 'budget_view':
            return self.view(args['budget_id'])
        if op == 'budget_list':
            return [self.view(x[0]) for x in self.db.execute('SELECT id FROM budgets WHERE version=?', (args.get('version') or self.h.current(),))]
        if op == 'budget_version_view':
            version = self.h.version(args.get('version'), writable=False)['id']
            counts = {r[0]:r[1] for r in self.db.execute('SELECT kind,SUM(tokens) FROM budget_usage WHERE version=? GROUP BY kind', (version,))}
            cap = self.db.execute('SELECT token_limit FROM budget_version_limits WHERE version=?', (version,)).fetchone()
            return {'version': version, 'allocated_tokens': self.db.execute('SELECT COALESCE(SUM(token_limit),0) FROM budgets WHERE version=?', (version,)).fetchone()[0],
                    'allocation_limit': cap[0] if cap else None, 'work_tokens': counts.get('work',0),
                    'coordination_tokens': counts.get('review',0), 'total_recorded_tokens': sum(counts.values()),
                    'legacy_unmanaged_usage': 'excluded; not retroactively assigned'}
        if op == 'budget_review_get':
            c = self.db.execute('SELECT * FROM budget_reviews WHERE id=?', (args['review_id'],)).fetchone()
            if not c:
                raise ValueError('评议不存在')
            return dict(dict(c), budget=self.view(c['budget_id']))
        with self.h.transaction():
            if op == 'budget_configure':
                self.h.require_manager(role)
                if 'enabled' in args:
                    if not isinstance(args['enabled'], bool):
                        raise ValueError('enabled须为布尔值')
                    self.h.set_meta('budget_enabled', args['enabled'])
                if 'version_token_limit' in args:
                    version = self.h.version(args.get('version'))['id'];limit = positive(args['version_token_limit'], '项目额度')
                    allocated = self.db.execute('SELECT COALESCE(SUM(token_limit),0) FROM budgets WHERE version=?', (version,)).fetchone()[0]
                    if limit < allocated:
                        raise ValueError('项目总额不能小于已分配额度')
                    self.db.execute('INSERT INTO budget_version_limits VALUES(?,?) ON CONFLICT(version) DO UPDATE SET token_limit=excluded.token_limit', (version, limit))
                result = {'enabled': self.h.meta('budget_enabled', False), 'external_billing_authorized': False}
            elif op == 'budget_create':
                result = self.view(self.create(role, args))
            elif op == 'budget_estimate':
                run = self.h.active_run(role)
                if not run:
                    raise ValueError('先begin再估算')
                r = self.db.execute('SELECT budget_id FROM budget_runs WHERE run_id=?', (run['id'],)).fetchone()
                if not r:
                    raise ValueError('本轮未绑定预算')
                low = positive(args.get('low'), '预计下限');high = positive(args.get('high'), '预计上限');reserve = positive(args.get('reserve'), '收束余量')
                if high < low:
                    raise ValueError('预计范围倒置')
                estimate = dict(low=low, high=high, reserve=reserve, plan=text(args.get('plan'), '完成路径'))
                self.db.execute('UPDATE budget_runs SET estimate=? WHERE run_id=?', (encode(estimate), run['id']))
                result = dict(estimate, budget=self.view(r[0]))
            elif op == 'budget_report':
                result = self.report(role, args)
            elif op == 'budget_decide':
                result = self.decide(role, args)
            elif op == 'budget_ack':
                self.ack_pending(role, args['budget_id'], args['delivery_id']);result = {'processed': True}
            elif op == 'budget_close':
                self.h.require_manager(role);b = self.account(args['budget_id'])
                pending = self.db.execute("SELECT 1 FROM budget_requests br JOIN requests r ON r.id=br.request_id WHERE br.budget_id=? AND r.status NOT IN ('done','cancelled','superseded')", (b['id'],)).fetchone()
                active = self.db.execute('SELECT 1 FROM budget_sources WHERE (inherited_budget=? OR thread_id=?) AND turn_id IS NOT NULL AND turn_ended IS NULL', (b['id'], b['thread_id'])).fetchone()
                if pending or active:
                    raise ValueError('任务或后代仍在进行，不能关闭预算')
                self.db.execute("UPDATE budgets SET state='closed',updated=? WHERE id=?", (stamp(), b['id']))
                result = self.view(b['id'])
            else:
                raise ValueError('未知预算操作')
            self.reconcile()
        self.h.render()
        return result
