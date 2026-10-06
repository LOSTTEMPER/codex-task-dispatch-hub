"""Reproducible synthetic performance evidence; no real hub or desktop data.
Run: PYTHONDONTWRITEBYTECODE=1 python3 tests/benchmark_audit.py
"""
import ast
import json
import platform
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import hub as hub_module
from hub import Hub,now,dump
from transport import DesktopIPC
from dispatch_extensions import DispatchExtensions
import test_hub as fixture
import test_team_cycles as cycle_fixture

BASE='37369ce12be7fac0d1760f411212c61f4078a5d1'

def original(file,cls,method,environment):
    source=subprocess.check_output(['git','show',BASE+':'+file],text=True)
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name==cls)
    function=next(n for n in node.body if isinstance(n,ast.FunctionDef) and n.name==method)
    environment=dict(environment)
    exec(compile(ast.Module(body=[function],type_ignores=[]),file,'exec'),environment)
    return environment[method]

def measure(fn):
    values=[]
    for _ in range(5):
        start=time.process_time();fn();values.append(time.process_time()-start)
    return dict(median_seconds=statistics.median(values),worst_seconds=max(values),samples_seconds=values)

class Socket:
    def __init__(self,chunk):self.chunk=chunk
    def recv(self,n):return b'x'*min(n,self.chunk)

def frame(method,size,chunk):
    ipc=DesktopIPC.__new__(DesktopIPC);ipc.socket=Socket(chunk)
    result=method(ipc,size)
    assert len(result)==size and result[0]==120 and result[-1]==120

report={'environment':dict(platform=platform.platform(),machine=platform.machine(),python=sys.version.split()[0],rounds=5,clock='process_time',profiler=False),'frames':[],'history':[],'graphs':[]}
old_frame=original('transport.py','DesktopIPC','_read_exact',{})
for chunk in (4096,65536):
    for mib in (1,4,8,16):
        size=mib*1024*1024
        entry=dict(mib=mib,chunk_bytes=chunk,before=measure(lambda:frame(old_frame,size,chunk)),after=measure(lambda:frame(DesktopIPC._read_exact,size,chunk)))
        report['frames'].append(entry);print('frame',mib,chunk,entry['after']['median_seconds'],flush=True)
old_render=original('hub.py','BaseHub','render',vars(hub_module))
for count in (3000,10000):
    f=fixture.HubTests();f.setUp()
    try:
        h=f.h;seed=dict(h.db.execute('SELECT * FROM requests LIMIT 1').fetchone())
        with h.transaction():
            h.db.execute('DELETE FROM outbox');h.db.execute('DELETE FROM requests');h.db.execute('DELETE FROM events')
            for i in range(count):
                item=dict(seed,id='r'+str(i),idempotency_key='r'+str(i),status='done')
                h.db.execute('INSERT INTO requests('+','.join(item)+') VALUES('+','.join('?' for _ in item)+')',list(item.values()))
                for j in range(4):h.event('a','request_test','e'+str(j),item['id'],'v1')
        h.render()
        after=measure(h.render)
        plans=[tuple(r) for r in h.db.execute("EXPLAIN QUERY PLAN SELECT * FROM events WHERE request_id='r1' ORDER BY seq")]
        with h.transaction():h.db.execute('DROP INDEX events_request_seq')
        before_plan=[tuple(r) for r in h.db.execute("EXPLAIN QUERY PLAN SELECT * FROM events WHERE request_id='r2' ORDER BY seq")]
        before=measure(lambda:old_render(h))
        entry=dict(requests=count,events=count*4,before=before,after=after,plan_before=before_plan,plan_after=plans)
        report['history'].append(entry);print('history',count,after['median_seconds'],flush=True)
    finally:f.tearDown()
old_graph=original('dispatch_extensions.py','DispatchExtensions','team_cycle_snapshots',dict(json=json,hashlib=__import__('hashlib'),dump=dump,TERMINAL={'done','cancelled','superseded'}))
for count in (12,16,20):
    f=cycle_fixture.TeamCycleTests();f.setUp()
    try:
        for i in range(count):
            for j in range(i+1,count):f.edge(f'{i}-{j}',str(i),str(j))
        def check(method):assert method(f.hub)==[]
        entry=dict(roles=count,edges=count*(count-1)//2,before=measure(lambda:check(old_graph)),after=measure(lambda:check(DispatchExtensions.team_cycle_snapshots)))
        report['graphs'].append(entry);print('graph',count,entry['after']['median_seconds'],flush=True)
    finally:f.tearDown()
Path('validation/audit-performance-20261006.json').write_text(json.dumps(report,indent=2)+'\n')
