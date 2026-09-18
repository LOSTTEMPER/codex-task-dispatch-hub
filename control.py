#!/usr/bin/env python3
"""Explicit start/status/stop, run from an authorized Codex terminal."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from hub import Hub,ROOT

p=argparse.ArgumentParser();p.add_argument('action',choices=['start','status','stop']);a=p.parse_args()
h=Hub();state=h.meta('worker',{});pid=state.get('pid')
if a.action=='status':print(json.dumps(h.status(),ensure_ascii=False))
elif a.action=='start':
 with open(h.state/'worker.out.log','a') as out,open(h.state/'worker.err.log','a') as err:
  process=subprocess.Popen([sys.executable,str(ROOT/'worker.py')],cwd=str(ROOT),stdout=out,stderr=err,stdin=subprocess.DEVNULL,start_new_session=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
 print(json.dumps({'launched_pid':process.pid,'note':'worker.lock 防止重复进程；稍后 status 核对心跳'},ensure_ascii=False))
else:
 if not pid:raise SystemExit('没有已登记的后台进程')
 command=subprocess.check_output(['/bin/ps','-p',str(pid),'-o','command='],text=True).strip()
 if str(ROOT/'worker.py') not in command:raise SystemExit('PID 已复用或进程不同，拒绝停止')
 os.kill(pid,signal.SIGTERM);print('已请求中枢停止；不删除队列与记录')
h.close()
