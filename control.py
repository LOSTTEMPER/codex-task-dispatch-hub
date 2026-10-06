#!/usr/bin/env python3
"""Explicit start/status/stop, run from an authorized Codex terminal."""
import argparse
import json
import os
import subprocess
import sys
from hub import Hub, ROOT
from lifecycle import request_stop


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start','status','stop'])
    args = parser.parse_args()
    hub = Hub()
    try:
        if args.action == 'status':
            result = hub.status()
        elif args.action == 'start':
            with open(hub.state/'worker.out.log','a') as out, open(hub.state/'worker.err.log','a') as err:
                process = subprocess.Popen([sys.executable,str(ROOT/'worker.py')],cwd=str(ROOT),
                    stdout=out,stderr=err,stdin=subprocess.DEVNULL,start_new_session=True,
                    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
            result = {'launched_pid':process.pid,'note':'worker.lock 防止重复进程；稍后 status 核对心跳'}
        else:
            try:
                result = request_stop(hub.state/'worker-control.sock')
            except (OSError, ValueError) as error:
                raise SystemExit('无法确认中枢控制端点，未向任何PID发信号：' + str(error))
        print(json.dumps(result,ensure_ascii=False))
    finally:
        hub.close()


if __name__ == '__main__':
    main()
