import argparse
import os
from pathlib import Path
import socket
import threading
import webbrowser

import uvicorn
from app.main import create_app

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description='Drive 本地续传工作台')
    parser.add_argument('--port', type=int, default=8787)
    parser.add_argument('--no-browser', action='store_true')
    args=parser.parse_args()
    origin=f'http://127.0.0.1:{args.port}'
    with socket.socket() as probe:
        occupied=probe.connect_ex(('127.0.0.1', args.port))==0
    if occupied:
        print('端口已被使用，请打开已有工作台或使用 --port 指定其他端口。', flush=True)
        if not args.no_browser: webbrowser.open(origin)
        raise SystemExit(1)
    app=create_app(origin=origin)
    if not args.no_browser:
        threading.Timer(1.5, lambda:webbrowser.open(origin)).start()
    print('工作台地址：'+origin+'；关闭终端会暂停服务，源文件不会删除。', flush=True)
    server=uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=args.port, access_log=False, log_level='warning'))
    app.state.shutdown_callback=lambda:setattr(server,'should_exit',True)
    server.run()
