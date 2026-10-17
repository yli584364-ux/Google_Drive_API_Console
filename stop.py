import sys
import requests

origin='http://127.0.0.1:'+str(int(sys.argv[1]) if len(sys.argv)>1 else 8787)
try:
    with requests.Session() as session:
        session.trust_env=False
        data=session.get(origin+'/api/bootstrap',timeout=5).json()
        r=session.post(origin+'/api/service/stop',json={},headers={'Origin':origin,'X-CSRF-Token':data['csrf']},timeout=10)
        if r.status_code!=202:
            print('无法停止，请关闭工作台服务终端。')
            raise SystemExit(1)
        print('已请求正常停止。正在进行的任务会保存进度并暂停。')
except requests.RequestException:
    print('无法连接服务，它可能已经停止。')
