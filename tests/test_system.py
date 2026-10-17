import hashlib
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.auth import AuthManager
from app.drive import committed, folder_id
from app.engine import Engine, Files
from app.main import create_app
from app.storage import AppError, Store, Vault, InstanceLock


class FakeAuth:
    def __init__(self, store, vault, origin):
        self.config_path=''; self.pending=None; self.busy=lambda:False
    def identity(self): return {'email':'test@example.com','client_id':'test-client'}
    def status(self): return {'connected':True,'email':'test@example.com','pending':False}
    def configure(self,path,remember=True):self.config_path=path;return {'path':path}


class Response:
    def __init__(self,status,headers=None):self.status_code=status;self.headers=headers or {}


class Remote:
    def __init__(self):
        self.data=b'';self.expected=b'';self.mode=None;self.first=True;self.offsets=[]
        self.entered=threading.Event();self.release=threading.Event();self.final=False
        self.init_count=0;self.reserve_count=0;self.md5_bad=False;self.expired=False
        self.existing=None
    def factory(self,auth):return self
    def folder(self,value):return {'id':value,'name':'目标目录'}
    def find_files(self,*args):return [self.existing] if self.existing else []
    def reserve(self):self.reserve_count+=1;return 'remote-id'
    def initiate(self,job):self.init_count+=1;self.job=job;return 'https://www.googleapis.com/upload/test'
    def metadata(self,identity):
        if self.existing:return self.existing
        if not self.final:return None
        return {'id':identity,'name':self.job['name'],'size':str(len(self.data)),
                'md5Checksum':'wrong' if self.md5_bad else hashlib.md5(self.data).hexdigest(),
                'parents':['folder1'],'ownedByMe':True,'webViewLink':'https://drive.google.com/file/d/remote-id/view'}
    def probe(self,*args):
        if self.expired:return Response(404)
        if self.final:return Response(200)
        return Response(308,{'Range':f'bytes=0-{len(self.data)-1}'} if self.data else {})
    def chunk(self,uri,data,start,size):
        assert start==len(self.data)
        self.offsets.append(start)
        if self.first and self.mode=='network':
            self.first=False;self.data+=data[:65536]
            raise AppError('network','transient',503)
        if self.mode=='always_network':raise AppError('network','transient',503)
        if self.first and self.mode=='blocked':
            self.first=False;self.entered.set();assert self.release.wait(5)
        self.data+=data
        if len(self.data)==size:self.final=True;return Response(200)
        return Response(308,{'Range':f'bytes=0-{len(self.data)-1}'})
    def expect(self,r,statuses=(200,)):
        if r.status_code not in statuses:raise AppError('bad response')
        return r


@pytest.fixture
def rig(tmp_path):
    source=tmp_path/'source';source.mkdir();path=source/'large.zip';data=b'1234567890abcdef'*170000;path.write_bytes(data)
    store=Store(tmp_path/'db.sqlite3');vault=Vault(tmp_path/'secrets');auth=FakeAuth(store,vault,'http://127.0.0.1:8787')
    files=Files(store,auth,tmp_path/'application');root=files.register(str(source));f=files.resolve(root['id'],path=str(path))[0]
    remote=Remote();remote.expected=data
    engine=Engine(store,vault,auth,files,remote.factory,retry_scale=.001)
    j=engine.create(f['id'],{'id':'folder1','name':'目标目录'},1)
    yield SimpleNamespace(store=store,vault=vault,auth=auth,files=files,engine=engine,remote=remote,path=path,data=data,job=j,root=root)
    remote.release.set();engine.stop()


def run(rig):
    r=rig;r.engine.start();r.engine.action(r.job['id'],'resume')
    return r


def wait_state(r,states,timeout=6):
    end=time.time()+timeout
    while time.time()<end:
        j=r.store.get('jobs',r.job['id'])
        if j['state'] in states:return j
        time.sleep(.02)
    raise AssertionError(r.store.get('jobs',r.job['id']))


def test_real_vault_encrypts_and_round_trips(tmp_path):
    v=Vault(tmp_path);v.put('test',{'token':'fake-token'})
    assert b'fake-token' not in v.path('test').read_bytes()
    assert v.get('test')=={'token':'fake-token'}


def test_upload_handles_partial_network_commit_and_md5(rig):
    rig.remote.mode='network';run(rig)
    j=wait_state(rig,{'completed'})
    assert rig.remote.offsets[:2]==[0,65536]
    assert rig.remote.data==rig.data
    assert j['confirmed']==len(rig.data)
    assert rig.path.read_bytes()==rig.data
    assert rig.vault.get('session-'+j['id'])=={}


def test_zero_byte_file_can_finish_without_invalid_byte_range(rig):
    rig.path.write_bytes(b'');st=rig.path.stat()
    rig.engine.update(rig.job['id'],size=0,mtime_ns=st.st_mtime_ns)
    run(rig);j=wait_state(rig,{'completed'})
    assert j['confirmed']==0 and j['md5']==hashlib.md5(b'').hexdigest()


def test_pause_waits_for_request_then_resume_uses_server_range(rig):
    r=rig;r.remote.mode='blocked';run(r)
    assert r.remote.entered.wait(5)
    r.engine.action(r.job['id'],'pause')
    assert r.store.get('jobs',r.job['id'])['state']=='pausing'
    r.remote.release.set();j=wait_state(r,{'paused'})
    assert j['confirmed']==1048576
    time.sleep(.4);r.engine.action(j['id'],'resume');wait_state(r,{'completed'})
    assert r.remote.init_count==1
    assert r.remote.offsets==[0,1048576,2097152]


def test_cancel_never_deletes_source_or_sends_more_chunks(rig):
    r=rig;r.remote.mode='blocked';run(r);assert r.remote.entered.wait(5)
    r.engine.action(r.job['id'],'cancel');r.remote.release.set();wait_state(r,{'cancelled'})
    assert r.path.read_bytes()==r.data
    assert r.remote.offsets==[0]
    with pytest.raises(AppError):r.engine.action(r.job['id'],'resume')


def test_final_commit_wins_cancel_race_without_cloud_delete(rig):
    r=rig;r.path.write_bytes(b'final');st=r.path.stat()
    r.engine.update(r.job['id'],size=st.st_size,mtime_ns=st.st_mtime_ns)
    r.remote.mode='blocked';run(r);assert r.remote.entered.wait(5)
    r.engine.action(r.job['id'],'cancel');r.remote.release.set()
    wait_state(r,{'completed'})
    assert r.path.read_bytes()==b'final'


def test_md5_mismatch_cannot_mark_complete(rig):
    rig.remote.md5_bad=True;run(rig);j=wait_state(rig,{'failed'})
    assert 'MD5' in j['error'];assert rig.path.is_file()


def test_restart_preserves_uri_and_server_progress(rig):
    r=rig;r.remote.mode='blocked';run(r);assert r.remote.entered.wait(5)
    r.engine.action(r.job['id'],'pause');r.remote.release.set();wait_state(r,{'paused'});r.engine.stop()
    r.engine=Engine(r.store,r.vault,r.auth,r.files,r.remote.factory,.001)
    run(r);wait_state(r,{'completed'})
    assert r.remote.init_count==1 and r.remote.offsets==[0,1048576,2097152]


def test_expired_session_requires_explicit_restart(rig):
    r=rig;r.vault.put('session-'+r.job['id'],{'uri':'https://www.googleapis.com/upload/test'})
    r.engine.update(r.job['id'],remote_id='remote-id');r.remote.expired=True
    run(r);wait_state(r,{'expired'});assert not r.remote.offsets
    with pytest.raises(AppError):r.engine.action(r.job['id'],'resume')


def test_expired_session_can_restart_without_allocating_another_id(rig):
    r=rig;r.vault.put('session-'+r.job['id'],{'uri':'https://www.googleapis.com/upload/test'})
    r.engine.update(r.job['id'],remote_id='remote-id');r.remote.expired=True
    run(r);wait_state(r,{'expired'});time.sleep(.4)
    r.remote.expired=False;r.engine.action(r.job['id'],'restart');wait_state(r,{'completed'})
    assert r.remote.reserve_count==0 and r.remote.init_count==1


def test_retry_wait_is_interruptible_by_cancel(rig):
    r=rig;r.remote.mode='always_network';r.engine.retry_scale=1
    run(r);wait_state(r,{'waiting_retry'});r.engine.action(r.job['id'],'cancel')
    wait_state(r,{'cancelled'},timeout=1)
    assert r.path.read_bytes()==r.data


def test_startup_marks_crashed_upload_paused_without_auto_resume(rig):
    rig.store.set_setting('auto_resume',False)
    rig.engine.update(rig.job['id'],state='uploading',confirmed=262144)
    rig.engine.start();j=wait_state(rig,{'paused'})
    assert j['confirmed']==262144 and not rig.remote.offsets


def test_crash_recovery_uses_google_offset_ahead_of_saved_progress(rig):
    r=rig
    r.remote.data=r.data[:131072]
    r.remote.job=r.job
    r.engine.update(r.job['id'],state='uploading',confirmed=65536,remote_id='remote-id')
    r.vault.put('session-'+r.job['id'],{'uri':'https://www.googleapis.com/upload/test'})
    r.engine.start()
    done=wait_state(r,{'completed'})
    assert r.remote.offsets[0]==131072 and r.remote.init_count==0
    assert r.remote.data==r.data and done['confirmed']==len(r.data)
    assert done['resume_on_start'] is False


def test_normal_backend_stop_then_start_resumes_without_manual_action(rig):
    r=rig;r.remote.mode='blocked';run(r);assert r.remote.entered.wait(5)
    stopping=threading.Thread(target=r.engine.stop);stopping.start()
    assert r.engine.shutdown_event.wait(2)
    r.remote.release.set();stopping.join(5);assert not stopping.is_alive()
    saved=r.store.get('jobs',r.job['id'])
    assert saved['state']=='paused' and saved['resume_on_start'] is True
    r.engine=Engine(r.store,r.vault,r.auth,r.files,r.remote.factory,.001)
    r.engine.start();wait_state(r,{'completed'})
    assert r.remote.init_count==1 and r.remote.offsets==[0,1048576,2097152]


@pytest.mark.parametrize('state',['staged','paused','cancelled','completed','failed','waiting_auth','expired'])
def test_start_does_not_resume_tasks_without_running_intent(rig,state):
    r=rig;r.engine.update(r.job['id'],state=state,resume_on_start=False)
    r.engine.start()
    assert r.store.get('jobs',r.job['id'])['state']==state and not r.remote.offsets


@pytest.mark.parametrize('state,expected',[('pausing','paused'),('cancelling','cancelled')])
def test_restart_finishes_pending_user_pause_or_cancel_without_upload(rig,state,expected):
    r=rig;r.engine.update(r.job['id'],state=state,resume_on_start=True)
    r.engine.start();saved=r.store.get('jobs',r.job['id'])
    assert saved['state']==expected and saved['resume_on_start'] is False
    assert not r.remote.offsets


def test_automatic_recovery_refuses_modified_source(rig):
    r=rig;r.engine.update(r.job['id'],state='uploading',resume_on_start=True)
    r.path.write_bytes(b'changed since previous startup')
    r.engine.start();saved=wait_state(r,{'failed'})
    assert '源文件' in saved['error'] and not r.remote.offsets


def test_automatic_recovery_waits_when_account_changes(rig):
    r=rig;r.engine.update(r.job['id'],state='uploading',resume_on_start=True)
    r.auth.identity=lambda:{'email':'different@example.com','client_id':'test-client'}
    r.engine.start();saved=wait_state(r,{'waiting_auth'})
    assert not saved['resume_on_start'] and not r.remote.offsets


def test_automatic_recovery_does_not_restart_expired_session(rig):
    r=rig;r.engine.update(r.job['id'],state='uploading',remote_id='remote-id',resume_on_start=True)
    r.vault.put('session-'+r.job['id'],{'uri':'https://www.googleapis.com/upload/test'})
    r.remote.expired=True;r.engine.start();saved=wait_state(r,{'expired'})
    assert not saved['resume_on_start'] and not r.remote.offsets and r.remote.init_count==0


def test_auto_resume_setting_is_persistent_and_has_csrf_protection(tmp_path):
    data=tmp_path/'data'
    for initial in [True,False]:
        app=create_app(data,auth_factory=FakeAuth,lock_instance=False)
        with TestClient(app,base_url='http://127.0.0.1:8787') as client:
            boot=client.get('/api/bootstrap').json()
            assert boot['auto_resume'] is initial
            assert app.state.store.db.execute('PRAGMA synchronous').fetchone()[0]==2
            if initial:
                body={'concurrency':1,'auto_resume':False}
                assert client.put('/api/settings/tuning',json=body).status_code==403
                changed=client.put('/api/settings/tuning',json=body,headers={'X-CSRF-Token':boot['csrf']})
                assert changed.json()['auto_resume'] is False


def test_repeated_chunk_failures_are_bounded(rig):
    rig.remote.mode='always_network';run(rig);wait_state(rig,{'failed'})
    assert len(rig.remote.offsets)==12


def test_changed_source_blocks_resume_and_preserves_file(rig):
    rig.path.write_bytes(b'changed')
    with pytest.raises(AppError,match='变化'):rig.engine.action(rig.job['id'],'resume')
    assert rig.path.read_bytes()==b'changed'


def test_duplicate_job_returns_same_id(rig):
    old=rig.job
    new=rig.engine.create(old['file_id'],{'id':'folder1','name':'same'},8)
    assert old['id']==new['id']


def test_same_name_different_content_not_overwritten(rig):
    rig.remote.existing={'id':'other','size':'1','md5Checksum':'other'}
    run(rig);wait_state(rig,{'failed'});assert not rig.remote.offsets;assert not rig.remote.init_count


def test_path_outside_source_and_credentials_are_blocked(rig,tmp_path):
    other=tmp_path/'private.json';other.write_text('{}')
    with pytest.raises(AppError):rig.files.resolve(rig.root['id'],path=str(other))
    rig.auth.config_path=str(rig.path)
    with pytest.raises(AppError,match='凭据'):rig.files.resolve(rig.root['id'],path=str(rig.path))
    with pytest.raises(AppError):rig.files.resolve(rig.root['id'],[{'name':'../private.json','size':2}])


def test_instance_lock_blocks_second_server(tmp_path):
    first=InstanceLock(tmp_path/'lock');second=InstanceLock(tmp_path/'lock');first.acquire()
    try:
        with pytest.raises(AppError):second.acquire()
    finally:first.release()


def test_token_cache_has_no_client_secret_or_original_config(tmp_path):
    store=Store(tmp_path/'db');vault=Vault(tmp_path/'secret');auth=AuthManager(store,vault,'http://127.0.0.1:8787')
    config={'installed':{'client_id':'test','client_secret':'do-not-cache'}}
    creds=SimpleNamespace(token='access',refresh_token='refresh',expiry=None)
    auth.save(config,creds,'test@example.com')
    result=vault.get(auth.key(config))
    assert 'client_secret' not in result and 'installed' not in result
    assert 'do-not-cache' not in json.dumps(result)


def test_oauth_state_mismatch_rejected(tmp_path):
    auth=AuthManager(Store(tmp_path/'db'),Vault(tmp_path/'vault'),'http://127.0.0.1:8787')
    auth.pending={'state':'valid','expires':time.time()+10}
    with pytest.raises(AppError):auth.callback('evil','code')


def test_desktop_config_not_copied_and_authorization_uses_pkce(tmp_path):
    from urllib.parse import urlparse,parse_qs
    config={'installed':{'client_id':'test.apps.googleusercontent.com','client_secret':'never-persist-me',
        'auth_uri':'https://accounts.google.com/o/oauth2/auth','token_uri':'https://oauth2.googleapis.com/token'}}
    original=tmp_path/'original.json';original.write_text(json.dumps(config))
    data=tmp_path/'data';store=Store(data/'db');vault=Vault(data/'secrets')
    auth=AuthManager(store,vault,'http://127.0.0.1:8787')
    auth.configure(str(original),remember=False)
    assert store.setting('credential_path')==''
    auth_url=auth.start()['authorization_url'];query=parse_qs(urlparse(auth_url).query)
    assert query['code_challenge_method']==['S256'] and query['code_challenge']
    assert query['redirect_uri']==['http://127.0.0.1:8787/api/auth/callback']
    assert query['access_type']==['offline']
    for p in data.rglob('*'):
        if p.is_file():assert b'never-persist-me' not in p.read_bytes()


def test_range_and_folder_url_parsing():
    assert committed(Response(308,{'Range':'bytes=0-42'}),100)==43
    with pytest.raises(AppError):committed(Response(308,{'Range':'bytes=0-100'}),100)
    assert folder_id('https://drive.google.com/drive/folders/abcdef?usp=sharing')=='abcdef'
    with pytest.raises(AppError):folder_id('https://evil.example/drive/folders/abcdef')


def test_api_csrf_websocket_and_persistence(tmp_path):
    app=create_app(tmp_path/'appdata',auth_factory=FakeAuth,lock_instance=False)
    origin='http://127.0.0.1:8787'
    with TestClient(app,base_url=origin) as client:
        assert client.get('/').status_code==200
        assert client.get('/api/bootstrap',headers={'Origin':'https://evil.example'}).status_code==403
        b=client.get('/api/bootstrap').json();csrf=b['csrf']
        assert client.post('/api/sources',json={'path':str(tmp_path)}).status_code==403
        headers={'X-CSRF-Token':csrf,'Origin':origin}
        source=client.post('/api/sources',json={'path':str(tmp_path)},headers=headers)
        assert source.status_code==200
        with client.websocket_connect('/ws/jobs',headers={'Origin':origin,'Cookie':'drive_session='+csrf}) as ws:
            d=ws.receive_json();assert 'jobs' in d and 'events' in d and 'auth' in d
        assert client.get('/api/jobs').status_code==200
        assert client.get('/api/bootstrap',headers={'Host':'evil.example'}).status_code==403
        assert 'client_secret' not in json.dumps(client.get('/api/bootstrap').json())


def test_api_websocket_rejects_foreign_origin(tmp_path):
    app=create_app(tmp_path/'data',auth_factory=FakeAuth,lock_instance=False)
    with TestClient(app,base_url='http://127.0.0.1:8787') as client:
        client.get('/api/bootstrap')
        from starlette.websockets import WebSocketDisconnect
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect('/ws/jobs',headers={'Origin':'https://evil.example'}):pass


def test_stop_service_requires_csrf_and_requests_graceful_shutdown(tmp_path):
    app=create_app(tmp_path/'data',auth_factory=FakeAuth,lock_instance=False)
    called=[];app.state.shutdown_callback=lambda:called.append(True)
    with TestClient(app,base_url='http://127.0.0.1:8787') as client:
        csrf=client.get('/api/bootstrap').json()['csrf']
        assert client.post('/api/service/stop',json={}).status_code==403
        assert not called
        assert client.post('/api/service/stop',json={},headers={'X-CSRF-Token':csrf}).status_code==202
        assert called==[True]


def test_hide_restore_preserves_cancelled_progress_sessions_and_source(rig):
    r=rig;identity=r.job['id']
    r.engine.update(identity,confirmed=262144)
    r.vault.put('session-'+identity,{'uri':'https://www.googleapis.com/upload/test'})
    r.engine.action(identity,'cancel')
    hidden=r.engine.action(identity,'archive')
    assert hidden['archived'] is True and hidden['state']=='cancelled'
    reopened=Store(r.store.db.execute('PRAGMA database_list').fetchone()[2])
    assert reopened.get('jobs',identity)['archived'] is True
    restored=r.engine.action(identity,'restore')
    assert restored['archived'] is False and restored['state']=='cancelled'
    assert restored['confirmed']==262144
    assert r.vault.get('session-'+identity)['uri'].endswith('/test')
    assert r.path.read_bytes()==r.data and not r.remote.offsets


@pytest.mark.parametrize('state',['staged','paused','queued','uploading','failed'])
def test_unfinished_tasks_cannot_be_hidden(rig,state):
    rig.engine.update(rig.job['id'],state=state)
    with pytest.raises(AppError,match='只能隐藏'):
        rig.engine.action(rig.job['id'],'archive')
    assert rig.store.get('jobs',rig.job['id'])['state']==state


def test_bulk_hide_only_finished_and_new_upload_does_not_reuse_hidden_completed(rig):
    r=rig;r.engine.update(r.job['id'],state='completed')
    for identity,state in [('queued-test','queued'),('cancelled-test','cancelled'),('staged-test','staged')]:
        r.store.put('jobs',{**r.job,'id':identity,'state':state})
    assert r.engine.archive_finished()=={'count':2}
    assert r.engine.archive_finished()=={'count':0}
    assert not r.store.get('jobs','queued-test').get('archived')
    assert not r.store.get('jobs','staged-test').get('archived')
    # Remove the nonterminal duplicates from this fixture's identity search.
    for identity in ['queued-test','staged-test']:
        r.engine.update(identity,file_id='other-file')
    new=r.engine.create(r.job['file_id'],{'id':'folder1','name':'目标目录'},8)
    assert new['id']!=r.job['id'] and new['state']=='staged' and not new.get('archived')


def test_history_api_requires_csrf_and_restore_does_not_resume(tmp_path):
    app=create_app(tmp_path/'data',auth_factory=FakeAuth,lock_instance=False)
    with TestClient(app,base_url='http://127.0.0.1:8787') as client:
        csrf=client.get('/api/bootstrap').json()['csrf'];headers={'X-CSRF-Token':csrf}
        app.state.store.put('jobs',{'id':'old-job','state':'cancelled','size':10,'confirmed':0})
        assert client.post('/api/history/archive',json={}).status_code==403
        assert client.post('/api/history/archive',json={},headers=headers).json()=={'count':1}
        assert client.get('/api/jobs').json()['jobs'][0]['archived'] is True
        restored=client.post('/api/jobs/old-job/restore',json={},headers=headers)
        assert restored.status_code==202
        assert restored.json()['state']=='cancelled' and restored.json()['archived'] is False
