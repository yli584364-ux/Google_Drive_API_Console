import hashlib
from pathlib import Path
import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app.drive import Drive
from app.engine import Engine, Files
from app.main import create_app
from app.storage import AppError, Store, Vault
from test_system import FakeAuth, Remote


class FileRemote(Remote):
    def metadata(self, identity):
        result = super().metadata(identity)
        if result: result['parents'] = [self.job['folder_id']]
        return result


class TreeRemote:
    def __init__(self):
        self.dirs = {}; self.sessions = {}; self.files = {}; self.calls = []
        self.mode = None; self.blocked = None; self.expired = False
    def factory(self, auth): return self
    def reserve(self): return uuid.uuid4().hex
    def folder(self, identity): return {'id': identity, 'name': self.dirs.get(identity, {}).get('name', '目标')}
    def ensure_folder(self, name, parent, reserved):
        self.calls.append(('folder', name, parent))
        matches = [d for d in self.dirs.values() if d['name']==name and d['parent']==parent]
        if matches: return self.folder(matches[0]['id'])
        self.dirs[reserved] = {'id': reserved, 'name': name, 'parent': parent}
        return self.folder(reserved)
    def find_files(self, name, parent):
        return [self.metadata(k) for k, r in self.files.items() if r.job['name']==name and r.job['folder_id']==parent and r.final]
    def initiate(self, job):
        remote = FileRemote(); remote.mode = self.mode; remote.initiate(job)
        self.files[job['remote_id']] = remote
        uri = 'https://www.googleapis.com/upload/'+job['remote_id']
        self.sessions[uri] = remote
        if self.mode=='blocked' and self.blocked is None: self.blocked = remote
        return uri
    def metadata(self, identity):
        remote = self.files.get(identity)
        return remote.metadata(identity) if remote else None
    def probe(self, uri, size):
        self.sessions[uri].expired = self.expired
        return self.sessions[uri].probe(uri, size)
    def chunk(self, uri, data, start, size): return self.sessions[uri].chunk(uri, data, start, size)
    def expect(self, response, statuses=(200,)): return Drive.expect(response, statuses)


def wait(store, identity, states, timeout=10):
    deadline = time.time()+timeout
    while time.time()<deadline:
        job = store.get('jobs', identity)
        if job['state'] in states: return job
        time.sleep(.02)
    raise AssertionError(store.get('jobs', identity))


@pytest.fixture
def tree(tmp_path):
    source = tmp_path/'sources'; source.mkdir()
    root = source/'样例数据集'; root.mkdir()
    (root/'甲').mkdir(); (root/'乙').mkdir(); (root/'空文件夹').mkdir()
    (root/'甲'/'same.txt').write_bytes(b'first')
    (root/'乙'/'same.txt').write_bytes(b'second')
    (root/'empty.bin').write_bytes(b'')
    store = Store(tmp_path/'state.db'); vault = Vault(tmp_path/'secrets')
    auth = FakeAuth(store, vault, '')
    files = Files(store, auth, tmp_path/'application'); registered = files.register(str(source))
    remote = TreeRemote(); engine = Engine(store, vault, auth, files, remote.factory, retry_scale=.001)
    engine.start()
    ctx = dict(source=source, root=root, store=store, vault=vault, auth=auth, files=files,
        registered=registered, remote=remote, engine=engine)
    yield ctx
    for remote_file in remote.files.values(): remote_file.release.set()
    ctx['engine'].stop()


def add(tree):
    job = tree['engine'].create_folder(tree['registered']['id'], {'id': 'target1', 'name': 'Datasets'}, 1, name=tree['root'].name)
    return wait(tree['store'], job['id'], {'staged', 'failed'})


def test_scan_recursive_manifest_is_staged_and_does_not_touch_cloud(tree):
    job = add(tree)
    assert job['state']=='staged' and job['file_count']==3 and job['directory_count']==4
    assert job['size']==11 and not tree['remote'].calls
    assert len(tree['store'].jobs())==1
    items = tree['store'].items(job['id'])
    assert {i['relative'] for i in items}=={'甲/same.txt','乙/same.txt','empty.bin'}
    assert tree['store'].directory(job['id'], '空文件夹')['remote_id'] is None


def test_start_immediately_after_scan_is_queued_until_scanner_exits(tree):
    published = threading.Event(); release = threading.Event()
    original = tree['store'].event
    def hold_scan_exit(message, job_id=None):
        if message.startswith('扫描完成：'):
            published.set(); assert release.wait(5)
        return original(message, job_id)
    tree['store'].event = hold_scan_exit
    try:
        job = tree['engine'].create_folder(tree['registered']['id'], {'id':'target1','name':'Datasets'}, 1, name=tree['root'].name)
        assert published.wait(5)
        assert not tree['engine'].running[job['id']].done()
        assert tree['engine'].action(job['id'], 'resume')['state']=='queued'
        assert not tree['remote'].calls
        release.set()
        done = wait(tree['store'], job['id'], {'completed','failed'})
        assert done['state']=='completed', done
    finally:
        release.set(); tree['store'].event = original


def test_upload_preserves_root_nested_names_empty_files_and_empty_directories(tree):
    job = add(tree); tree['engine'].action(job['id'], 'resume')
    done = wait(tree['store'], job['id'], {'completed', 'failed'})
    assert done['state']=='completed', done
    assert done['completed_files']==3 and done['confirmed']==11
    assert {d['name'] for d in tree['remote'].dirs.values()}=={'样例数据集','甲','乙','空文件夹'}
    assert all(r.data==Path(r.job['path']).read_bytes() for r in tree['remote'].files.values())
    assert len({r.job['folder_id'] for r in tree['remote'].files.values() if r.job['name']=='same.txt'})==2
    assert len(list(tree['root'].rglob('*')))==6


def test_folder_network_failure_uses_confirmed_offset(tree):
    large = tree['root']/'large.bin'; large.write_bytes(b'0123456789abcdef'*170000)
    tree['remote'].mode = 'network'
    job = add(tree); tree['engine'].action(job['id'], 'resume')
    done = wait(tree['store'], job['id'], {'completed', 'failed'})
    assert done['state']=='completed', done
    remote = next(r for r in tree['remote'].files.values() if r.job['name']=='large.bin')
    assert remote.offsets[:2]==[0,65536] and remote.data==large.read_bytes()
    assert done['confirmed']==done['size'] and done['retry_count']>0


def block_folder(tree):
    # Remove small files so that pause interrupts the first multi-chunk file.
    for p in tree['root'].rglob('*'):
        if p.is_file(): p.unlink()
    (tree['root']/'large.bin').write_bytes(b'x'*(3*1024*1024))
    tree['remote'].mode = 'blocked'; job = add(tree)
    tree['engine'].action(job['id'], 'resume')
    end = time.time()+5
    while tree['remote'].blocked is None and time.time()<end: time.sleep(.01)
    remote = tree['remote'].blocked
    assert remote and remote.entered.wait(5)
    return job, remote


def test_folder_pause_restart_service_resume_keeps_session_and_directories(tree):
    job, remote = block_folder(tree)
    tree['engine'].action(job['id'], 'pause'); remote.release.set()
    paused = wait(tree['store'], job['id'], {'paused'})
    assert paused['confirmed']==1024*1024
    tree['engine'].stop()
    engine = Engine(tree['store'], tree['vault'], tree['auth'], tree['files'], tree['remote'].factory, .001)
    tree['engine'] = engine; engine.start(); engine.action(job['id'], 'resume')
    done = wait(tree['store'], job['id'], {'completed', 'failed'})
    assert done['state']=='completed', done
    assert len(tree['remote'].files)==1 and remote.offsets==[0,1048576,2097152]
    assert len(tree['remote'].dirs)==4


def test_folder_backend_stop_then_start_automatically_restores_current_child(tree):
    job, remote = block_folder(tree)
    stopping = threading.Thread(target=tree['engine'].stop); stopping.start()
    assert tree['engine'].shutdown_event.wait(2)
    remote.release.set(); stopping.join(5); assert not stopping.is_alive()
    saved = tree['store'].get('jobs', job['id'])
    assert saved['state']=='paused' and saved['resume_on_start'] is True
    engine = Engine(tree['store'], tree['vault'], tree['auth'], tree['files'], tree['remote'].factory, .001)
    tree['engine'] = engine; engine.start()
    done = wait(tree['store'], job['id'], {'completed','failed'})
    assert done['state']=='completed', done
    assert remote.offsets==[0,1048576,2097152] and len(tree['remote'].files)==1
    assert len(tree['remote'].dirs)==4 and done['confirmed']==done['size']


def test_folder_cancel_stops_remaining_files_without_deleting(tree):
    job, remote = block_folder(tree)
    tree['engine'].action(job['id'], 'cancel'); remote.release.set()
    wait(tree['store'], job['id'], {'cancelled'})
    assert remote.offsets==[0]
    assert (tree['root']/'large.bin').stat().st_size==3*1024*1024
    assert len(tree['remote'].dirs)==4


@pytest.mark.parametrize('action,expected', [('pause','paused'), ('cancel','cancelled')])
def test_scanning_can_be_paused_or_cancelled_without_cloud_writes(tree, action, expected):
    entered = threading.Event(); release = threading.Event(); original = tree['files'].check
    def blocked_check(*args, **kwargs):
        entered.set(); assert release.wait(5)
        return original(*args, **kwargs)
    tree['files'].check = blocked_check
    job = tree['engine'].create_folder(tree['registered']['id'], {'id':'target1','name':'Datasets'}, 1, name=tree['root'].name)
    try:
        assert entered.wait(5)
        tree['engine'].action(job['id'], action); release.set()
        stopped = wait(tree['store'], job['id'], {expected})
        assert not stopped['scanned'] and not tree['remote'].calls
    finally:
        release.set(); tree['files'].check = original


def test_folder_expired_child_requires_explicit_restart(tree):
    job, remote = block_folder(tree)
    tree['engine'].action(job['id'], 'pause'); remote.release.set()
    wait(tree['store'], job['id'], {'paused'}); tree['engine'].stop()
    def fresh_engine():
        engine = Engine(tree['store'], tree['vault'], tree['auth'], tree['files'], tree['remote'].factory, .001)
        tree['engine'] = engine; engine.start(); return engine
    tree['remote'].expired = True
    engine = fresh_engine(); engine.action(job['id'], 'resume')
    wait(tree['store'], job['id'], {'expired'})
    with pytest.raises(AppError): engine.action(job['id'], 'resume')
    engine.stop(); tree['remote'].expired = False; tree['remote'].mode = None
    engine = fresh_engine(); engine.action(job['id'], 'restart')
    done = wait(tree['store'], job['id'], {'completed','failed'})
    assert done['state']=='completed', done
    assert done['confirmed']==done['size'] and len(tree['remote'].files)==1


def test_changed_file_stops_folder_with_relative_path(tree):
    job = add(tree); (tree['root']/'empty.bin').write_bytes(b'changed')
    tree['engine'].action(job['id'], 'resume')
    failed = wait(tree['store'], job['id'], {'failed'})
    assert 'empty.bin' in failed['error'] and '变化' in failed['error']
    assert (tree['root']/'empty.bin').read_bytes()==b'changed'


def test_scan_more_than_browser_entry_batch_and_no_200_file_limit(tree):
    for i in range(205): (tree['root']/f'{i}.txt').write_bytes(b'x')
    job = add(tree)
    assert job['state']=='staged' and job['file_count']==208
    assert len(tree['store'].items(job['id'], limit=100, offset=200))==8


def test_empty_root_folder_uploads_as_folder(tree):
    empty = tree['source']/'empty'; empty.mkdir()
    job = tree['engine'].create_folder(tree['registered']['id'], {'id':'target1','name':'Datasets'}, 1, name='empty')
    wait(tree['store'], job['id'], {'staged'}); tree['engine'].action(job['id'], 'resume')
    done = wait(tree['store'], job['id'], {'completed', 'failed'})
    assert done['state']=='completed' and done['file_count']==0 and done['directory_count']==1
    assert len(tree['remote'].dirs)==1 and not tree['remote'].files


def test_folder_path_matching_and_private_config_guard(tree):
    files = tree['files']; registered = files.register(str(tree['root']))
    assert files.folder(registered['id'], name=tree['root'].name)['path']==str(tree['root'])
    with pytest.raises(AppError): files.folder(registered['id'], path=str(tree['source']))
    with pytest.raises(AppError): files.folder(registered['id'], name='../outside')
    with pytest.raises(AppError): files.folder(registered['id'], name=tree['root'].name, samples=[{'relative':'../secret','size':0}])
    tree['auth'].config_path = str(tree['root']/'empty.bin')
    tree['registered'] = registered; job = add(tree)
    assert job['state']=='failed' and '凭据' in job['error']
    assert not tree['remote'].calls


def test_api_folder_jobs_staged_scan_and_manifest_pagination(tmp_path):
    source = tmp_path/'sources'; source.mkdir(); (source/'folder').mkdir()
    (source/'folder'/'hello.txt').write_text('hello')
    remote = TreeRemote()
    app = create_app(tmp_path/'data', auth_factory=FakeAuth, drive_factory=remote.factory, lock_instance=False)
    with TestClient(app, base_url='http://127.0.0.1:8787') as client:
        csrf = client.get('/api/bootstrap').json()['csrf']; headers = {'X-CSRF-Token':csrf}
        registered = client.post('/api/sources', json={'path':str(source)}, headers=headers).json()
        payload = {'source_id':registered['id'], 'folder_id':'target1', 'name':'folder'}
        assert client.post('/api/folder-jobs', json=payload).status_code==403
        response = client.post('/api/folder-jobs', json=payload, headers=headers)
        assert response.status_code==200, response.text
        job = wait(app.state.store, response.json()['id'], {'staged'})
        listing = client.get(f"/api/jobs/{job['id']}/items?limit=1").json()
        assert listing['total']==1 and listing['items'][0]['relative']=='hello.txt'
        assert client.get(f"/api/jobs/{job['id']}/items?limit=999").status_code==400
        assert not remote.calls


def test_drive_folder_id_recovers_lost_create_response_and_rejects_duplicates():
    drive = Drive(None); created = {}; posted = []
    def metadata(identity): return created.get(identity)
    drive.metadata = metadata; drive.find_files = lambda *a:[]
    drive.folder = lambda identity: {'id':identity,'name':'folder'}
    def request(method, url, **kwargs):
        posted.append(kwargs['json']); item = dict(kwargs['json'], capabilities={'canAddChildren':True})
        created[item['id']] = item
        raise AppError('lost POST response', 'transient', 503)
    drive.request = request
    with pytest.raises(AppError): drive.ensure_folder('folder','parent1','reserved1')
    assert drive.ensure_folder('folder','parent1','reserved1')['id']=='reserved1'
    assert len(posted)==1
    drive.find_files = lambda *a:[{'id':'a'}, {'id':'b'}]
    with pytest.raises(AppError, match='多个同名'): drive.ensure_folder('folder','parent1','reserved2')
