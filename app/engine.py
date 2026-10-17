from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import random
import threading
import time
import uuid

from .drive import Drive, committed
from .storage import AppError

ACTIVE = {'scan_queued', 'scanning', 'queued', 'preparing', 'uploading', 'waiting_retry', 'pausing', 'cancelling', 'verifying'}
TERMINAL = {'completed', 'cancelled'}


def canonical(value):
    p = Path(str(value).strip().strip('"')).expanduser()
    if not p.is_absolute(): raise AppError('请输入本机绝对路径。')
    # Resolve junctions/symlinks, but refuse traversing them when selecting sources.
    for part in [p, *p.parents]:
        if part.exists() and getattr(part.stat(follow_symlinks=False), 'st_file_attributes', 0) & 0x400:
            raise AppError('不支持符号链接或目录联接，请填写实际路径。')
    return p.resolve()


class Files:
    def __init__(self, store, auth, app_root):
        self.store, self.auth, self.app_root = store, auth, Path(app_root).resolve()
    def register(self, value):
        p = canonical(value)
        if not p.is_dir(): raise AppError('本地源目录不存在。')
        if p == Path(p.anchor): raise AppError('请选择具体源文件夹，不要注册整个盘符。')
        source = self.store.add_source(str(p))
        self.store.set_setting('last_source', source)
        return source
    def check(self, p, source):
        p = canonical(p); root = canonical(source['path'])
        if not p.is_relative_to(root) or not p.is_file():
            raise AppError('文件不在注册的源目录内，或文件不存在。')
        if p.is_relative_to(self.app_root): raise AppError('不能上传工作台自己的代码、授权缓存或数据库。')
        if self.auth.config_path and p == Path(self.auth.config_path).resolve():
            raise AppError('不能将当前 OAuth 凭据文件加入上传队列。')
        stat = p.stat()
        with p.open('rb'): pass
        return {'path': str(p), 'name': p.name, 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns, 'source_root': str(root)}
    def resolve(self, root_id, descriptors=None, path=None):
        source = next((x for x in self.store.sources() if x['id'] == root_id), None)
        if not source: raise AppError('请先注册本地源目录。')
        root = canonical(source['path'])
        candidates = []
        if path:
            candidates.append(self.check(path, source))
        else:
            for f in descriptors or []:
                name = f['name']
                if Path(name).name != name or '/' in name or '\\' in name or name in ('.', '..'):
                    raise AppError('拖入文件的名称无效。')
                p = root/name
                info = self.check(p, source)
                if info['size'] != f['size']:
                    raise AppError('拖入文件与源目录同名文件的大小不同，请核对来源。')
                candidates.append(info)
        result = []
        for info in candidates:
            info['id'] = hashlib.sha256((info['path']+'|'+str(info['size'])+'|'+str(info['mtime_ns'])).encode()).hexdigest()
            self.store.put('files', info); result.append(info)
        return result
    def assert_job(self, job):
        if job.get('kind') == 'folder':
            p = canonical(job['path'])
            if not p.is_dir() or not p.is_relative_to(canonical(job['source_root'])):
                raise AppError('源文件夹已不存在或不在注册目录内。', 'source_changed', 409)
            return
        info = self.check(job['path'], {'path': job['source_root']})
        if (info['size'], info['mtime_ns']) != (job['size'], job['mtime_ns']):
            raise AppError('源文件发生变化，已停止旧会话；请重新加入此文件。', 'source_changed', 409)
    def folder(self, source_id, name=None, path=None, samples=None):
        source = next((s for s in self.store.sources() if s['id'] == source_id), None)
        if not source: raise AppError('请先确认本地源目录。')
        root = canonical(source['path'])
        if path:
            p = canonical(path)
        else:
            if not name or Path(name).name != name or '/' in name or '\\' in name or name in ('.', '..'):
                raise AppError('文件夹名称无效。')
            candidates = [p for p in (root/name, root) if p.is_dir() and (p != root or root.name.lower() == name.lower())]
            if len(candidates) != 1:
                raise AppError('无法唯一匹配拖入的文件夹。请将源目录填写为该文件夹，或使用完整文件夹路径。')
            p = canonical(candidates[0])
        if not p.is_relative_to(root) or not p.is_dir() or p == Path(p.anchor):
            raise AppError('文件夹必须位于注册的源目录内。')
        if p.is_relative_to(self.app_root): raise AppError('不能上传工作台自己的目录。')
        for sample in samples or []:
            relative = Path(sample['relative'])
            if relative.is_absolute() or '..' in relative.parts:
                raise AppError('文件夹中的相对路径无效。')
            info = self.check(p/relative, source)
            if info['size'] != sample['size']:
                raise AppError('拖入文件夹与本地源目录不一致，请核对队列中的实际路径。')
        return {'path': str(p), 'name': p.name, 'source_root': str(root)}


class StopJob(Exception):
    pass


class Engine:
    def __init__(self, store, vault, auth, files, drive_factory=Drive, retry_scale=1):
        self.store, self.vault, self.auth, self.files = store, vault, auth, files
        self.drive_factory, self.retry_scale = drive_factory, retry_scale
        self.lock = threading.RLock()
        self.wake = threading.Event(); self.shutdown_event = threading.Event()
        self.controls = {}; self.samples = {}; self.running = {}; self.seq = 0
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='drive-upload')
        self.thread = None
    def busy(self):
        # Do not acquire engine.lock from AuthManager (auth->engine would invert
        # the action path's engine->auth lock order).
        return any(j['state'] in ACTIVE for j in self.store.jobs())
    def start(self):
        for j in self.store.jobs():
            if j['state'] in ACTIVE:
                self.update(j['id'], state='paused', error='后台曾退出；点击继续将查询云端进度。')
        self.thread = threading.Thread(target=self.schedule, name='upload-scheduler', daemon=True)
        self.thread.start()
    def stop(self):
        self.shutdown_event.set(); self.wake.set()
        with self.lock:
            for c in list(self.controls.values()): c.set()
        if self.thread: self.thread.join(timeout=3)
        self.pool.shutdown(wait=True)
    def schedule(self):
        while not self.shutdown_event.is_set():
            with self.lock:
                for identity, future in list(self.running.items()):
                    if future.done():
                        self.running.pop(identity); self.controls.pop(identity, None)
                capacity = int(self.store.setting('concurrency', 1)) - len(self.running)
                for job in reversed(self.store.jobs()):
                    if capacity <= 0 or self.shutdown_event.is_set(): break
                    if job['state'] in ('queued', 'scan_queued') and job['id'] not in self.running:
                        identity = job['id']; self.controls[identity] = threading.Event()
                        self.update(identity, state='scanning' if job['state']=='scan_queued' else 'preparing', error=None)
                        self.samples[identity] = deque()
                        worker = self.folder_worker if job.get('kind')=='folder' else self.worker
                        self.running[identity] = self.pool.submit(worker, identity)
                        capacity -= 1
            self.wake.wait(.3); self.wake.clear()
    def update(self, identity, **changes):
        with self.lock:
            job = self.store.get('jobs', identity)
            # A worker progress response must not overwrite a just-requested control state.
            if job['state'] in ('pausing', 'cancelling') and changes.get('state') in ('uploading', 'waiting_retry', 'preparing'):
                changes.pop('state')
            previous_confirmed = job['confirmed']; previous_state = job['state']
            job.update(changes); job['updated_at'] = time.time()
            self.store.put('jobs', job); self.seq += 1
            if job.get('parent_id'):
                parent = self.store.get('jobs', job['parent_id'])
                summary = dict(confirmed=max(0, parent['confirmed']+job['confirmed']-previous_confirmed),
                    current_relative=job['relative'], current_state=job['state'], hash_bytes=job['hash_bytes'],
                    retry_count=parent['retry_count']+max(0, job['retry_count']-parent.get('current_retry_count', 0)),
                    current_retry_count=job['retry_count'], next_retry_at=job.get('next_retry_at'))
                if job['state']=='completed' and previous_state!='completed':
                    summary['completed_files'] = parent['completed_files']+1
                if job['state'] in ('preparing','uploading','waiting_retry','verifying') and parent['state'] not in ('pausing','cancelling'):
                    summary['state'] = job['state']
                self.update(parent['id'], **summary)
                if 'confirmed' in changes: self.sample(parent['id'], summary['confirmed'])
            return job
    def create(self, file_id, folder, chunk_mib):
        identity = self.auth.identity()
        source = self.store.get('files', file_id)
        self.files.assert_job(source)
        with self.lock:
            for old in self.store.jobs():
                if old.get('file_id') == file_id and old['folder_id'] == folder['id'] and old['account'] == identity['email'] and old['state'] != 'cancelled':
                    return old
            job = {**source, 'id': uuid.uuid4().hex, 'file_id': file_id, 'folder_id': folder['id'],
                'folder_name': folder['name'], 'account': identity['email'], 'client_id': identity['client_id'],
                'state': 'staged', 'confirmed': 0, 'hash_bytes': 0, 'chunk_mib': chunk_mib,
                'retry_count': 0, 'next_retry_at': None, 'remote_id': None, 'remote_url': None,
                'md5': None, 'sha256': None, 'error': None, 'created_at': time.time(), 'updated_at': time.time()}
            self.store.put('jobs', job); self.store.event('文件已加入队列。', job['id']); self.seq += 1
            return job
    def create_folder(self, source_id, folder, chunk_mib, name=None, path=None, samples=None):
        account = self.auth.identity()
        source = self.files.folder(source_id, name, path, samples)
        with self.lock:
            for old in self.store.jobs():
                if old.get('kind')=='folder' and old['path']==source['path'] and old['folder_id']==folder['id'] and old['account']==account['email'] and old['client_id']==account['client_id'] and old['state'] not in TERMINAL:
                    return old
            job = {**source, 'id': uuid.uuid4().hex, 'kind': 'folder', 'folder_id': folder['id'],
                'folder_name': folder['name'], 'account': account['email'], 'client_id': account['client_id'],
                'state': 'scan_queued', 'scanned': False, 'file_count': 0, 'directory_count': 0,
                'completed_files': 0, 'size': 0, 'confirmed': 0, 'hash_bytes': 0, 'chunk_mib': chunk_mib,
                'retry_count': 0, 'next_retry_at': None, 'remote_id': None, 'remote_url': None,
                'error': None, 'created_at': time.time(), 'updated_at': time.time()}
            self.store.put('jobs', job); self.store.event('文件夹已加入队列；正在扫描，扫描完成后点击开始。', job['id'])
            self.wake.set(); return job
    def action(self, identity, action):
        with self.lock:
            j = self.store.get('jobs', identity); state = j['state']
            if action == 'resume':
                if state in ('scan_queued', 'scanning', 'queued', 'preparing', 'uploading', 'waiting_retry', 'verifying'): return j
                # Scanning publishes its staged result before its future exits.
                # Accept start now; the scheduler still waits for that future to
                # finish before starting the upload worker for the same task.
                finishing_scan = state=='staged' and j.get('kind')=='folder' and j.get('scanned')
                if identity in self.running and not finishing_scan:
                    raise AppError('任务正在停止，请稍候再继续。', 'busy', 409)
                if state not in ('staged', 'paused', 'failed', 'waiting_auth'):
                    raise AppError('这个任务不能直接继续。', 'state_conflict', 409)
                current = self.auth.identity()
                if current != {'email': j['account'], 'client_id': j['client_id']}:
                    raise AppError('任务绑定的 Google 账号或客户端与当前授权不同。')
                self.files.assert_job(j)
                j = self.update(identity, state='scan_queued' if j.get('kind')=='folder' and not j['scanned'] else 'queued', error=None, next_retry_at=None)
            elif action == 'pause':
                if state in TERMINAL or state in ('paused', 'expired', 'failed', 'waiting_auth', 'staged'): return j
                j = self.update(identity, state='pausing' if identity in self.running else 'paused', next_retry_at=None)
                if identity in self.controls: self.controls[identity].set()
            elif action == 'cancel':
                if state in TERMINAL: return j
                j = self.update(identity, state='cancelling' if identity in self.running else 'cancelled', next_retry_at=None)
                if identity in self.controls: self.controls[identity].set()
            elif action == 'restart':
                if state != 'expired' or identity in self.running:
                    raise AppError('只有已失效的上传会话可以重新开始。', 'state_conflict', 409)
                if j.get('kind')=='folder':
                    child = self.store.next_item(identity)
                    if child and child['state']=='expired':
                        self.vault.put('session-'+child['id'], {})
                        self.update(child['id'], state='paused', confirmed=0, error=None)
                    self.update(identity, state='paused', error=None)
                else:
                    self.vault.put('session-'+identity, {})
                    self.update(identity, state='paused', confirmed=0, error=None)
                return self.action(identity, 'resume')
            else: raise AppError('操作不存在。')
            self.store.event({'resume':'已请求开始/继续。','pause':'已请求暂停。','cancel':'已请求取消；不会删除源文件。'}[action], identity)
            self.wake.set()
            return j
    def control(self, identity):
        j = self.store.get('jobs', identity)
        parent = self.store.get('jobs', j['parent_id']) if j.get('parent_id') else j
        if self.shutdown_event.is_set() or parent['state'] in ('pausing', 'cancelling', 'paused', 'cancelled') or j['state'] in ('pausing', 'cancelling'):
            cancelled = parent['state'] in ('cancelling','cancelled') or j['state']=='cancelling'
            self.update(identity, state='cancelled' if cancelled else 'paused', next_retry_at=None)
            self.store.event('任务已取消，源文件保留。' if cancelled else '已暂停，续传进度保留。', identity)
            raise StopJob()
    def request(self, identity, operation):
        for attempt in range(12):
            self.control(identity)
            try:
                result = operation()
                return result
            except AppError as exc:
                if exc.code != 'transient': raise
                if attempt == 11: raise AppError('连续重试后仍无法连接，请恢复网络后点击继续。', 'network_failed', 503) from None
                delay = min(60, 2**min(attempt+1, 6)) * self.retry_scale + random.random()*self.retry_scale
                job = self.store.get('jobs', identity)
                self.update(identity, state='waiting_retry', retry_count=job['retry_count']+1, next_retry_at=time.time()+delay)
                self.store.event('网络中断或限流，稍后查询已确认进度。', identity)
                self.controls[identity].wait(delay)
                self.control(identity)
        raise AssertionError('unreachable')
    def acknowledge(self, identity, offset):
        self.sample(identity, offset)
        return self.update(identity, confirmed=offset, state='uploading', next_retry_at=None)
    def sample(self, identity, offset):
        now = time.monotonic()
        with self.lock:
            history = self.samples.setdefault(identity, deque())
            if not history: history.append((now, offset))
            elif offset != history[-1][1]: history.append((now, offset))
            while len(history) > 1 and history[0][0] < now-30: history.popleft()
    def hash_file(self, identity, job):
        if job.get('md5') and job.get('sha256'): return job
        md5, sha = hashlib.md5(), hashlib.sha256(); count = 0; last = time.monotonic()
        with open(job['path'], 'rb') as stream:
            while chunk := stream.read(8*1024*1024):
                self.control(identity); md5.update(chunk); sha.update(chunk); count += len(chunk)
                if time.monotonic()-last >= .7:
                    self.update(identity, hash_bytes=count); last = time.monotonic()
        self.files.assert_job(job)
        return self.update(identity, md5=md5.hexdigest(), sha256=sha.hexdigest(), hash_bytes=count)
    def finish(self, identity, drive, metadata=None):
        job = self.store.get('jobs', identity)
        self.update(identity, state='verifying')
        metadata = metadata or self.request(identity, lambda: drive.metadata(job['remote_id']))
        self.files.assert_job(job)
        if not metadata or metadata.get('name') != job['name'] or int(metadata.get('size', -1)) != job['size']:
            raise AppError('云端文件名或大小校验失败；本地源文件保留。', 'verification_failed')
        if metadata.get('md5Checksum') != job['md5'] or job['folder_id'] not in metadata.get('parents', []) or metadata.get('ownedByMe') is not True:
            raise AppError('云端内容 MD5、目录或所有权校验失败。', 'verification_failed')
        self.update(identity, state='completed', confirmed=job['size'], remote_id=metadata['id'],
            remote_url=metadata.get('webViewLink'), error=None, next_retry_at=None, verified_at=time.time())
        self.vault.put('session-'+identity, {})
        self.store.event('上传完成，云端大小和 MD5 一致；源文件保留。', identity)
    def probe(self, identity, drive, uri):
        job = self.store.get('jobs', identity)
        r = self.request(identity, lambda: drive.probe(uri, job['size']))
        if r.status_code in (200, 201):
            # A final response wins a concurrent cancel: completed data is never deleted.
            self.finish(identity, drive); return False
        if r.status_code in (404, 410):
            self.update(identity, state='expired', error='上传会话已失效。点击“重新上传”从头开始。')
            self.store.event('Google 续传会话已失效，需要明确重新上传。', identity)
            raise StopJob()
        drive.expect(r, (308,))
        self.acknowledge(identity, committed(r, job['size']))
        return True
    def scan_folder(self, identity):
        job = self.store.get('jobs', identity)
        self.files.assert_job(job)
        self.store.reset_manifest(identity)
        self.update(identity, state='scanning', file_count=0, directory_count=0,
            completed_files=0, size=0, confirmed=0, current_relative=None)
        root = canonical(job['path']); stack = [root]; count = size = directories = 0
        last = time.monotonic()
        while stack:
            self.control(identity)
            directory = canonical(stack.pop())
            if not directory.is_relative_to(root) or not directory.is_dir():
                raise AppError('扫描过程中源目录发生变化，请重新扫描。', 'source_changed', 409)
            relative_dir = directory.relative_to(root).as_posix()
            self.store.add_directory(identity, relative_dir); directories += 1
            with os.scandir(directory) as entries:
                for entry in entries:
                    self.control(identity)
                    path = canonical(entry.path)  # Refuse junctions/reparse points, even inside subfolders.
                    if path.is_dir():
                        stack.append(path); continue
                    info = self.files.check(path, {'path': job['source_root']})
                    relative = path.relative_to(root).as_posix()
                    child = {**info, 'id': uuid.uuid4().hex, 'parent_id': identity, 'relative': relative,
                        'file_id': None, 'folder_id': None, 'folder_name': None,
                        'account': job['account'], 'client_id': job['client_id'], 'state': 'staged',
                        'confirmed': 0, 'hash_bytes': 0, 'chunk_mib': job['chunk_mib'], 'retry_count': 0,
                        'next_retry_at': None, 'remote_id': None, 'remote_url': None, 'md5': None,
                        'sha256': None, 'error': None, 'created_at': time.time(), 'updated_at': time.time()}
                    self.store.put('jobs', child); count += 1; size += info['size']
                    if time.monotonic()-last > .5:
                        self.update(identity, file_count=count, directory_count=directories, size=size,
                            current_relative=relative)
                        last = time.monotonic()
        with self.lock:
            self.control(identity)
            self.update(identity, state='staged', scanned=True, file_count=count, directory_count=directories,
                size=size, current_relative=None, current_state=None, error=None)
        self.store.event(f'扫描完成：{count} 个文件，{directories} 个文件夹（含根目录），{size} 字节。点击开始后上传。', identity)
    def folder_worker(self, identity):
        try:
            self.control(identity)
            job = self.store.get('jobs', identity)
            if not job['scanned']:
                self.scan_folder(identity); return
            if self.auth.identity() != {'email': job['account'], 'client_id': job['client_id']}:
                raise AppError('当前账号与任务绑定账号不一致。', 'waiting_auth', 401)
            self.files.assert_job(job)
            drive = self.drive_factory(self.auth)
            self.request(identity, lambda: drive.folder(job['folder_id']))
            while directory := self.store.next_directory(identity):
                self.control(identity)
                relative = directory['relative']
                local = canonical(Path(job['path'])/relative)
                if not local.is_dir(): raise AppError('清单中的目录已不存在：'+relative, 'source_changed', 409)
                if relative == '.':
                    parent_id, name = job['folder_id'], job['name']
                else:
                    parent_relative = Path(relative).parent.as_posix()
                    parent_id = self.store.directory(identity, parent_relative)['remote_id']
                    name = Path(relative).name
                if not parent_id: raise AppError('父目录尚未准备好。')
                reserved = directory['reserved_id']
                if not reserved:
                    reserved = self.request(identity, drive.reserve)
                    self.store.set_directory(identity, relative, reserved_id=reserved)
                self.update(identity, current_relative=relative, current_state='preparing')
                cloud = self.request(identity, lambda: drive.ensure_folder(name, parent_id, reserved))
                self.store.set_directory(identity, relative, remote_id=cloud['id'])
                if relative == '.':
                    self.update(identity, remote_id=cloud['id'], remote_url=cloud.get('url') or 'https://drive.google.com/drive/folders/'+cloud['id'])
            while child := self.store.next_item(identity):
                self.control(identity)
                if child['state']=='expired':
                    self.update(identity, state='expired', error=child['error']); return
                folder = self.store.directory(identity, Path(child['relative']).parent.as_posix())
                self.update(identity, current_retry_count=child['retry_count'])
                self.update(child['id'], folder_id=folder['remote_id'], folder_name=Path(child['relative']).parent.as_posix(), state='preparing', error=None)
                self.controls[child['id']] = self.controls[identity]
                try:
                    self.worker(child['id'])
                finally:
                    self.controls.pop(child['id'], None); self.samples.pop(child['id'], None)
                self.control(identity)
                result = self.store.get('jobs', child['id'])
                if result['state']!='completed':
                    self.update(identity, state=result['state'], error=child['relative']+'：'+(result.get('error') or '任务已停止。'), next_retry_at=None)
                    return
            self.control(identity)
            self.update(identity, state='completed', confirmed=job['size'], current_relative=None,
                current_state=None, error=None, next_retry_at=None, verified_at=time.time())
            self.store.event('文件夹上传完成；全部文件的云端大小与 MD5 校验通过，目录结构与空目录已保留。', identity)
        except StopJob:
            pass
        except Exception as exc:
            current = self.store.get('jobs', identity)
            if self.shutdown_event.is_set() or current['state'] in ('pausing','cancelling'):
                self.update(identity, state='cancelled' if current['state']=='cancelling' else 'paused', error=None, next_retry_at=None)
            else:
                message = str(exc) if isinstance(exc, AppError) else '目录处理异常（'+type(exc).__name__+'），请检查目录访问权限或文件是否变化。'
                self.update(identity, state='waiting_auth' if isinstance(exc, AppError) and exc.code=='waiting_auth' else 'failed', error=message, next_retry_at=None)
                self.store.event(message, identity)
        finally:
            self.wake.set()
    def worker(self, identity):
        try:
            self.control(identity)
            job = self.store.get('jobs', identity)
            if self.auth.identity() != {'email':job['account'], 'client_id':job['client_id']}:
                raise AppError('当前账号与任务绑定账号不一致。', 'waiting_auth', 401)
            self.files.assert_job(job)
            drive = self.drive_factory(self.auth)
            self.request(identity, lambda: drive.folder(job['folder_id']))
            job = self.hash_file(identity, job)
            secret = self.vault.get('session-'+identity); uri = secret.get('uri')
            if not uri:
                if job.get('remote_id'):
                    existing = self.request(identity, lambda: drive.metadata(job['remote_id']))
                    if existing:
                        self.finish(identity, drive, existing); return
                else:
                    existing = self.request(identity, lambda: drive.find_files(job['name'], job['folder_id']))
                    match = next((x for x in existing if int(x.get('size', -1)) == job['size'] and x.get('md5Checksum') == job['md5']), None)
                    if match:
                        self.finish(identity, drive, match); return
                    if existing: raise AppError('目标已有同名但内容不同的文件，请为新任务设置不同文件名。', 'name_conflict', 409)
                    remote = self.request(identity, drive.reserve)
                    job = self.update(identity, remote_id=remote)
                uri = self.request(identity, lambda: drive.initiate(job))
                if not uri:
                    self.finish(identity, drive); return
                self.vault.put('session-'+identity, {'uri': uri})
                self.store.event('可恢复上传会话已建立。', identity)
            if not self.probe(identity, drive, uri): return
            chunk_failures = 0
            with open(job['path'], 'rb') as stream:
                while True:
                    self.control(identity); self.files.assert_job(job)
                    current = self.store.get('jobs', identity); start = current['confirmed']
                    stream.seek(start)
                    data = stream.read(min(job['chunk_mib']*1024*1024, max(0, job['size']-start)))
                    try:
                        r = drive.chunk(uri, data, start, job['size'])
                    except AppError as exc:
                        if exc.code != 'transient': raise
                        chunk_failures += 1
                        if chunk_failures >= 12:
                            raise AppError('连续上传失败，请恢复网络后点击继续。', 'network_failed', 503) from None
                        delay = min(60, 2**min(chunk_failures, 6))*self.retry_scale
                        self.update(identity, state='waiting_retry', retry_count=current['retry_count']+1, next_retry_at=time.time()+delay)
                        self.controls[identity].wait(delay)
                        self.control(identity)
                        # Never blindly resend a chunk: Google may have received part/all of it.
                        if not self.probe(identity, drive, uri): return
                        continue
                    if r.status_code in (200, 201):
                        self.finish(identity, drive); return
                    if r.status_code in (404, 410):
                        if not self.probe(identity, drive, uri): return
                        continue
                    drive.expect(r, (308,))
                    offset = committed(r, job['size'])
                    if not start < offset <= start+len(data):
                        raise AppError('Google 未确认新增字节，请稍后继续查询进度。', 'no_progress')
                    self.acknowledge(identity, offset)
                    chunk_failures = 0
                    if offset == job['size']:
                        if not self.probe(identity, drive, uri): return
                        raise AppError('数据发送完成，但 Google 尚未确认完成，请稍后继续校验。')
        except StopJob:
            pass
        except AppError as exc:
            with self.lock:
                current = self.store.get('jobs', identity)
                if current['state'] in ('pausing', 'cancelling') or self.shutdown_event.is_set():
                    self.update(identity, state='cancelled' if current['state']=='cancelling' else 'paused', error=None, next_retry_at=None)
                else:
                    self.update(identity, state='waiting_auth' if exc.code=='waiting_auth' else 'failed', error=str(exc), next_retry_at=None)
                    self.store.event(str(exc), identity)
        except Exception as exc:
            self.update(identity, state='failed', error='任务异常（'+type(exc).__name__+'），进度已保留。', next_retry_at=None)
            self.store.event('工作线程异常，源文件保留。', identity)
        finally:
            self.wake.set()
    def snapshot(self):
        now = time.monotonic(); result=[]
        with self.lock:
            for job in self.store.jobs():
                history = self.samples.get(job['id'], deque()); speed=0
                if job['state']=='uploading' and len(history)>1 and now-history[-1][0]<10:
                    speed = max(0, (history[-1][1]-history[0][1])/max(.1, now-history[0][0]))
                job['speed_bps']=speed
                job['eta_seconds']=(job['size']-job['confirmed'])/speed if speed>0 else None
                job['percent']=100*job['confirmed']/job['size'] if job['size'] else (100 if job['state']=='completed' else 0)
                result.append(job)
        return {'jobs':result, 'events':self.store.events(), 'seq':self.seq, 'server_time':time.time()}
