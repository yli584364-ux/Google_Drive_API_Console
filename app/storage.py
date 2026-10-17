import ctypes
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid


class AppError(Exception):
    def __init__(self, message, code='invalid_request', status=400):
        super().__init__(message)
        self.code, self.status = code, status


def protect(data, decrypt=False):
    if os.name != 'nt':
        raise AppError('本程序的凭据加密需要 Windows。')
    class Blob(ctypes.Structure):
        _fields_ = [('size', ctypes.c_uint32), ('data', ctypes.POINTER(ctypes.c_ubyte))]
    buf = ctypes.create_string_buffer(data)
    src, dst = Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte))), Blob()
    lib = ctypes.WinDLL('crypt32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    if decrypt:
        fn = lib.CryptUnprotectData
        fn.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p,
                       ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(Blob)]
        ok = fn(ctypes.byref(src), None, None, None, None, 1, ctypes.byref(dst))
    else:
        fn = lib.CryptProtectData
        fn.argtypes = [ctypes.POINTER(Blob), ctypes.c_wchar_p, ctypes.c_void_p,
                       ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(Blob)]
        ok = fn(ctypes.byref(src), 'Drive upload workspace', None, None, None, 1, ctypes.byref(dst))
    if not ok:
        raise AppError('无法解密本机授权数据，请使用原 Windows 账号或重新授权。', 'vault_error')
    try:
        return ctypes.string_at(dst.data, dst.size)
    finally:
        kernel.LocalFree(ctypes.cast(dst.data, ctypes.c_void_p))


class Vault:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
    def path(self, key):
        if not key.replace('-', '').replace('_', '').isalnum():
            raise ValueError('Invalid vault key')
        return self.root / (key + '.dpapi')
    def get(self, key):
        with self.lock:
            p = self.path(key)
            return json.loads(protect(p.read_bytes(), True)) if p.exists() else {}
    def put(self, key, value):
        with self.lock:
            p = self.path(key)
            tmp = p.with_suffix('.tmp')
            with tmp.open('wb') as stream:
                stream.write(protect(json.dumps(value).encode()))
                stream.flush(); os.fsync(stream.fileno())
            os.replace(tmp, p)


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS folder_items (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, body TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS folder_items_parent ON folder_items(parent_id);
            CREATE TABLE IF NOT EXISTS folder_dirs (parent_id TEXT NOT NULL, relative TEXT NOT NULL,
                remote_id TEXT, reserved_id TEXT, PRIMARY KEY(parent_id,relative));
            CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT, time REAL NOT NULL, message TEXT NOT NULL);
        ''')
        self.db.commit()
    def setting(self, key, default=None):
        with self.lock:
            row = self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
            return json.loads(row[0]) if row else default
    def set_setting(self, key, value):
        with self.lock, self.db:
            self.db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)', (key, json.dumps(value)))
    def add_source(self, path):
        with self.lock, self.db:
            row = self.db.execute('SELECT id FROM sources WHERE path=?', (path,)).fetchone()
            identity = row[0] if row else uuid.uuid4().hex
            self.db.execute('INSERT OR IGNORE INTO sources VALUES (?,?)', (identity, path))
            return {'id': identity, 'path': path}
    def sources(self):
        with self.lock:
            return [{'id': r[0], 'path': r[1]} for r in self.db.execute('SELECT id,path FROM sources')]
    def put(self, table, value):
        if table not in ('jobs', 'files'):
            raise ValueError('Unknown table')
        with self.lock, self.db:
            if table == 'jobs' and value.get('parent_id'):
                self.db.execute('INSERT INTO folder_items VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body',
                    (value['id'], value['parent_id'], json.dumps(value, ensure_ascii=False)))
                return
            self.db.execute(f'INSERT OR REPLACE INTO {table} VALUES (?,?)',
                            (value['id'], json.dumps(value, ensure_ascii=False)))
    def get(self, table, identity):
        if table not in ('jobs', 'files'):
            raise ValueError('Unknown table')
        with self.lock:
            row = self.db.execute(f'SELECT body FROM {table} WHERE id=?', (identity,)).fetchone()
            if not row and table == 'jobs':
                row = self.db.execute('SELECT body FROM folder_items WHERE id=?', (identity,)).fetchone()
            if not row:
                raise AppError('找不到任务或文件，请重新加入队列。', 'not_found', 404)
            return json.loads(row[0])
    def jobs(self):
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute('SELECT body FROM jobs ORDER BY rowid DESC')]
    def reset_manifest(self, parent):
        with self.lock, self.db:
            self.db.execute('DELETE FROM folder_items WHERE parent_id=?', (parent,))
            self.db.execute('DELETE FROM folder_dirs WHERE parent_id=?', (parent,))
    def add_directory(self, parent, relative):
        with self.lock, self.db:
            self.db.execute('INSERT OR IGNORE INTO folder_dirs(parent_id,relative) VALUES (?,?)', (parent, relative))
    def directory(self, parent, relative):
        with self.lock:
            row = self.db.execute('SELECT remote_id,reserved_id FROM folder_dirs WHERE parent_id=? AND relative=?', (parent, relative)).fetchone()
            if not row: raise AppError('目录清单不完整，请重新添加文件夹。')
            return {'relative': relative, 'remote_id': row[0], 'reserved_id': row[1]}
    def set_directory(self, parent, relative, remote_id=None, reserved_id=None):
        with self.lock, self.db:
            self.db.execute('UPDATE folder_dirs SET remote_id=COALESCE(?,remote_id),reserved_id=COALESCE(?,reserved_id) WHERE parent_id=? AND relative=?', (remote_id, reserved_id, parent, relative))
    def next_directory(self, parent):
        with self.lock:
            row = self.db.execute('SELECT relative FROM folder_dirs WHERE parent_id=? AND remote_id IS NULL ORDER BY rowid LIMIT 1', (parent,)).fetchone()
            return self.directory(parent, row[0]) if row else None
    def next_item(self, parent):
        with self.lock:
            row = self.db.execute("SELECT body FROM folder_items WHERE parent_id=? AND json_extract(body,'$.state')!='completed' ORDER BY rowid LIMIT 1", (parent,)).fetchone()
            return json.loads(row[0]) if row else None
    def items(self, parent, limit=100, offset=0):
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute('SELECT body FROM folder_items WHERE parent_id=? ORDER BY rowid LIMIT ? OFFSET ?', (parent, limit, offset))]
    def event(self, message, job_id=None):
        with self.lock, self.db:
            self.db.execute('INSERT INTO events(job_id,time,message) VALUES (?,?,?)',
                            (job_id, time.time(), message))
    def events(self, job_id=None, limit=60):
        with self.lock:
            if job_id:
                rows = self.db.execute('SELECT seq,job_id,time,message FROM events WHERE job_id=? ORDER BY seq DESC LIMIT ?', (job_id, limit))
            else:
                rows = self.db.execute('SELECT seq,job_id,time,message FROM events ORDER BY seq DESC LIMIT ?', (limit,))
            return [dict(seq=r[0], job_id=r[1], time=r[2], message=r[3]) for r in rows]


class InstanceLock:
    def __init__(self, path):
        self.path = path
    def acquire(self):
        import msvcrt
        self.handle = open(self.path, 'a+b')
        if self.handle.tell() == 0:
            self.handle.write(b'0'); self.handle.flush()
        self.handle.seek(0)
        try:
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            self.handle.close()
            raise AppError('此工作台已有一个后台实例运行，请直接打开原页面。') from None
    def release(self):
        import msvcrt
        if getattr(self, 'handle', None):
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            self.handle.close()
