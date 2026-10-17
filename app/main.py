import asyncio
from contextlib import asynccontextmanager
import html
import os
from pathlib import Path
import secrets
import time

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .auth import AuthManager
from .drive import Drive
from .engine import Engine, Files
from .storage import AppError, InstanceLock, Store, Vault

ROOT = Path(__file__).resolve().parents[1]


class CredentialPath(BaseModel):
    path: str = Field(min_length=1, max_length=4096)
    remember: bool = True
class SourcePath(BaseModel):
    path: str = Field(min_length=1, max_length=4096)
class FileDescriptor(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)
    lastModified: int | None = None
class ResolveFiles(BaseModel):
    source_id: str
    files: list[FileDescriptor] = Field(default_factory=list, max_length=200)
    path: str | None = Field(default=None, max_length=4096)
class FolderRequest(BaseModel):
    value: str = Field(min_length=1, max_length=2048)
class NewJob(BaseModel):
    file_id: str
    folder_id: str
    chunk_mib: int = Field(default=8, ge=1, le=64)
class FolderSample(BaseModel):
    relative: str = Field(min_length=1, max_length=4096)
    size: int = Field(ge=0)
class NewFolderJob(BaseModel):
    source_id: str
    folder_id: str
    name: str | None = Field(default=None, max_length=255)
    path: str | None = Field(default=None, max_length=4096)
    samples: list[FolderSample] = Field(default_factory=list, max_length=64)
    chunk_mib: int = Field(default=8, ge=1, le=64)
class Tuning(BaseModel):
    concurrency: int = Field(ge=1, le=2)


def create_app(data_dir=None, origin='http://127.0.0.1:8787', auth_factory=AuthManager, drive_factory=Drive, lock_instance=True):
    data_dir = Path(data_dir or ROOT/'data')
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(data_dir/'jobs.sqlite3')
    vault = Vault(data_dir/'secrets')
    auth = auth_factory(store, vault, origin)
    files = Files(store, auth, ROOT)
    engine = Engine(store, vault, auth, files, drive_factory)
    auth.busy = engine.busy
    csrf = secrets.token_urlsafe(32)
    instance = InstanceLock(data_dir/'server.lock')

    @asynccontextmanager
    async def lifespan(app):
        if lock_instance: instance.acquire()
        engine.start()
        try: yield
        finally:
            await asyncio.to_thread(engine.stop)
            if lock_instance: instance.release()

    app = FastAPI(title='Drive 续传工作台', lifespan=lifespan, docs_url='/api/docs', redoc_url=None)
    app.state.store, app.state.auth, app.state.engine = store, auth, engine
    app.state.files = files

    @app.middleware('http')
    async def local_guard(request: Request, call_next):
        allowed_host = origin.split('://', 1)[1]
        if request.headers.get('host') != allowed_host:
            return JSONResponse({'detail':'请通过本机启动地址访问。'}, status_code=403)
        supplied_origin = request.headers.get('origin')
        if supplied_origin and supplied_origin != origin:
            return JSONResponse({'detail':'不接受其他站点发起的请求。'}, status_code=403)
        if request.method not in ('GET', 'HEAD', 'OPTIONS'):
            if request.headers.get('x-csrf-token') != csrf or request.cookies.get('drive_session') != csrf:
                return JSONResponse({'detail':'页面会话失效，请刷新工作台。'}, status_code=403)
            try:
                if int(request.headers.get('content-length', '0')) > 1024*1024:
                    return JSONResponse({'detail':'只接受文件元数据，不接受整文件内容。'}, status_code=413)
            except ValueError:
                return JSONResponse({'detail':'请求长度无效。'}, status_code=400)
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        return response

    @app.exception_handler(AppError)
    async def app_error(request, exc):
        return JSONResponse({'detail': str(exc), 'code': exc.code}, status_code=exc.status)

    @app.exception_handler(Exception)
    async def unknown_error(request, exc):
        # No raw exception/token-bearing URL is returned to the client.
        return JSONResponse({'detail':'操作失败，请检查本地路径、授权或网络。', 'code':'internal_error'}, status_code=500)

    @app.get('/')
    def index(): return FileResponse(ROOT/'static/index.html')

    @app.get('/api/bootstrap')
    def bootstrap():
        response = JSONResponse({'csrf': csrf, 'credential_path': auth.config_path, 'auth':auth.status(),
            'sources':store.sources(), 'concurrency':store.setting('concurrency', 1),
            'last_source':store.setting('last_source', None),
            'target':store.setting('target', None), **engine.snapshot()})
        response.set_cookie('drive_session', csrf, httponly=True, samesite='strict', path='/')
        return response

    @app.put('/api/settings/credential-path')
    def credential_path(body: CredentialPath): return auth.configure(body.path, body.remember)

    @app.put('/api/settings/tuning')
    def tuning(body: Tuning):
        store.set_setting('concurrency', body.concurrency); engine.wake.set()
        return {'concurrency':body.concurrency}

    @app.post('/api/auth/start')
    def auth_start(): return auth.start()

    @app.get('/api/auth/status')
    def auth_status(): return auth.status()

    @app.get('/api/auth/callback')
    def callback(state: str='', code: str='', error: str | None=None):
        try:
            auth.callback(state, code, error)
            text='Google 授权成功。可以关闭此页，返回 Drive 续传工作台。'
        except AppError as exc:
            text=str(exc)
        return HTMLResponse('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>Google 授权结果</title><body><h2>'+html.escape(text)+'</h2><p>本地源文件未被修改。</p></body></html>')

    @app.post('/api/sources')
    def source(body: SourcePath): return files.register(body.path)

    @app.post('/api/sources/resolve')
    def resolve(body: ResolveFiles):
        if not body.path and not body.files: raise AppError('请拖入文件或填写文件路径。')
        return {'files': files.resolve(body.source_id, [x.model_dump() for x in body.files], body.path)}

    @app.get('/api/drive/folders')
    def folders(parent: str='root', page_token: str | None=None, search: str=''):
        return drive_factory(auth).folders(parent, page_token, search[:200])

    @app.post('/api/drive/folders/resolve')
    def resolve_folder(body: FolderRequest):
        folder = drive_factory(auth).folder(body.value)
        store.set_setting('target', folder)
        return folder

    @app.post('/api/jobs')
    def new_job(body: NewJob):
        folder = drive_factory(auth).folder(body.folder_id)
        return engine.create(body.file_id, folder, body.chunk_mib)

    @app.get('/api/jobs')
    def get_jobs(): return engine.snapshot()

    @app.post('/api/folder-jobs')
    def new_folder_job(body: NewFolderJob):
        folder = drive_factory(auth).folder(body.folder_id)
        return engine.create_folder(body.source_id, folder, body.chunk_mib,
            body.name, body.path, [s.model_dump() for s in body.samples])

    @app.get('/api/jobs/{identity}/items')
    def folder_items(identity: str, limit: int=100, offset: int=0):
        job = store.get('jobs', identity)
        if job.get('kind')!='folder': raise AppError('这个任务不是文件夹。')
        if not 1 <= limit <= 200 or offset < 0: raise AppError('分页参数无效。')
        return {'items': store.items(identity, limit, offset), 'total': job['file_count']}

    @app.post('/api/service/stop', status_code=202)
    def stop_service():
        callback = getattr(app.state, 'shutdown_callback', None)
        if callback is None: raise AppError('当前运行方式不支持网页停止；请关闭服务终端。')
        callback()
        return {'status':'stopping', 'message':'正在保存任务并停止本地服务，源文件保留。'}

    @app.post('/api/jobs/{identity}/{action}', status_code=202)
    def action(identity: str, action: str): return engine.action(identity, action)

    @app.get('/api/jobs/{identity}/events')
    def events(identity: str):
        store.get('jobs', identity)
        return {'events':store.events(identity)}

    @app.websocket('/ws/jobs')
    async def socket(ws: WebSocket):
        if ws.headers.get('origin') != origin or ws.cookies.get('drive_session') != csrf:
            await ws.close(code=1008); return
        await ws.accept()
        try:
            while True:
                await ws.send_json({**engine.snapshot(), 'auth':auth.status()})
                await asyncio.sleep(1)
        except (WebSocketDisconnect, RuntimeError, OSError): pass

    app.mount('/static', StaticFiles(directory=ROOT/'static'), name='static')
    return app
