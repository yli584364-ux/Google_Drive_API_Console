from datetime import datetime
import hashlib
import json
from pathlib import Path
import threading
import time

import requests
from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from .storage import AppError

SCOPES = ['https://www.googleapis.com/auth/drive']


class AuthManager:
    def __init__(self, store, vault, origin, busy=lambda: False):
        self.store, self.vault, self.origin, self.busy = store, vault, origin, busy
        self.lock = threading.RLock()
        self.pending = None
        self.last_error = None
        self.config_path = store.setting('credential_path', '')
        self.session_identity = None
    def read_config(self, path=None):
        path = Path((path or self.config_path).strip().strip('"')).expanduser()
        if not path.is_file() or path.suffix.lower() != '.json' or path.stat().st_size > 1024*1024:
            raise AppError('请输入可读取的 OAuth 桌面应用 JSON 文件完整路径。')
        try:
            config = json.loads(path.read_text(encoding='utf-8-sig'))
            c = config['installed']
            assert c.get('client_id') and c.get('client_secret')
            assert c['token_uri'] == 'https://oauth2.googleapis.com/token'
            assert c['auth_uri'] in ('https://accounts.google.com/o/oauth2/auth', 'https://accounts.google.com/o/oauth2/v2/auth')
        except (ValueError, KeyError, AssertionError):
            raise AppError('不是有效的 Google OAuth 桌面应用凭据；不会保存 JSON 副本。') from None
        return str(path.resolve()), config
    def key(self, config):
        return 'token-' + hashlib.sha256(config['installed']['client_id'].encode()).hexdigest()[:24]
    def configure(self, path, remember=True):
        with self.lock:
            if self.busy():
                raise AppError('请先暂停全部上传，再更换凭据路径。', 'busy', 409)
            resolved, config = self.read_config(path)
            self.config_path = resolved
            self.store.set_setting('credential_path', resolved if remember else '')
            self.pending = None; self.session_identity = None; self.last_error = None
            return {'path': resolved, 'client_type': 'desktop', 'original_json_copied': False}
    def start(self):
        with self.lock:
            if self.busy():
                raise AppError('请先暂停上传，再重新授权。', 'busy', 409)
            _, config = self.read_config()
            flow = InstalledAppFlow.from_client_config(config, SCOPES, autogenerate_code_verifier=True)
            flow.redirect_uri = self.origin + '/api/auth/callback'
            url, state = flow.authorization_url(access_type='offline', prompt='consent')
            self.pending = {'flow': flow, 'state': state, 'expires': time.time()+900, 'client_id': config['installed']['client_id']}
            self.last_error = None
            return {'authorization_url': url}
    def callback(self, state, code, error=None):
        with self.lock:
            pending = self.pending
            self.pending = None
            if not pending or pending['expires'] < time.time() or state != pending['state']:
                raise AppError('授权请求已过期或不匹配，请回到工作台重新连接。')
            if error or not code:
                self.last_error = 'Google 授权未完成，请重新连接。'
                raise AppError(self.last_error)
            _, config = self.read_config()
            if config['installed']['client_id'] != pending['client_id']:
                raise AppError('授权期间凭据改变，请重新连接。')
            try:
                pending['flow'].fetch_token(code=code, timeout=45)
                creds = pending['flow'].credentials
                r = requests.get('https://www.googleapis.com/drive/v3/about',
                    params={'fields': 'user(emailAddress)'}, headers={'Authorization': 'Bearer '+creds.token}, timeout=(15, 45))
                r.raise_for_status()
                email = r.json()['user']['emailAddress']
            except Exception:
                self.last_error = '授权回调或账号校验失败，请检查网络后重新连接。'
                raise AppError(self.last_error, 'auth_failed', 401) from None
            self.save(config, creds, email)
            self.session_identity = {'email': email, 'client_id': pending['client_id']}
            self.store.event('Google 授权完成。')
            return email
    def save(self, config, creds, email):
        # Intentionally do NOT serialize Credentials.to_json(): it contains client_secret.
        self.vault.put(self.key(config), {'access_token': creds.token, 'refresh_token': creds.refresh_token,
            'expiry': creds.expiry.isoformat() if creds.expiry else None,
            'email': email, 'client_id': config['installed']['client_id']})
    def saved(self):
        _, config = self.read_config()
        data = self.vault.get(self.key(config))
        if not data.get('refresh_token') or data.get('client_id') != config['installed']['client_id']:
            raise AppError('请先连接 Google 并完成授权。', 'waiting_auth', 401)
        return config, data
    def identity(self):
        with self.lock:
            config, saved = self.saved()
            return {'email': saved['email'], 'client_id': config['installed']['client_id']}
    def status(self):
        try:
            identity = self.identity()
            return {'connected': True, 'email': identity['email'], 'pending': bool(self.pending), 'error': self.last_error}
        except AppError:
            return {'connected': False, 'pending': bool(self.pending and self.pending['expires'] > time.time()), 'error': self.last_error}
    def token(self, force=False):
        with self.lock:
            config, data = self.saved()
            c = config['installed']
            expiry = datetime.fromisoformat(data['expiry']).replace(tzinfo=None) if data.get('expiry') else None
            creds = Credentials(token=data.get('access_token'), refresh_token=data['refresh_token'],
                token_uri=c['token_uri'], client_id=c['client_id'], client_secret=c['client_secret'],
                scopes=SCOPES, expiry=expiry)
            if force or not creds.valid:
                try:
                    creds.refresh(Request())
                except TransportError:
                    raise AppError('网络暂时不可用，稍后重试。', 'transient', 503) from None
                except RefreshError as exc:
                    if getattr(exc, 'retryable', False):
                        raise AppError('Google 授权服务暂时不可用。', 'transient', 503) from None
                    raise AppError('Google 授权已过期或撤销，请重新连接。', 'waiting_auth', 401) from None
                self.save(config, creds, data['email'])
            return creds.token
