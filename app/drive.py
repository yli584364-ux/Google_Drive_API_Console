import re
from urllib.parse import urlparse
import requests
from .storage import AppError

API = 'https://www.googleapis.com/drive/v3'
UPLOAD = 'https://www.googleapis.com/upload/drive/v3/files'
FIELDS = 'id,name,size,md5Checksum,parents,ownedByMe,webViewLink,mimeType,trashed,capabilities(canAddChildren)'


def quote(value):
    return value.replace('\\', '\\\\').replace("'", "\\'")


def folder_id(value):
    value = value.strip()
    if value == 'root': return value
    if value.startswith('https://'):
        parsed = urlparse(value)
        match = re.match(r'/drive/(?:u/\d+/)?folders/([\w-]+)', parsed.path)
        if parsed.hostname != 'drive.google.com' or not match:
            raise AppError('请输入 Google Drive 文件夹链接或文件夹 ID。')
        return match[1]
    if not re.fullmatch(r'[A-Za-z0-9_-]{5,200}', value):
        raise AppError('文件夹 ID 格式无效。')
    return value


class Drive:
    def __init__(self, auth):
        self.auth = auth
        self.http = requests.Session()
    def request(self, method, url, **kwargs):
        p = urlparse(url)
        if p.scheme != 'https' or p.hostname != 'www.googleapis.com' or p.username or p.port not in (None, 443):
            raise AppError('上传会话地址异常，已停止访问。', 'invalid_session')
        headers = dict(kwargs.pop('headers', {}))
        for attempt in range(2):
            headers['Authorization'] = 'Bearer ' + self.auth.token(force=attempt > 0)
            try:
                response = self.http.request(method, url, headers=headers, timeout=(15, 60), allow_redirects=False, **kwargs)
            except requests.RequestException:
                raise AppError('网络中断，等待重试。', 'transient', 503) from None
            if response.status_code != 401: break
        if response.status_code in (408, 429) or response.status_code >= 500:
            raise AppError('Google 暂时不可用或限流，等待重试。', 'transient', 503)
        if response.status_code == 403:
            try: reasons = {e.get('reason') for e in response.json().get('error', {}).get('errors', [])}
            except ValueError: reasons = set()
            if reasons & {'rateLimitExceeded', 'userRateLimitExceeded', 'backendError'}:
                raise AppError('Google 限流，等待重试。', 'transient', 503)
            if 'storageQuotaExceeded' in reasons:
                raise AppError('Google 云盘可用空间不足。', 'quota', 409)
            raise AppError('Google 拒绝访问，请检查账号、API启用状态及目录权限。', 'permission', 403)
        if response.status_code == 401:
            raise AppError('Google 授权失效，请重新连接。', 'waiting_auth', 401)
        return response
    @staticmethod
    def expect(response, statuses=(200,)):
        if response.status_code not in statuses:
            raise AppError('Google 返回 HTTP '+str(response.status_code)+'，上传会话已保留。', 'google_error', 502)
        return response
    def metadata(self, identity):
        r = self.request('GET', API+'/files/'+identity, params={'fields': FIELDS})
        if r.status_code == 404: return None
        return self.expect(r).json()
    def folder(self, identity):
        identity = folder_id(identity)
        m = self.metadata(identity)
        if not m or m.get('trashed') or m.get('mimeType') != 'application/vnd.google-apps.folder':
            raise AppError('云端文件夹不存在或已移入回收站。')
        if not m.get('capabilities', {}).get('canAddChildren'):
            raise AppError('当前账号没有向这个文件夹上传的权限。', 'permission', 403)
        return {'id': m['id'], 'name': m['name'], 'url': m.get('webViewLink')}
    def folders(self, parent='root', page_token=None, search=''):
        parent = folder_id(parent)
        query = "trashed = false and mimeType = 'application/vnd.google-apps.folder' and '"+quote(parent)+"' in parents"
        if search: query += " and name contains '"+quote(search)+"'"
        params = {'q': query, 'fields': 'nextPageToken,files(id,name,webViewLink,capabilities(canAddChildren))', 'pageSize': 100, 'orderBy': 'name'}
        if page_token: params['pageToken'] = page_token
        d = self.expect(self.request('GET', API+'/files', params=params)).json()
        return {'folders': [dict(id=f['id'], name=f['name'], writable=f.get('capabilities', {}).get('canAddChildren', False)) for f in d.get('files', [])], 'next_page_token': d.get('nextPageToken')}
    def find_files(self, name, parent):
        query = "trashed = false and name = '"+quote(name)+"' and '"+quote(parent)+"' in parents"
        page = None; results = []
        while True:
            params = {'q': query, 'fields': 'nextPageToken,files('+FIELDS+')', 'pageSize': 100}
            if page: params['pageToken'] = page
            d = self.expect(self.request('GET', API+'/files', params=params)).json()
            results.extend(d.get('files', [])); page = d.get('nextPageToken')
            if not page: return results
    def reserve(self):
        return self.expect(self.request('GET', API+'/files/generateIds', params={'count': 1, 'space': 'drive', 'type': 'files'})).json()['ids'][0]
    def ensure_folder(self, name, parent, reserved):
        # Persist the reserved ID before calling this method. A lost POST response
        # is recovered by ID rather than creating a second same-name folder.
        existing = self.metadata(reserved)
        if not existing:
            matches = self.find_files(name, parent)
            if len(matches) > 1:
                raise AppError('云端存在多个同名目录：'+name+'，请先处理重名。', 'name_conflict', 409)
            if matches:
                existing = matches[0]
            else:
                response = self.request('POST', API+'/files', params={'fields': FIELDS},
                    json={'id': reserved, 'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent]})
                existing = self.metadata(reserved) if response.status_code == 409 else self.expect(response, (200, 201)).json()
        if (not existing or existing.get('trashed') or existing.get('name') != name
                or parent not in existing.get('parents', [])
                or existing.get('mimeType') != 'application/vnd.google-apps.folder'):
            raise AppError('云端同名项目不是目标文件夹，或目录信息不一致：'+name, 'name_conflict', 409)
        return self.folder(existing['id'])
    def initiate(self, job):
        r = self.request('POST', UPLOAD, params={'uploadType': 'resumable'},
            json={'id': job['remote_id'], 'name': job['name'], 'parents': [job['folder_id']], 'mimeType': 'application/octet-stream'},
            headers={'X-Upload-Content-Type': 'application/octet-stream', 'X-Upload-Content-Length': str(job['size'])})
        if r.status_code == 409: return None
        self.expect(r, (200, 201))
        uri = r.headers.get('Location', '')
        if urlparse(uri).hostname != 'www.googleapis.com' or urlparse(uri).scheme != 'https':
            raise AppError('Google 没有返回有效的续传会话。')
        return uri
    def probe(self, uri, size):
        return self.request('PUT', uri, data=b'', headers={'Content-Length': '0', 'Content-Range': 'bytes */'+str(size)})
    def chunk(self, uri, data, start, size):
        headers = {'Content-Type': 'application/octet-stream', 'Content-Length': str(len(data))}
        if size:
            headers['Content-Range'] = f'bytes {start}-{start+len(data)-1}/{size}'
        return self.request('PUT', uri, data=data, headers=headers)


def committed(response, size):
    value = response.headers.get('Range')
    if not value: return 0
    match = re.fullmatch(r'bytes=0-(\d+)', value)
    if not match or not 0 <= int(match[1]) < size:
        raise AppError('Google 返回的续传位置异常。')
    return int(match[1])+1
