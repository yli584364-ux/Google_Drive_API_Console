'use strict';
const $=id=>document.getElementById(id),MiB=1024*1024;
const labels={scan_queued:'等待扫描',scanning:'扫描文件夹',staged:'等待开始',queued:'队列等待',preparing:'准备与计算校验值',uploading:'上传中',waiting_retry:'等待网络',pausing:'暂停中',paused:'已暂停',verifying:'校验中',completed:'已完成 · MD5 一致',cancelling:'取消中',cancelled:'已取消',failed:'需要处理',waiting_auth:'等待授权',expired:'会话失效'};
let csrf='',jobs=[],source=null,target=null,connected=false,filter='all',details=new Set(),socket=null,reconnect=null,cancelId=null,toastTimer,browserStack=[{id:'root',name:'我的云盘'}],folderPage=null,lastSnapshot=0,pendingAdd=false;
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const bytes=n=>n>=1e9?(n/1e9).toFixed(3)+' GB':n>=MiB?(n/MiB).toFixed(2)+' MiB':n+' B';
const seconds=n=>{if(!Number.isFinite(n)||n<0)return'计算中';const h=Math.floor(n/3600),m=Math.floor(n%3600/60),s=Math.floor(n%60);return h?`${h}小时 ${m}分`:`${m}分 ${s}秒`};
function message(text){$('toast').textContent=text;$('toast').hidden=false;clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('toast').hidden=true,6000)}
async function api(path,method='GET',body){const r=await fetch(path,{method,headers:{'Content-Type':'application/json','X-CSRF-Token':csrf},...(body===undefined?{}:{body:JSON.stringify(body)})});const data=await r.json();if(!r.ok)throw Error(typeof data.detail==='string'?data.detail:'请求格式无效，请检查输入。');return data}
function remember(){try{if($('remember').checked)localStorage.setItem('drive_credentials_path',$('credential').value);else localStorage.removeItem('drive_credentials_path')}catch{}}
function authState(data){connected=data.connected;$('authStatus').innerHTML=connected?'<span class="dot on"></span>'+esc(data.email):'<span class="dot"></span>'+(data.pending?'请在授权页面完成登录':esc(data.error||'尚未连接 Google'));$('auth').textContent=connected?'重新授权':'连接 Google'}
function applySnapshot(data){jobs=data.jobs||[];lastSnapshot=Date.now();if(data.auth)authState(data.auth);render();$('logs').innerHTML=(data.events||[]).map(e=>`<div class="log-row"><span class="log-time">${esc(new Date(e.time*1000).toLocaleTimeString('zh-CN',{hour12:false}))}</span><span>${esc(e.message)}</span></div>`).join('')}
function render(){
 const shown=jobs.filter(j=>!j.archived),hidden=jobs.filter(j=>j.archived),counted=shown.filter(j=>j.state!=='cancelled'),active=counted.filter(j=>j.state!=='completed'),total=counted.reduce((s,j)=>s+j.size,0),sent=counted.reduce((s,j)=>s+j.confirmed,0),speed=active.reduce((s,j)=>s+j.speed_bps,0);
 $('overall').textContent=total?(sent/total*100).toFixed(1)+'%':counted.length?'0%':'—';
 $('bytesSummary').textContent=bytes(sent)+' / '+bytes(total);$('speed').textContent=(speed/MiB).toFixed(2);
 $('eta').textContent=speed?seconds(active.reduce((s,j)=>s+j.size-j.confirmed,0)/speed):active.some(j=>j.state==='waiting_retry')?'等待网络':active.some(j=>['uploading','preparing','verifying','scanning','scan_queued'].includes(j.state))?'计算中':active.length?'待继续':'—';
 $('activeCount').textContent=active.length;$('doneCount').textContent=shown.filter(j=>j.state==='completed').length+' 个已校验';$('queueCount').textContent=shown.length+' 个任务';$('hiddenCount').textContent=hidden.length;$('hideFinished').disabled=!shown.some(j=>['completed','cancelled'].includes(j.state));
 const visible=filter==='history'?hidden:shown.filter(j=>filter==='all'||(filter==='completed'?j.state==='completed':!['completed','cancelled'].includes(j.state)));
 const tasks=$('tasks'),scrollTop=tasks.scrollTop;
 $('queueFootnote').textContent=filter==='history'?'已隐藏记录保留在本机；恢复只恢复显示，不会开始上传。':'任务列表可在区域内上下滚动；隐藏记录不影响本地文件或云端文件。';
 $('tasks').innerHTML=visible.length?visible.map(j=>{
  const folder=j.kind==='folder',resume=['staged','paused','failed','waiting_auth'].includes(j.state),pause=['scan_queued','scanning','queued','preparing','uploading','waiting_retry','verifying'].includes(j.state),cancel=!['completed','cancelled','cancelling'].includes(j.state);
  const pill={uploading:'running',completed:'complete',waiting_retry:'retry'}[j.state]||j.state;
  const status=folder&&j.state==='completed'?'已完成 · 全部文件已校验':labels[j.state]||j.state;
  return `<article class="task ${esc(pill)}"><div class="task-top"><div class="file-icon">${folder?'DIR':'FILE'}</div><div class="task-title"><strong>${esc(j.name)}</strong><div class="task-path" title="${esc(j.path)}">${esc(j.path)}</div></div><span class="pill ${esc(pill)}">${esc(status)}</span></div>
   ${folder?`<div class="folder-summary">${j.scanned?'目录清单':'扫描统计'} · ${j.completed_files||0} / ${j.file_count||0} 个文件已校验 · ${j.directory_count||0} 个文件夹（含根目录）${j.current_relative?`<div class="current-item">${esc(labels[j.current_state]||'正在处理')}：${esc(j.current_relative)}</div>`:''}</div>`:''}
   <div class="progress" role="progressbar" aria-label="${esc(j.name)}" aria-valuenow="${j.percent.toFixed(2)}" aria-valuemin="0" aria-valuemax="100"><div data-progress="${j.id}"></div></div><div class="task-bottom"><span><b>${j.percent.toFixed(2)}%</b> · ${bytes(j.confirmed)} / ${bytes(j.size)}${j.state==='uploading'?' · '+(j.speed_bps/MiB).toFixed(2)+' MiB/s':''}${j.state==='preparing'?' · 当前文件校验读取 '+bytes(j.hash_bytes||0):''}</span><div class="task-actions">
   ${pause?`<button data-action="pause" data-id="${j.id}">暂停</button>`:''}${resume?`<button data-action="resume" data-id="${j.id}">${j.state==='staged'?'开始':'继续'}</button>`:''}${j.state==='expired'?`<button data-action="restart" data-id="${j.id}">重新上传</button>`:''}${cancel?`<button class="quiet danger" data-action="cancel" data-id="${j.id}">取消</button>`:''}${['completed','cancelled'].includes(j.state)?`<button class="quiet" data-action="${j.archived?'restore':'archive'}" data-id="${j.id}">${j.archived?'恢复显示':'隐藏记录'}</button>`:''}${folder?`<button class="quiet" data-action="items" data-id="${j.id}">文件清单</button>`:''}<button class="quiet" data-action="details" data-id="${j.id}">${details.has(j.id)?'收起':'详情'}</button></div></div>
   ${j.error?`<div class="error-note">${esc(j.error)}</div>`:''}${details.has(j.id)?`<div class="details"><p>本地来源：${esc(j.path)}</p><p>目标：${esc(j.folder_name)} / ${folder?esc(j.name):''} · ${esc(j.folder_id)}</p><p>Google 账号：${esc(j.account)}</p><p>分块 ${j.chunk_mib} MiB · 累计重试 ${j.retry_count} 次${j.next_retry_at?' · '+Math.max(0,Math.ceil(j.next_retry_at-Date.now()/1000))+'秒后重试':''}</p><p>剩余时间：${j.eta_seconds===null?'等待有效速度样本':seconds(j.eta_seconds)}</p>${folder?'<p>保留根文件夹、所有子目录及空目录。按扫描时的文件清单上传；扫描后新增的文件需要另建任务。</p>':`<p>本地 MD5：${esc(j.md5||'尚未计算')}</p>`}${j.state==='completed'?'<p>全部上传文件的云端大小和 MD5 均已核验。</p>':''}${j.remote_url?`<p><a class="remote-link" href="${esc(j.remote_url)}" target="_blank" rel="noopener noreferrer">打开云盘${folder?'文件夹':'文件'} ↗</a></p>`:''}</div>`:''}</article>`;
 }).join(''):(filter==='history'?'<div class="empty">没有已隐藏记录。</div>':'<div class="empty">当前视图没有任务。先确认连接与目录，再添加文件或文件夹。</div>');
 tasks.scrollTop=scrollTop;
 for(const j of visible){const bar=document.querySelector(`[data-progress="${j.id}"]`);if(bar)bar.style.width=j.percent+'%'}
}
async function refresh(){applySnapshot(await api('/api/jobs'));authState(await api('/api/auth/status'))}
function connectSocket(){if(socket)socket.close();socket=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws/jobs');socket.onopen=()=>{$('serverBadge').textContent='实时连接正常';$('connectionNote').textContent='关闭网页后后台仍可上传'};socket.onmessage=e=>{try{applySnapshot(JSON.parse(e.data))}catch{}};socket.onclose=()=>{$('serverBadge').textContent='实时连接断开 · 重连中';clearTimeout(reconnect);reconnect=setTimeout(async()=>{try{await bootstrap(false)}catch{connectSocket()}},2500)};socket.onerror=()=>socket.close()}
async function bootstrap(initial=true){const d=await api('/api/bootstrap');csrf=d.csrf;if(initial){let saved='';try{saved=localStorage.getItem('drive_credentials_path')||''}catch{}$('credential').value=saved||d.credential_path||$('credential').value;$('concurrency').value=d.concurrency;$('autoResume').checked=d.auto_resume??true;if(d.last_source)$('source').value=d.last_source.path;source=d.sources.find(x=>x.path.toLowerCase()===$('source').value.toLowerCase())||null;if(source)$('sourceStatus').textContent='已注册：'+source.path;if(d.target){target=d.target;$('target').value=target.id;$('breadcrumb').textContent=target.name+' / '+target.id}}authState(d.auth);applySnapshot(d);connectSocket()}
async function saveCredentials(){const d=await api('/api/settings/credential-path','PUT',{path:$('credential').value,remember:$('remember').checked});remember();authState(await api('/api/auth/status'));return d}
async function ensureSource(){if(!source||source.path.toLowerCase()!==$('source').value.trim().replace(/^"|"$/g,'').toLowerCase()){source=await api('/api/sources','POST',{path:$('source').value});$('sourceStatus').textContent='已注册：'+source.path}return source}
async function ensureTarget(){if(!connected)throw Error('请先连接 Google。');if(!target||$('target').value.trim()!==target.id){target=await api('/api/drive/folders/resolve','POST',{value:$('target').value});$('target').value=target.id;$('breadcrumb').textContent=target.name+' / '+target.id}return target}
async function addFiles(fileList,path=null){const descriptors=Array.from(fileList).map(f=>({name:f.name,size:f.size,lastModified:f.lastModified}));if(pendingAdd)return;pendingAdd=true;try{await ensureSource();await ensureTarget();const body={source_id:source.id,...(path?{path}:{files:descriptors})};if(!path&&body.files.length>200)throw Error('每次最多加入 200 个文件。');const d=await api('/api/sources/resolve','POST',body);for(const f of d.files)await api('/api/jobs','POST',{file_id:f.id,folder_id:target.id,chunk_mib:Number($('chunk').value)});await refresh();message('文件已加入队列，点击“开始”后上传。')}catch(e){message(e.message)}finally{pendingAdd=false}}
async function addDirectory({name=null,path=null,samples=[]}){
 if(pendingAdd){message('正在添加任务，请稍候。');return}pendingAdd=true;
 try{await ensureSource();await ensureTarget();await api('/api/folder-jobs','POST',{source_id:source.id,folder_id:target.id,chunk_mib:Number($('chunk').value),name,path,samples});await refresh();message('文件夹已加入扫描队列。请核对本地路径，扫描完成后点击开始。')}
 catch(e){message(e.message)}finally{pendingAdd=false}
}
let manifestId=null,manifestOffset=0;
async function showManifest(id,append=false){
 if(!append){manifestId=id;manifestOffset=0;$('manifestList').textContent='';$('manifestDialog').showModal()}
 try{const d=await api(`/api/jobs/${id}/items?offset=${manifestOffset}&limit=100`),job=jobs.find(j=>j.id===id);$('manifestTitle').textContent=(job?.name||'文件夹')+' · 文件清单';$('manifestSummary').textContent=`扫描时的文件清单，共 ${d.total} 个文件。${job?.scanned?'':'扫描尚未完成。'}`;
 for(const item of d.items){const row=document.createElement('div');row.className='manifest-row';const name=document.createElement('span'),status=document.createElement('small');name.textContent=item.relative;status.textContent=bytes(item.size)+' · '+(labels[item.state]||item.state);row.append(name,status);$('manifestList').append(row)}
 manifestOffset+=d.items.length;$('manifestMore').hidden=manifestOffset>=d.total;if(!manifestOffset)$('manifestList').textContent='当前没有文件；空目录也会保留上传。'
 }catch(e){message(e.message)}
}
$('manifestMore').onclick=()=>showManifest(manifestId,true);$('closeManifest').onclick=()=>$('manifestDialog').close();
async function perform(id,action){try{await api(`/api/jobs/${id}/${action}`,'POST',{});await refresh()}catch(e){message(e.message)}}
$('themeToggle').value=window.driveTheme.getPreference();$('themeToggle').onchange=e=>window.driveTheme.setPreference(e.target.value);
$('remember').onchange=remember;$('credential').onchange=remember;$('validate').onclick=async()=>{try{await saveCredentials();message('桌面应用凭据有效，仅记录原文件路径。')}catch(e){message(e.message)}};
$('auth').onclick=async()=>{const popup=window.open('about:blank','drive_google_auth');try{await saveCredentials();const d=await api('/api/auth/start','POST',{});if(popup)popup.location.href=d.authorization_url;else{message('浏览器阻止授权弹窗，请允许此站点打开弹窗后再点击连接。')}authState(await api('/api/auth/status'))}catch(e){if(popup)popup.close();message(e.message)}};
$('registerSource').onclick=async()=>{try{await ensureSource();message('源目录已确认。')}catch(e){message(e.message)}};$('source').onchange=()=>{source=null};
$('resolveFolder').onclick=async()=>{try{await ensureTarget();message('目标目录可写。')}catch(e){message(e.message)}};
$('concurrency').onchange=async()=>{try{await api('/api/settings/tuning','PUT',{concurrency:Number($('concurrency').value)})}catch(e){message(e.message)}};
$('autoResume').onchange=async()=>{try{await api('/api/settings/tuning','PUT',{concurrency:Number($('concurrency').value),auto_resume:$('autoResume').checked});message($('autoResume').checked?'下次启动将自动恢复中断任务；手动暂停与取消不受影响。':'下次启动只恢复记录，需手动点击继续。')}catch(e){message(e.message)}};
$('tasks').onclick=e=>{const b=e.target.closest('[data-action]');if(!b)return;const id=b.dataset.id,action=b.dataset.action;if(action==='items'){showManifest(id)}else if(action==='details'){details.has(id)?details.delete(id):details.add(id);render()}else if(action==='cancel'){cancelId=id;$('cancelName').textContent=jobs.find(j=>j.id===id)?.name||'';$('cancelDialog').showModal()}else if(action==='restart'){if(confirm('原会话已失效，将从头上传该文件。继续吗？'))perform(id,action)}else perform(id,action)};
$('keepTask').onclick=()=>$('cancelDialog').close();$('confirmCancel').onclick=()=>{$('cancelDialog').close();perform(cancelId,'cancel')};
$('startAll').onclick=async()=>{for(const j of jobs.filter(j=>['staged','paused','failed','waiting_auth'].includes(j.state)))await perform(j.id,'resume')};$('pauseAll').onclick=async()=>{for(const j of jobs.filter(j=>['scan_queued','scanning','queued','preparing','uploading','waiting_retry','verifying'].includes(j.state)))await perform(j.id,'pause')};
$('refresh').onclick=()=>refresh().catch(e=>message(e.message));document.querySelectorAll('[data-filter]').forEach(b=>b.onclick=()=>{filter=b.dataset.filter;$('tasks').scrollTop=0;document.querySelectorAll('[data-filter]').forEach(x=>x.classList.toggle('active',x===b));render()});
$('hideFinished').onclick=async()=>{try{const result=await api('/api/history/archive','POST',{});await refresh();message('已隐藏 '+result.count+' 条已结束记录，可在“已隐藏”页恢复。')}catch(e){message(e.message)}};
$('pickFiles').onclick=()=>$('fileInput').click();$('fileInput').onchange=e=>{addFiles(e.target.files);e.target.value=''};
$('pickDirectory').onclick=()=>$('directoryInput').click();
$('directoryInput').onchange=e=>{
 const files=Array.from(e.target.files);e.target.value='';if(!files.length)return;
 const name=files[0].webkitRelativePath.split('/')[0];
 const samples=files.slice(0,64).map(f=>({relative:f.webkitRelativePath.split('/').slice(1).join('/'),size:f.size}));
 addDirectory({name,samples});
};
const drop=$('dropzone');
['dragenter','dragover'].forEach(n=>drop.addEventListener(n,e=>{e.preventDefault();drop.classList.add('over')}));
['dragleave','drop'].forEach(n=>drop.addEventListener(n,e=>{e.preventDefault();drop.classList.remove('over')}));
drop.addEventListener('drop',async e=>{
 // Capture entries synchronously: browsers clear DataTransfer after this event.
 const entries=Array.from(e.dataTransfer.items||[]).filter(i=>i.kind==='file').map(i=>({entry:i.webkitGetAsEntry?.()||null,file:i.getAsFile()}));
 const fallback=Array.from(e.dataTransfer.files||[]);
 if(pendingAdd){message('正在添加任务，请稍候。');return}
 const directories=entries.filter(x=>x.entry?.isDirectory),files=entries.filter(x=>!x.entry?.isDirectory&&x.file).map(x=>x.file);
 for(const item of directories)await addDirectory({name:item.entry.name});
 if(files.length)await addFiles(files);else if(!directories.length&&fallback.length)await addFiles(fallback);
 else if(!directories.length)message('请拖入本机文件或文件夹，也可以填写完整路径。');
});
window.addEventListener('dragover',e=>e.preventDefault());window.addEventListener('drop',e=>e.preventDefault());
$('addPath').onclick=()=>$('pathDialog').showModal();$('closePath').onclick=()=>$('pathDialog').close();
$('confirmPath').onclick=async()=>{if($('pathKind').value==='folder')await addDirectory({path:$('filePath').value});else await addFiles([],$('filePath').value);$('pathDialog').close()};
async function showFolders(append=false){try{const parent=browserStack.at(-1);$('folderPath').textContent=browserStack.map(x=>x.name).join(' / ');$('folderUp').disabled=browserStack.length===1;if(!append)$('folderList').textContent='正在读取…';const d=await api('/api/drive/folders?'+new URLSearchParams({parent:parent.id,...(append&&folderPage?{page_token:folderPage}:{})}));if(!append)$('folderList').textContent='';for(const f of d.folders){const button=document.createElement('button');button.className='folder-option';button.textContent='▣　'+f.name+(f.writable?'':'（不可写，可继续浏览）');button.onclick=()=>{browserStack.push(f);folderPage=null;showFolders()};$('folderList').append(button)}if(!$('folderList').children.length)$('folderList').textContent='此目录没有子文件夹。';folderPage=d.next_page_token;$('folderMore').hidden=!folderPage}catch(e){$('folderList').textContent=e.message}}
$('chooseFolder').onclick=()=>{$('folders').showModal();showFolders()};$('folderUp').onclick=()=>{browserStack.pop();showFolders()};$('folderMore').onclick=()=>showFolders(true);$('closeFolders').onclick=()=>$('folders').close();$('selectFolder').onclick=async()=>{try{const folder=browserStack.at(-1);target=await api('/api/drive/folders/resolve','POST',{value:folder.id});$('target').value=target.id;$('breadcrumb').textContent=browserStack.map(x=>x.name).join(' / ');$('folders').close()}catch(e){message(e.message)}};
setInterval(()=>{if(lastSnapshot&&Date.now()-lastSnapshot>6000){$('serverBadge').textContent='服务连接中断';$('speed').textContent='0.00';$('eta').textContent='等待连接'}},2000);
bootstrap().catch(e=>{$('serverBadge').textContent='未连接本地服务';message(e.message)});
