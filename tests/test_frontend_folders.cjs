// Exercise the actual UI event handlers without browser globals or Drive writes.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const elements = new Map(), requests = [];
function element(id) {
  if (!elements.has(id)) elements.set(id, {
    value: '', checked: true, hidden: false, textContent: '', innerHTML: '', handlers: {},
    classList: {add(){},remove(){},toggle(){}},
    addEventListener(type, handler){this.handlers[type]=handler},
    append(){},showModal(){},close(){},click(){}
  });
  return elements.get(id);
}
element('chunk').value='8';
const source={id:'source1',path:'G:\\Datasets'},target={id:'folder1',name:'Datasets'};
const boot={csrf:'test',sources:[source],last_source:source,target,concurrency:1,jobs:[],events:[],auth:{connected:true,email:'test@example.com'}};
const context={
  console,URLSearchParams,setTimeout:()=>0,clearTimeout(){},setInterval(){},
  localStorage:{getItem(){return null},setItem(){},removeItem(){}},
  location:{protocol:'http:',host:'127.0.0.1:8787'},
  window:{driveTheme:{getPreference(){return 'system'},setPreference(){}},addEventListener(){}},
  document:{getElementById:element,querySelectorAll(){return []},querySelector(){return null},createElement(){return element('row')}},
  WebSocket:class {close(){}},
  async fetch(path, options){
    const body=options.body?JSON.parse(options.body):null;requests.push({path,body});
    const data=path==='/api/bootstrap'?boot:path==='/api/auth/status'?boot.auth:path==='/api/jobs'?boot:path==='/api/sources/resolve'?{files:[]}:{};
    return {ok:true,json:async()=>data};
  }
};
vm.createContext(context);
vm.runInContext(fs.readFileSync('static/app.js','utf8'),context);
const settle=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
  await settle();requests.length=0;
  let active=true;
  const dropEvent={dataTransfer:{items:[
    {kind:'file',webkitGetAsEntry(){assert.ok(active);return {isDirectory:true,name:'COCO'}},getAsFile(){return null}},
    {kind:'file',webkitGetAsEntry(){assert.ok(active);return {isDirectory:true,name:'LIS'}},getAsFile(){return null}},
    {kind:'file',webkitGetAsEntry(){return {isDirectory:false}},getAsFile(){return {name:'annotations.zip',size:123,lastModified:1}}}
  ],files:[]}};
  const dropped=element('dropzone').handlers.drop(dropEvent);active=false;await dropped;
  const directories=requests.filter(r=>r.path==='/api/folder-jobs');
  assert.deepEqual(directories.map(r=>r.body.name),['COCO','LIS']);
  assert.ok(directories.every(r=>r.body.source_id==='source1'&&r.body.folder_id==='folder1'));
  assert.deepEqual(requests.find(r=>r.path==='/api/sources/resolve').body.files.map(f=>f.name),['annotations.zip']);
  requests.length=0;
  const input={value:'selected',files:Array.from({length:120},(_,i)=>({name:`${i}.txt`,size:i,webkitRelativePath:`Folder/sub/${i}.txt`}))};
  element('directoryInput').onchange({target:input});await settle();await settle();
  const picked=requests.find(r=>r.path==='/api/folder-jobs').body;
  assert.equal(picked.name,'Folder');assert.equal(picked.samples.length,64);
  assert.deepEqual(picked.samples[0],{relative:'sub/0.txt',size:0});assert.equal(input.value,'');
  console.log('2 frontend folder tests passed: synchronous drop capture, mixed/multiple folders, directory picker metadata.');
})().catch(error=>{console.error(error);process.exitCode=1});
