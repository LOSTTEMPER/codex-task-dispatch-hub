import assert from 'node:assert/strict';
import {readFileSync, mkdtempSync, cpSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {execFileSync, exec as execCallback} from 'node:child_process';
import {promisify} from 'node:util';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';
const exec = promisify(execCallback);
const source = resolve(fileURLToPath(new URL('..', import.meta.url)));
const run = new Function('tools','operation','payload','hubPath',readFileSync(join(source,'native_call.js'),'utf8'));
const wrap = data => ({content:[{type:'text',text:JSON.stringify(data)}]});
const complete = result => ({exit_code:0,output:JSON.stringify({ok:true,result})});

test('R02 real CLI large Unicode message survives lost claim and truncated chunk, one claim/send', async()=>{
  const root=mkdtempSync(join(tmpdir(),'hub-r02-'));
  for(const file of ['hub.py','budget.py','usage.py','dispatch_extensions.py','projections.py']) cpSync(join(source,file),join(root,file));
  try {
    execFileSync('python3',['-c',`import sys
sys.path.insert(0,sys.argv[1])
from hub import Hub,dump,now
h=Hub(sys.argv[1])
with h.transaction():
 for role in ('manager','a'): h.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',(role,'thread-'+role,role,'{}',now()))
h.version_create('manager',dict(id='v1',title='test',goal='test',tasks={}))
h.begin('manager',{})
h.request('manager',dict(to='a',kind='wait',urgent=False,important=True,reason='测'*300,title='large',action='字'*2000,acceptance='好'*1200,refs=[dict(path='资料/'+str(i)+'/'+'界'*980,revision='版'*80) for i in range(12)],idempotency_key='large'))
h.close()`,root],{env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'}});
    let sent=0, claims=0, lost=false, truncated=false, wanted;
    const tools={
      async exec_command({cmd}) {
        const {stdout}=await exec(cmd,{cwd:root,env:{...process.env,CODEX_THREAD_ID:'thread-manager'},maxBuffer:1000000});
        assert(stdout.length<6500,'each ledger reply must stay bounded');
        if(cmd.includes("call 'delivery_claim_native'")) {claims++;if(!lost){lost=true;return {exit_code:0,output:'TRUNCATED'};}}
        if(cmd.includes("call 'delivery_message_native'")&&!truncated) {truncated=true;return {exit_code:0,output:'TRUNCATED'};}
        return {exit_code:0,output:stdout};
      },
      async mcp__codex_app__wait_threads(){return wrap({polls:[{thread:{id:'thread-a',status:{type:'idle'}}}]});},
      async mcp__codex_app__send_message_to_thread({prompt}) {
        sent++; wanted=prompt; assert(prompt.length>16000); assert(prompt.includes('字'.repeat(2000)));
        return wrap({threadId:'thread-a',turnId:'actual-turn'});
      }
    };
    const result=await run(tools,'drain',{},join(root,'hub.py'));
    assert.equal(sent,1);assert.equal(claims,1);assert.equal(result.delivery[0].turn_id,'actual-turn');
    const verification=JSON.parse(execFileSync('python3',['-c',`import sys,json
sys.path.insert(0,sys.argv[1])
from hub import Hub
h=Hub(sys.argv[1]);r=h.db.execute('SELECT * FROM outbox').fetchone()
print(json.dumps(dict(state=r['state'],attempts=r['attempts'],message=h.meta('native_message:'+r['id']))));h.close()`,root],{env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8'}));
    assert.equal(verification.state,'delivered');assert.equal(verification.attempts,1);assert.equal(verification.message,wanted);
  } finally {rmSync(root,{recursive:true,force:true});}
});

test('R03 authoritative receipt is saved before throwing optional status/history', async()=>{
  let sent=false;const events=[];
  const tools={
    async exec_command({cmd}){
      const op=cmd.match(/ call '([^']+)'/)[1];events.push(op);
      if(op==='delivery_candidates')return complete({calling_thread_id:'manager',deliveries:[{delivery_id:'d',thread_id:'a'}]});
      if(op==='delivery_claim_native')return complete({thread_id:'a',message:'exact'});
      if(op==='delivery_receipt_native') {assert(cmd.includes('"confirmed":true'));assert(cmd.includes('authoritative'));return complete({state:'delivered'});}
      throw Error('unexpected '+op);
    },
    async mcp__codex_app__wait_threads(){if(sent){events.push('throw-status');throw Error('status failed');}return wrap({polls:[{thread:{id:'a',status:{type:'idle'}}}]});},
    async mcp__codex_app__read_thread(){throw Error('optional history failed');},
    async mcp__codex_app__send_message_to_thread(){sent=true;events.push('send');return wrap({threadId:'a',turnId:'authoritative'});}
  };
  const result=await run(tools,'drain',{});
  assert.equal(result.delivery[0].state,'delivered');assert.equal(result.delivery[0].turn_id,'authoritative');
  assert(events.indexOf('delivery_receipt_native')<events.indexOf('throw-status'));
});

for(const duplicate of [false,true])test('R03 full long message lookup and cross-page ambiguity '+duplicate,async()=>{
  const message='汉'.repeat(17500);let page=0,committed=false;
  const tools={
    async exec_command({cmd}){
      if(cmd.includes("call 'delivery_link_native_prepare'"))return complete({thread_id:'a',message,token:'ticket'});
      if(cmd.includes("call 'delivery_link_native_commit'")){committed=true;return complete({turn_id:'exact'});}
      throw Error('unexpected');
    },
    async mcp__codex_app__read_thread(args){
      assert(args.maxOutputCharsPerItem>=message.length);page++;
      return wrap({thread:{id:'a'},page:{hasMore:page===1,nextCursor:'next'},turns:page===1||duplicate?[{id:page===1?'exact':'other',items:[{type:'userMessage',content:[{type:'text',text:message.slice(0,args.maxOutputCharsPerItem)}]}]}]:[]});
    }
  };
  if(duplicate){await assert.rejects(run(tools,'delivery_link_native',{delivery_id:'d',expected_thread_id:'a'}));assert(!committed);}
  else {const result=await run(tools,'delivery_link_native',{delivery_id:'d',expected_thread_id:'a'});assert.equal(result.result.turn_id,'exact');assert.equal(page,2);}
});
