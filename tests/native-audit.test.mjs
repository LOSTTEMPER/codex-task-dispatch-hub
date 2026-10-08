import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
const run = new Function('tools','operation','payload','hubPath',readFileSync(new URL('../native_call.js',import.meta.url),'utf8'));
const wrap = data => ({ content: [{type:'text',text:JSON.stringify(data)}] });
const complete = result => ({exit_code:0,output:JSON.stringify({ok:true,result})});
function fixture({idle=false, missing=false, unrelated=false, asyncClaim=false, lostClaim=false, corrupt=false, authoritative=false}={}) {
  let sent=false, polls=0;const calls=[],receipts=[];
  const tools={
    async exec_command({cmd}) {
      const op=cmd.match(/ call '([^']+)'/)[1];const args=JSON.parse(cmd.match(/ --json '(.*)'$/)[1]);calls.push(op);
      if(op==='delivery_candidates')return complete({calling_thread_id:'manager',deliveries:[{delivery_id:'d',thread_id:'target'}]});
      if(op==='delivery_claim_native') {
        if(lostClaim)return {session_id:31,output:''};
        if(asyncClaim)return {session_id:31,output:'{"ok":'};
        return complete({thread_id:'target',message:'exact registered message'});
      }
      if(op==='delivery_receipt_native'){receipts.push(args);return complete({state:'delivered'});}
      if(op==='delivery_link_native_prepare')return complete({thread_id:'target',message:'exact registered message',token:'ticket'});
      if(op==='delivery_link_native_commit'){receipts.push(args);return complete({turn_id:args.proof.turn_id,state:'delivered'});}
      return complete({});
    },
    async write_stdin({session_id}) {
      assert.equal(session_id,31);calls.push('wait-command');
      if(corrupt)return {exit_code:0,output:'TRUNCATED'};
      return {exit_code:0,output:'true,"result":{"thread_id":"target","message":"exact registered message"}}'};
    },
    async mcp__codex_app__wait_threads() {
      polls++;
      if(sent&&missing)return wrap({polls:[]});
      return wrap({polls:[{thread:{id:'target',status:{type:sent&&!idle?'active':'idle'}},latestTurn:{id:sent?'new':'old',status:sent&&!idle?'inProgress':'completed'}}]});
    },
    async mcp__codex_app__read_thread() {
      return wrap({thread:{id:'target'},page:{hasMore:false},turns:[{id:unrelated?'unrelated':'verified',status:idle?'completed':'inProgress',items:[{type:'userMessage',content:[{type:'text',text:unrelated?'a different user request':'exact registered message'}]}]}]});
    },
    async mcp__codex_app__send_message_to_thread(args) {
      assert.equal(args.prompt,'exact registered message');calls.push('send');sent=true;
      return wrap({threadId:'target',...(authoritative?{turnId:'authoritative'}:{})});
    }
  };
  if(lostClaim)delete tools.write_stdin;
  return {tools,calls,receipts};
}
for(const mode of [{idle:true},{missing:true}])test('F06 completed or missing snapshot associates exact sent user message '+JSON.stringify(mode),async()=>{
  const f=fixture(mode);await run(f.tools,'drain',{});
  assert.equal(f.receipts[0].confirmed,true);assert.equal(f.receipts.at(-1).turn_id,'verified');assert.equal(f.calls.filter(x=>x==='send').length,1);
});
test('F06 unrelated newest turn is never used as receipt',async()=>{
  const f=fixture({unrelated:true});const r=await run(f.tools,'drain',{});
  assert.equal(f.receipts[0].turn_id,undefined);assert.equal(r.delivery[0].needs_turn_link,true);
});
test('F06 authoritative send response supplies turn even without snapshot',async()=>{
  const f=fixture({missing:true,authoritative:true});await run(f.tools,'drain',{});
  assert.equal(f.receipts[0].turn_id,'authoritative');
});
test('F06 explicit missing-ID recovery only links exact native input and never resends',async()=>{
  const f=fixture({idle:true});const r=await run(f.tools,'delivery_link_native',{delivery_id:'d',expected_thread_id:'target'});
  assert.equal(r.result.turn_id,'verified');assert(!f.calls.includes('send'));
});
test('F06 unrelated recovery history remains unresolved',async()=>{
  const f=fixture({unrelated:true});const r=await run(f.tools,'delivery_link_native',{delivery_id:'d',expected_thread_id:'target'});
  assert.equal(r.result.linked,false);assert.equal(f.receipts.length,0);
});
test('F07 asynchronous ledger claim waits for terminal JSON before sending once',async()=>{
  const f=fixture({asyncClaim:true});await run(f.tools,'drain',{});
  assert.deepEqual(f.calls.slice(0,4),['delivery_candidates','delivery_claim_native','wait-command','send']);
  assert.equal(f.calls.filter(x=>x==='send').length,1);
});
for(const mode of [{lostClaim:true},{asyncClaim:true,corrupt:true}])test('F07 ambiguous claim is uncertain, never pending or resent '+JSON.stringify(mode),async()=>{
  const f=fixture(mode);const r=await run(f.tools,'drain',{});
  assert.equal(r.delivery[0].state,'uncertain');assert(!f.calls.includes('send'));
});
test('F07 nonzero exit rejects valid-looking JSON',async()=>{
  const tools={exec_command:async()=>({exit_code:1,output:'{"ok":true,"result":{}}'})};
  await assert.rejects(run(tools,'status',{}));
});
