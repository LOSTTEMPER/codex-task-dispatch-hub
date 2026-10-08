const fs = require('node:fs');
const assert = require('node:assert/strict');
const run = new Function('tools','operation','payload','hubPath',fs.readFileSync(require('node:path').join(__dirname,'../native_call.js'),'utf8'));
const result = x => ({content:[{type:'text',text:JSON.stringify(x)}]});
async function scenario(initial, failAfterSend=false, operation='request', archivedOnClaim=false) {
  const calls=[];let sent=false;
  const tools={
    exec_command:async ({cmd}) => {
      const op = / call '([^']+)'/.exec(cmd)[1];calls.push(op);
      let value={};
      if(op==='request')value={request_id:'r'};
      if(op==='delivery_candidates')value={calling_thread_id:'sender',deliveries:[{delivery_id:'d',thread_id:'target'}]};
      if(op==='delivery_claim_native')value=archivedOnClaim?{delivery_id:'d',state:'archived',skipped:true}:{thread_id:'target',message:'exact queued message'};
      if(op==='delivery_receipt_native')value={state:cmd.includes('false')?'uncertain':'delivered'};
      return {output:JSON.stringify({ok:true,result:value}),exit_code:0};
    },
    mcp__codex_app__wait_threads:async () => {
      if(sent && failAfterSend)throw Error('unknown receipt');
      return result({polls:[{thread:{id:'target',status:{type:sent?'active':initial}},latestTurn:{id:sent?'new':'old'}}]});
    },
    mcp__codex_app__send_message_to_thread:async args => {
      assert.deepEqual(args,{threadId:'target',prompt:'exact queued message'});calls.push('native-send');sent=true;return result({threadId:'target'});
    }
  };
  return {output:await run(tools,operation,{title:'safe'},"/tmp/example team's hub/hub.py"),calls};
}
(async()=>{
 const cold=await scenario('notLoaded');assert.equal(cold.output.delivery[0].state,'active');assert.equal(cold.calls.filter(x=>x==='native-send').length,1);
 const busy=await scenario('active');assert(!busy.calls.includes('delivery_claim_native'));assert(!busy.calls.includes('native-send'));
 const unknown=await scenario('idle',true);assert.equal(unknown.output.delivery[0].state,'delivered');assert.equal(unknown.calls.filter(x=>x==='native-send').length,1);
 const read=await scenario('idle',false,'status');assert.deepEqual(read.calls,['status']);
 const returned=await scenario('notLoaded',false,'end');assert.equal(returned.calls[0],'end');assert.equal(returned.output.delivery[0].state,'active');
 const obsolete=await scenario('idle',false,'end',true);assert.equal(obsolete.output.delivery[0].state,'archived');assert(!obsolete.calls.includes('native-send'));assert(!obsolete.calls.includes('delivery_receipt_native'));
 console.log('PASS 6 native bridge scenarios: cold wake, busy defer, confirmed receipt survives status failure, read-only, result wake, archived result stays silent');
})().catch(e=>{console.error(e);process.exitCode=1;});
