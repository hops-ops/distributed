import assert from 'node:assert/strict';
import {once} from 'node:events';
import WebSocket from 'ws';
import {startRuntime} from './runtime.mjs';
import {startShardedRuntime} from './sharded-runtime.mjs';
const apiOrigin=process.env.GATEWAY_ORIGIN;
let runtime=await startRuntime({apiOrigin,artifact:'live-workerd.log'});
const clients=[];
async function until(predicate,label='condition'){for(let n=0;n<500;n++){if(await predicate())return;await new Promise(resolve=>setTimeout(resolve,20));}throw Error(label+' not reached');}
async function metrics(){return await(await fetch(apiOrigin+'/__metrics')).json();}
async function counts(){const all=await(await fetch(runtime.publicOrigin+'/__coordinators')).json();return all.reduce((a,c)=>a.map((n,i)=>n+c.live[i]),[0,0,0,0,0,0,0]);}
async function connect(id,token,{resume,ack=true,legacy=false,ingress=0}={}){
  const socket=new WebSocket((runtime.at?runtime.at(ingress,'/graphql/ws'):runtime.publicOrigin+'/graphql/ws').replace('http:','ws:'),legacy?'graphql-ws':'graphql-transport-ws');
  const frames=[];const errors=[];let initialized=false;let closed=false;
  socket.on('message',data=>{const value=JSON.parse(data);if(value.type==='connection_ack')initialized=true;else if(value.type==='ping'&&ack)socket.send(JSON.stringify({type:'pong',payload:value.payload}));else if(value.type==='next'||value.type==='data'){assert.equal(value.id,id);frames.push(value.payload);}else if(value.type==='error')errors.push(value.payload);});
  socket.on('close',()=>{closed=true;});socket.on('error',error=>{errors.push(error.message);});
  clients.push(socket);await once(socket,'open');socket.send(JSON.stringify({type:'connection_init',payload:{authorization:`Bearer ${token}`}}));
  await until(()=>initialized,`connection ${id} admission`);
  const payload={query:'subscription WorkerWatch { causal_query_views { title } }',...(resume?{extensions:{distributed:{resume:{cursors:resume}}}}:{})};
  socket.send(JSON.stringify({id,type:legacy?'start':'subscribe',payload}));
  await until(()=>frames.length||errors.length||closed,`connection ${id} first frame`);
  assert.equal(errors.length,0,JSON.stringify(errors));assert.ok(frames.length,`connection ${id} closed without frame`);
  return {socket,frames,errors,closed:()=>closed,cancel:()=>socket.send(JSON.stringify({id,type:legacy?'stop':'complete'}))};
}
try{
  const alice=process.env.GATEWAY_TOKEN_ALICE,bob=process.env.GATEWAY_TOKEN_BOB;
  const group=[];for(let i=0;i<100;i++)group.push(await connect(`consumer-${i}`,alice));
  await until(async()=>{const c=await counts();return c[0]===1&&c[1]===100;},'100 consumers sharing one group');
  assert.equal((await metrics()).producers,1);assert.equal((await metrics()).resultExecutions,1);assert.equal((await metrics()).validations,100);
  const initial=group[0].frames[0];for(const consumer of group)assert.deepEqual(consumer.frames[0],initial);
  const other=await connect('bob',bob);assert.notEqual(other.frames[0].extensions.distributed.cacheScope,initial.extensions.distributed.cacheScope);assert.equal((await metrics()).producers,2);other.cancel();
  await until(async()=>(await metrics()).producers===1,'Bob cancellation');
  await fetch(apiOrigin+'/__commit',{method:'POST'});
  await until(()=>group.every(c=>c.frames.some(f=>f.data?.causal_query_views?.[0]?.title==='worker committed')),'100 committed fanout');
  const resumed=await connect('resumed',alice,{resume:initial.extensions.distributed.live.cursors});
  await until(async()=>(await metrics()).producers===1,'safe replay handoff');assert.ok((await counts())[6]>0);
  for(const consumer of group)consumer.cancel();await until(async()=>(await counts())[1]===1,'surviving resume consumer');
  assert.equal((await metrics()).producers,1);resumed.cancel();await until(async()=>(await metrics()).producers===0,'actual last-leave teardown');
  console.log('PASS actual workerd DO: 100 JWT-admitted WebSockets, one producer, subject isolation, commit fanout, safe resume and last-leave teardown');
  const legacy=await connect('legacy',alice,{legacy:true});legacy.cancel();await until(async()=>(await metrics()).producers===0,'legacy independent teardown');
  console.log('PASS legacy protocol remains independently admitted');
  const fast=await connect('fast',alice);const slow=await connect('slow',alice,{ack:false});
  for(let position=3;position<=23;position++){
    const previous=fast.frames.length;await fetch(apiOrigin+'/__next/'+position,{method:'POST'});
    await until(()=>fast.frames.length>previous,'proof-bearing frame '+position);
  }
  await until(()=>slow.closed()||slow.errors.length,'bounded slow consumer reset');
  assert.equal(slow.frames.length,1,'unacknowledged consumer cannot accumulate unbounded network frames');
  await until(async()=>(await counts())[1]===1,'slow consumer release');
  assert.equal((await metrics()).producers,1);
  assert.deepEqual(fast.frames[1].data,fast.frames[2].data,'same values across projection commits');
  assert.notDeepEqual(fast.frames[1].extensions.distributed,fast.frames[2].extensions.distributed,'new confirmation proof is delivered');
  fast.cancel();await until(async()=>(await metrics()).producers===0,'fast teardown');
  console.log('PASS slow consumer explicit reset; proof-only updates reach healthy consumer');
  const gap=await connect('gap',alice,{resume:initial.extensions.distributed.live.cursors});
  assert.equal(gap.frames[0].extensions.distributed.live.reset,true,'expired replay retention requires origin reset');
  gap.cancel();await until(async()=>(await metrics()).producers===0,'gap teardown');
  const shortToken=(await(await fetch(apiOrigin+'/__short_token')).json()).token;
  const expiring=await connect('expiring',shortToken);const survivor=await connect('survivor',alice);
  const attempts=(await counts())[2];
  await until(()=>expiring.errors.length||expiring.closed(),'per-consumer expiry');
  await until(async()=>(await counts())[2]>attempts,'remaining credential reconnect');
  await until(async()=>(await metrics()).producers===1,'remaining live producer');
  const previous=survivor.frames.length;await fetch(apiOrigin+'/__next/24',{method:'POST'});await until(()=>survivor.frames.length>previous,'post-expiry survivor update');
  survivor.cancel();await until(async()=>(await metrics()).producers===0,'expiry teardown');
  console.log('PASS replay gap reset, per-consumer expiry and remaining-credential upstream reconnect');

  await runtime.stop();runtime=await startShardedRuntime(apiOrigin);
  const baseline=await metrics();const sharded=[];
  for(let i=0;i<100;i++)sharded.push(await connect('sharded-'+i,alice,{ingress:i}));
  assert.equal((await metrics()).producers,1);assert.equal((await metrics()).resultExecutions,baseline.resultExecutions+1);
  assert.equal((await metrics()).validations,baseline.validations+100);
  await fetch(apiOrigin+'/__next/25',{method:'POST'});
  await until(()=>sharded.every(c=>c.frames.length>1),'cross-isolate fanout');
  const resume=sharded[0].frames.at(-1).extensions.distributed.live.cursors;
  await runtime.stop();await until(async()=>(await metrics()).producers===0,'runtime restart releases origin producer');
  runtime=await startShardedRuntime(apiOrigin);
  const recovered=await connect('recovered',alice,{resume,ingress:1});
  await fetch(apiOrigin+'/__next/26',{method:'POST'});await until(()=>recovered.frames.length>1,'fresh producer after restart');
  recovered.cancel();await until(async()=>(await metrics()).producers===0,'restarted producer teardown');
  console.log('PASS two ingress isolates share100live consumers; actual coordinator restart resumes from origin and tears down');

}finally{for(const socket of clients)socket.terminate();await runtime.stop();}
