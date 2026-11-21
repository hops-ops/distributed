import assert from 'node:assert/strict';
import http from 'node:http';
import {startRuntime} from './runtime.mjs';
import {startShardedRuntime} from './sharded-runtime.mjs';
const apiOrigin=process.env.GATEWAY_ORIGIN;
assert.ok(apiOrigin?.startsWith('http://127.0.0.1:'),'isolated origin required');
let runtime=await startRuntime({apiOrigin});
const body=JSON.stringify({query:'query WorkerShared { causal_query_views { title } }'});
const query=async()=>{const response=await fetch(runtime.publicOrigin+'/graphql',{method:'POST',headers:{'content-type':'application/json'},body});assert.equal(response.status,200,await response.clone().text());const value=await response.json();assert.equal(value.errors,undefined,JSON.stringify(value));return value;};
const metrics=async()=>await(await fetch(apiOrigin+'/__metrics')).json();
async function until(predicate){for(let n=0;n<300;n++){if(await predicate())return;await new Promise(resolve=>setTimeout(resolve,20));}throw Error('coordinator condition not reached');}
try{
  const pending=Promise.all(Array.from({length:100},()=>query()));
  pending.catch(()=>{});
  await until(async()=>{const counts=await(await fetch(runtime.publicOrigin+'/__coordinators')).json();return counts.reduce((n,c)=>n+c.query[2],0)===100;});
  assert.deepEqual(await metrics(),{validations:100,resultExecutions:0});
  await fetch(apiOrigin+'/__release',{method:'POST'});
  const responses=await pending;for(const response of responses)assert.deepEqual(response,responses[0]);
  assert.equal((await metrics()).resultExecutions,1);
  const before=await metrics();await query();const hit=await metrics();assert.equal(hit.resultExecutions,before.resultExecutions);assert.equal(hit.validations,before.validations+1);
  console.log('PASS actual workerd DO: 100 admitted queries, one actual origin SQL execution, current private hit');
  await fetch(apiOrigin+'/__write',{method:'POST'});
  assert.equal((await query()).data.causal_query_views[0].title,'external write');
  assert.equal((await metrics()).resultExecutions,2);
  console.log('PASS missed feed/external SQL write invalidates through current origin validation');
  await runtime.stop();runtime=await startRuntime({apiOrigin,artifact:'query-restarted-workerd.log'});
  assert.equal((await query()).data.causal_query_views[0].title,'external write');
  assert.equal((await metrics()).resultExecutions,3);
  console.log('PASS actual workerd restart loses cache and revalidates/refills');
  await runtime.stop();runtime=await startShardedRuntime(apiOrigin);
  assert.equal(await(await fetch(runtime.at(0,'/__gateway_health'))).text(),'ingress-a');
  assert.equal(await(await fetch(runtime.at(1,'/__gateway_health'))).text(),'ingress-b');
  await fetch(apiOrigin+'/__block',{method:'POST'});const beforeShard=await metrics();
  const shared=Promise.all(Array.from({length:100},async(_,index)=>{const response=await fetch(runtime.at(index,'/graphql'),{method:'POST',headers:{'content-type':'application/json'},body});assert.equal(response.status,200);return response.json();}));shared.catch(()=>{});
  await until(async()=>{const counts=await(await fetch(runtime.at(0,'/__coordinators'))).json();return counts.reduce((n,c)=>n+c.query[2],0)===100;});
  assert.equal((await metrics()).validations,beforeShard.validations+100);
  await fetch(apiOrigin+'/__release',{method:'POST'});const shardResults=await shared;for(const result of shardResults)assert.deepEqual(result,shardResults[0]);
  assert.equal((await metrics()).resultExecutions,beforeShard.resultExecutions+1);
  console.log('PASS two distinct ingress Wasm isolates coordinate100queries in one selected Durable Object');

  await fetch(runtime.at(0,'/__coordinators'),{method:'POST'});
  await fetch(apiOrigin+'/__block',{method:'POST'});
  const controllers=[new AbortController(),new AbortController()];
  const cancellable=controllers.map((controller,index)=>new Promise(resolve=>{const request=http.request(runtime.at(index,'/graphql'),{method:'POST',agent:false,headers:{'content-type':'application/json',...(index===0?{'x-fixture-cancel':'yes'}:{})},signal:controller.signal},response=>{let data='';response.on('data',chunk=>data+=chunk);response.on('end',()=>resolve(JSON.parse(data)));});request.on('error',error=>resolve(error.name));request.end(body);}));
  const consumers=async()=>{const all=await(await fetch(runtime.at(0,'/__coordinators'))).json();return all.reduce((n,c)=>n+c.query[2],0);};
  await until(async()=>await consumers()===2);controllers[0].abort();
  await until(async()=>await consumers()===1);
  await fetch(apiOrigin+'/__release',{method:'POST'});assert.equal(await cancellable[0],'AbortError');assert.ok((await cancellable[1]).data);
  console.log('PASS ingress AbortSignal releases one DO consumer while another completes');
  await fetch(runtime.at(0,'/__coordinators'),{method:'POST'});await fetch(apiOrigin+'/__block',{method:'POST'});
  const last=fetch(runtime.at(0,'/graphql'),{method:'POST',headers:{'content-type':'application/json','x-fixture-cancel':'yes'},body}).catch(()=>null);
  await until(async()=>await consumers()===1);await until(async()=>await consumers()===0);await last;
  console.log('PASS last ingress cancellation releases the actual DO flight');

}finally{await runtime.stop();}
