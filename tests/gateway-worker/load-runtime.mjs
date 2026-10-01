import assert from 'node:assert/strict';
import http from 'node:http';
import {once} from 'node:events';
import {mkdir,writeFile} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import WebSocket,{WebSocketServer} from 'ws';
import {startRuntime,root} from './runtime.mjs';

const origin=process.env.GATEWAY_ORIGIN;
const tokens={alice:process.env.GATEWAY_TOKEN_ALICE,bob:process.env.GATEWAY_TOKEN_BOB};
const native=JSON.parse(process.env.GATEWAY_NATIVE_MODES);
const meterOrigin=`http://127.0.0.1:${process.env.GATEWAY_METER_PORT}`;
// Count actual application bytes at the origin boundary, including control
// responses. HTTP/TLS framing and kernel/socket memory are not estimated here.
const wire={httpRequestBytes:0,httpResponseBytes:0,wsRequestBytes:0,wsResponseBytes:0,wsConnections:0};
const meter=http.createServer((request,response)=>{
  const upstream=http.request(origin+request.url,{method:request.method,headers:{...request.headers,host:new URL(origin).host}},reply=>{
    response.writeHead(reply.statusCode,reply.headers);
    reply.on('data',chunk=>wire.httpResponseBytes+=chunk.length);reply.pipe(response);
  });
  upstream.on('error',()=>{response.writeHead(502);response.end();});
  request.on('data',chunk=>wire.httpRequestBytes+=chunk.length);request.pipe(upstream);
  response.on('close',()=>{if(!response.writableFinished)upstream.destroy();});
});
const wss=new WebSocketServer({noServer:true});
meter.on('upgrade',(request,socket,head)=>{
  const protocol=request.headers['sec-websocket-protocol'];
  const upstream=new WebSocket((origin+request.url).replace('http:','ws:'),protocol,{headers:{origin:request.headers.origin??meterOrigin}});
  upstream.on('error',()=>socket.destroy());
  upstream.once('open',()=>wss.handleUpgrade(request,socket,head,downstream=>{
    wire.wsConnections++;
    downstream.on('message',(data,binary)=>{wire.wsRequestBytes+=data.length;if(upstream.readyState===WebSocket.OPEN)upstream.send(data,{binary});});
    upstream.on('message',(data,binary)=>{wire.wsResponseBytes+=data.length;if(downstream.readyState===WebSocket.OPEN)downstream.send(data,{binary});});
    downstream.on('close',()=>upstream.close());upstream.on('close',()=>downstream.close());
    downstream.on('error',()=>upstream.close());
  }));
});
meter.listen(Number(process.env.GATEWAY_METER_PORT),'127.0.0.1');await once(meter,'listening');
const report={version:1,revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),candidateChanges:execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim().length>0,node:process.version,platform:process.platform,participants:100,repetitions:1,payloadCharacters:4096,measurement:'actual HTTP bodies and WebSocket application frames; excludes headers/TLS/framing',limits:{snapshotEntries:128,snapshotBytes:2097152,entryBytes:262144,flightGroups:8,consumers:128,flightBytes:262144,liveGroups:4,liveFrameBytes:65536},rows:[]};
let position=2;
const snapshots=()=>({...wire});
const difference=(after,before)=>Object.fromEntries(Object.keys(after).map(key=>[key,after[key]-before[key]]));
async function metrics(){return (await fetch(origin+'/__metrics')).json();}
async function until(predicate,label){for(let n=0;n<1000;n++){if(await predicate())return;await new Promise(resolve=>setTimeout(resolve,10));}throw Error(label+' timed out');}
const latency=values=>{const sorted=[...values].sort((a,b)=>a-b);return {min:sorted[0],p50:sorted[Math.floor(sorted.length/2)],p95:sorted[Math.floor(sorted.length*.95)],max:sorted.at(-1)};};
async function advance(){position++;await fetch(origin+'/__next/'+position,{method:'POST'});return 'load-'+position+'-';}
async function modeRun(host,mode,runtime){
  console.log('START '+host+'/'+mode);
  const url=runtime.publicOrigin;
  const counts=async()=> (await fetch(url+'/__coordinators')).json();
  let downstreamBytes=0;
  const query=async(subject='alice',document='query Load { causal_query_views { title } }')=>{
    const started=performance.now();
    const body=JSON.stringify({query:document,extensions:{gatewayDelivery:{action:'execute',connectionInit:{authorization:'Bearer '+tokens[subject]}}}});
    const response=await fetch(url+'/graphql',{method:'POST',headers:{'content-type':'application/json'},body});
    const text=await response.text();downstreamBytes+=Buffer.byteLength(text);
    assert.equal(response.status,200,'query status');const value=JSON.parse(text);assert.equal(value.errors,undefined,'query errors');
    return {value,ms:performance.now()-started};
  };
  await advance();
  let warmup;
  if(mode==='snapshots'){
    const before=await metrics(),bytes=snapshots();await query();warmup={origin:difference(await metrics(),before),wire:difference(snapshots(),bytes),clientBytes:downstreamBytes};
  }
  const before=await metrics(),bytes=snapshots(),clientBefore=downstreamBytes;
  if(mode==='flights')await fetch(origin+'/__block',{method:'POST'});
  const pending=Promise.all(Array.from({length:100},()=>query()));pending.catch(()=>{});
  if(mode==='flights'){
    await until(async()=> (await counts()).reduce((sum,row)=>sum+row.query[2],0)===100,'100 flight consumers');
    assert.equal((await metrics()).resultExecutions,before.resultExecutions,'barrier holds actual SQL');
    await fetch(origin+'/__release',{method:'POST'});
  }
  const result=await pending;
  for(const item of result)assert.deepEqual(item.value,result[0].value);
  const queryWork=difference(await metrics(),before);
  assert.equal(queryWork.resultExecutions,mode==='snapshots'?0:mode==='flights'?1:100);
  assert.equal(queryWork.validations,mode==='snapshots'?100:mode==='flights'?101:0,'unselected query optimizations perform no validation work');
  const queryReport={origin:queryWork,wire:difference(snapshots(),bytes),clientResponseBytes:downstreamBytes-clientBefore,latencyMs:latency(result.map(item=>item.ms)),warmup};
  const alice=result[0].value.extensions.distributed.cacheScope;
  const control=await query('bob');assert.notEqual(control.value.extensions.distributed.cacheScope,alice,'subjects stay separate');
  const title=await advance();assert.ok((await query()).value.data.causal_query_views[0].title.startsWith(title),'external projection invalidates cache');
  const decisions=await counts();
  if(mode==='snapshots'){
    assert.ok(decisions.reduce((sum,row)=>sum+(row.metrics[0]?.hits??0),0)>=100);
    assert.ok(decisions.reduce((sum,row)=>sum+(row.metrics[0]?.staleRejections??0),0)>0);
  }
  // Unsupported protocol introspection follows the ordinary origin path and
  // remains distinct from a reusable query hit.
  const bypassBefore=decisions.reduce((sum,row)=>sum+row.metrics[1],0);
  if(mode==='snapshots'||mode==='flights'){
    await query('alice','query Introspection { __typename }');
    assert.ok((await counts()).reduce((sum,row)=>sum+row.metrics[1],0)>bypassBefore);
  }
  await fetch(url+'/__coordinators',{method:'POST'});
  const liveBefore=await metrics(),liveBytes=snapshots();let receivedBytes=0;
  const clients=[];
  async function connect(id,subject='alice',resume){
    const start=performance.now();const socket=new WebSocket(url.replace('http:','ws:')+'/graphql/ws','graphql-transport-ws');
    const frames=[];let initialized=false,closed=false;const errors=[];socket.on('close',()=>closed=true);
    socket.on('message',data=>{
      receivedBytes+=data.length;const frame=JSON.parse(data);
      if(frame.type==='connection_ack')initialized=true;
      if(frame.type==='ping'&&id!=='slow')socket.send(JSON.stringify({type:'pong',payload:frame.payload}));
      if(frame.type==='next'){assert.equal(frame.id,id);frames.push(frame.payload);}
      if(frame.type==='error')errors.push(frame.payload);
    });
    socket.on('error',()=>errors.push('socket failure'));clients.push(socket);
    await once(socket,'open');socket.send(JSON.stringify({type:'connection_init',payload:{authorization:'Bearer '+tokens[subject]}}));
    await until(()=>initialized||errors.length,'socket admission');assert.equal(errors.length,0);
    socket.send(JSON.stringify({id,type:'subscribe',payload:{query:'subscription LoadLive { causal_query_views { title } }',...(resume?{extensions:{distributed:{resume:{cursors:resume}}}}:{})}}));
    await until(()=>frames.length||errors.length,'live first frame');assert.equal(errors.length,0);assert.equal(frames[0].errors,undefined);
    return {frames,errors,closed:()=>closed,ms:performance.now()-start,cancel:()=>socket.send(JSON.stringify({id,type:'complete'}))};
  }
  try{
    const consumers=[];
    // Sequential admission makes steady-state producer count deterministic;
    // all100 remain concurrently subscribed during the measured commit fanout.
    for(let index=0;index<100;index++)consumers.push(await connect(String(index)));
    const producerCount=(await metrics()).producers;
    assert.equal(producerCount,mode==='live'?1:100);
    const initial=consumers[0].frames[0];for(const consumer of consumers)assert.deepEqual(consumer.frames[0],initial);
    const commitStart=performance.now(),title=await advance();
    await until(()=>consumers.every(consumer=>consumer.frames.some(frame=>frame.data?.causal_query_views?.[0]?.title.startsWith(title))),'100 commit deliveries');
    const fanoutMs=performance.now()-commitStart;
    const liveReport={logicalSubscriptions:100,upstreamProducers:producerCount,origin:difference(await metrics(),liveBefore),wire:difference(snapshots(),liveBytes),clientFrameBytes:receivedBytes,initialLatencyMs:latency(consumers.map(consumer=>consumer.ms)),commitFanoutMs:fanoutMs};
    const bob=await connect('bob','bob');assert.notEqual(bob.frames[0].extensions.distributed.cacheScope,initial.extensions.distributed.cacheScope);
    assert.equal((await metrics()).producers,producerCount+1);bob.cancel();
    await until(async()=>(await metrics()).producers===producerCount,'subject control teardown');
    for(const consumer of consumers)consumer.cancel();
    await until(async()=>(await metrics()).producers===0,'last subscriber teardown');
    const recoveryBefore=await metrics(),recoveryBytes=snapshots(),recoveryClientBytes=receivedBytes;
    let recovery;
    if(mode==='live'){
      for(let index=0;index<7;index++)await advance();
      const gap=await connect('gap','alice',initial.extensions.distributed.live.cursors);
      assert.equal(gap.frames[0].extensions.distributed.live.reset,true,'old cursor gets an explicit origin reset');
      gap.cancel();await until(async()=>(await metrics()).producers===0,'gap teardown');
      if(host==='workerd'){
        const fast=await connect('fast'),slow=await connect('slow');
        for(let index=0;index<20;index++){
          const before=fast.frames.length;await advance();await until(()=>fast.frames.length>before,'healthy consumer receives update');
        }
        await until(()=>slow.closed()||slow.errors.length,'slow consumer reset');
        assert.equal(slow.frames.length,1,'unacknowledged client has bounded network delivery');
        fast.cancel();await until(async()=>(await metrics()).producers===0,'slow scenario teardown');
      }
      recovery={origin:difference(await metrics(),recoveryBefore),wire:difference(snapshots(),recoveryBytes),clientFrameBytes:receivedBytes-recoveryClientBytes,cursorResets:1,slowClientResets:host==='workerd'?1:0};
    }
    report.rows.push({host,mode,recovery,query:queryReport,live:liveReport,decisions:await counts(),subjectControls:'query and live scope differ; separate producer',invalidation:'new projection returned; old cache envelope rejected'});
    console.log(`PASS ${host}/${mode}: 100 queries -> ${queryWork.resultExecutions} result SQL; 100 live -> ${producerCount} producers; measured origin and client bytes`);
  }finally{for(const socket of clients)socket.terminate();}
}
try{
  for(const host of ['native','workerd'])for(const mode of ['none','flights','snapshots','live']){
    const runtime=host==='native'?{publicOrigin:native[mode],stop:async()=>{}}:await startRuntime({apiOrigin:meterOrigin,mode,artifact:'load-'+mode+'-workerd.log'});
    try{await modeRun(host,mode,runtime);}finally{await runtime.stop();}
  }
  await mkdir(root+'/artifacts',{recursive:true});await writeFile(root+'/artifacts/load-report.json',JSON.stringify(report,null,2)+'\n');
}finally{for(const client of wss.clients)client.terminate();wss.close();meter.closeAllConnections();meter.close();}
