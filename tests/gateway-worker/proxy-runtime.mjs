import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {once} from 'node:events';
import WebSocket,{WebSocketServer} from 'ws';
import {startRuntime} from './runtime.mjs';
let streamClosed=false,upgrades=0,mutations=0;
const server=createServer(async(request,response)=>{
  if(request.url==='/redirect'){response.writeHead(302,{location:origin+'/target?one=1'});response.end();return;}
  if(request.url==='/cookies'){response.writeHead(200,{'set-cookie':['first=1; HttpOnly; SameSite=Lax','second=2; Secure; SameSite=Strict']});response.end('cookies');return;}
  if(request.url==='/stream'){
    response.writeHead(200,{'content-type':'application/octet-stream'});const interval=setInterval(()=>response.write(Buffer.alloc(4096,7)),10);
    response.on('close',()=>{clearInterval(interval);streamClosed=true;});return;
  }
  if(request.url==='/failure'){response.writeHead(503);response.end('origin failure');return;}
  let bytes=0,body='';try{for await(const chunk of request){bytes+=chunk.length;if(request.url==='/graphql')body+=chunk;}}catch{return;}
  if(request.url==='/graphql'&&JSON.parse(body).query.startsWith('mutation'))mutations++;
  response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify({headers:request.headers,bytes,method:request.method,data:{accepted:true}}));
});
const wsServer=new WebSocketServer({noServer:true});
server.on('upgrade',(request,socket,head)=>wsServer.handleUpgrade(request,socket,head,ws=>{upgrades++;ws.on('message',(bytes,binary)=>{
    if(request.url==='/graphql/ws'){
      const message=JSON.parse(bytes);
      if(message.type==='connection_init')ws.send(JSON.stringify({type:'connection_ack'}));
      else if(message.type==='subscribe'||message.type==='start'){
        if(message.payload.query.startsWith('mutation'))mutations++;
        ws.send(JSON.stringify({id:message.id,type:message.type==='start'?'data':'next',payload:{data:{accepted:true},extensions:{unchanged:'origin'}}}));
        ws.send(JSON.stringify({id:message.id,type:'complete'}));
      }
    }else ws.send(bytes,{binary});
  });ws.on('close',()=>upgrades--);}));
server.listen(0,'127.0.0.1');await once(server,'listening');const origin=`http://127.0.0.1:${server.address().port}`;
let runtime;
async function until(predicate){for(let i=0;i<300;i++){if(await predicate())return;await new Promise(r=>setTimeout(r,20));}throw Error('proxy condition not reached');}
try{
  runtime=await startRuntime({apiOrigin:origin,requestBytes:65536,mode:'none',artifact:'proxy-workerd.log'});const base=runtime.publicOrigin;
  const headers=(await(await fetch(base+'/echo',{headers:{origin:base,cookie:'opaque=session','x-user-id':'mallory','x-forwarded-host':'attacker.invalid','x-distributed-subject':'mallory'}})).json()).headers;
  assert.equal(headers.origin,base);assert.equal(headers.cookie,'opaque=session');assert.equal(headers['x-forwarded-host'],new URL(base).host);assert.equal(headers['x-forwarded-proto'],'http');assert.equal(headers['x-user-id'],undefined);assert.equal(headers['x-distributed-subject'],undefined);
  const redirect=await fetch(base+'/redirect',{redirect:'manual'});assert.equal(redirect.status,302);assert.equal(redirect.headers.get('location'),base+'/target?one=1');
  assert.equal((await fetch(base+'/cookies')).headers.getSetCookie().length,2);
  assert.equal((await fetch(base+'/failure')).status,503);assert.equal((await fetch(base+'/__owned/missing')).status,404);assert.equal((await fetch(base+'/graphql')).status,405);
  assert.equal((await fetch(base+'/graphql',{method:'POST',body:JSON.stringify({query:'mutation Change { change }'})})).status,200);assert.equal(mutations,1);
  console.log('PASS workerd header trust, redirects, duplicate cookies, terminal errors and one command execution');
  const upload=new ReadableStream({start(controller){for(let i=0;i<4;i++)controller.enqueue(new Uint8Array(4096));controller.close();}});
  const result=await(await fetch(base+'/upload',{method:'POST',body:upload,duplex:'half'})).json();assert.equal(result.bytes,16384);
  assert.equal((await fetch(base+'/graphql',{method:'POST',body:'x'.repeat(65537)})).status,413);
  const tooBig=new ReadableStream({start(controller){controller.enqueue(new Uint8Array(65537));controller.close();}});
  assert.equal((await fetch(base+'/upload',{method:'POST',body:tooBig,duplex:'half'})).status,413);
  const stream=await fetch(base+'/stream');const reader=stream.body.getReader();assert.ok((await reader.read()).value.length);await reader.cancel();await until(()=>streamClosed);
  console.log('PASS actual streamed upload, body limits and response cancellation reaches origin');
  const ws=new WebSocket(base.replace('http:','ws:')+'/socket');await once(ws,'open');
  let next=once(ws,'message');ws.send('hello');let [data,binary]=await next;assert.equal(data.toString(),'hello');assert.equal(binary,false);
  next=once(ws,'message');ws.send(Buffer.from([0,255,17]));[data,binary]=await next;assert.deepEqual([...data],[0,255,17]);assert.equal(binary,true);
  const closed=once(ws,'close');ws.close(1000,'done');const [code,reason]=await closed;assert.equal(code,1000);assert.equal(reason.toString(),'done');await until(()=>upgrades===0);
  console.log('PASS actual UI text/binary WebSocket relay and close teardown');
  for(const legacy of [false,true]){
    const socket=new WebSocket(base.replace('http:','ws:')+'/graphql/ws',legacy?'graphql-ws':'graphql-transport-ws');await once(socket,'open');
    const messages=[];socket.on('message',data=>messages.push(JSON.parse(data)));
    socket.send(JSON.stringify({type:'connection_init',payload:{opaque:'origin-owned'}}));await until(()=>messages.some(m=>m.type==='connection_ack'));
    for(const [id,query] of [['query','query Read { value }'],['command','mutation Change { change }']]){
      socket.send(JSON.stringify({id,type:legacy?'start':'subscribe',payload:{query}}));await until(()=>messages.some(m=>m.id===id&&m.type==='complete'));
      const frame=messages.find(m=>m.id===id&&m.type===(legacy?'data':'next'));assert.deepEqual(frame.payload,{data:{accepted:true},extensions:{unchanged:'origin'}});
    }
    socket.close();await once(socket,'close');
  }
  assert.equal(mutations,3);await until(()=>upgrades===0);
  console.log('PASS modern/legacy query and command envelopes with delivery entirely disabled');
  await runtime.stop();runtime=await startRuntime({apiOrigin:origin,requestBytes:65536,mode:'all',artifact:'proxy-coordinated-workerd.log'});
  const socket=new WebSocket(runtime.publicOrigin.replace('http:','ws:')+'/graphql/ws','graphql-transport-ws');await once(socket,'open');
  const messages=[];socket.on('message',data=>messages.push(JSON.parse(data)));
  socket.send(JSON.stringify({type:'connection_init',payload:{opaque:'origin-owned'}}));await until(()=>messages.some(m=>m.type==='connection_ack'));
  for(const [id,query] of [['read','query Read { value }'],['write','mutation Change { change }']]){
    socket.send(JSON.stringify({id,type:'subscribe',payload:{query}}));await until(()=>messages.some(m=>m.id===id&&m.type==='complete'));
    assert.deepEqual(messages.find(m=>m.id===id&&m.type==='next').payload,{data:{accepted:true},extensions:{unchanged:'origin'}});
  }
  socket.close();await once(socket,'close');await until(()=>upgrades===0);assert.equal(mutations,4);
  console.log('PASS delivery-enabled command identity and query fallback for origins without control protocol');


}finally{await runtime?.stop();for(const ws of wsServer.clients)ws.terminate();await new Promise(r=>wsServer.close(r));server.closeAllConnections();await new Promise(r=>server.close(r));}
