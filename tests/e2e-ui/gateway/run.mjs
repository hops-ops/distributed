import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {chromium,expect} from '../../gateway-auth/node_modules/@playwright/test/index.mjs';
import {startProvider} from '../../gateway-auth/provider.mjs';
import {verifySessionRefreshContinuity} from './refresh.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const devMode=process.env.GATEWAY_DEV==='1';
const environment={PATH:process.env.PATH,HOME:process.env.HOME,RUSTUP_TOOLCHAIN:process.env.RUSTUP_TOOLCHAIN||'stable',NODE_ENV:'production'};
async function freePort(){const server=createServer();server.listen(0,'127.0.0.1');await once(server,'listening');const port=server.address().port;await new Promise(r=>server.close(r));return port;}
function launch(program,args,{cwd=root,env=environment}={}){
  let log='';const child=spawn(program,args,{cwd,env,stdio:['ignore','pipe','pipe']});
  child.on('error',error=>log+='process launch failed: '+error.code);
  child.stdout.on('data',chunk=>log=(log+chunk).slice(-60000));child.stderr.on('data',chunk=>log=(log+chunk).slice(-60000));
  return {child,logs:()=>log,stop:async()=>{if(child.exitCode===null){child.kill('SIGINT');await once(child,'exit');}}};
}
async function ready(url,process){for(let i=0;i<3600;i++){if(process.child.exitCode!==null)throw Error(process.logs());try{const response=await fetch(url);if(response.ok)return;}catch{}await new Promise(r=>setTimeout(r,100));}throw Error('readiness timeout: '+process.logs());}
const artifacts=path.join(root,'gateway/artifacts');await mkdir(artifacts,{recursive:true});
if(process.env.GATEWAY_SKIP_BUILD!=='1'){
  const {NODE_ENV,...buildEnvironment}=environment;
  const build=launch('cargo',['run','--quiet','--manifest-path',path.resolve(root,'../../Cargo.toml'),'-p','distributed_cli','--bin','distributed','--','build',root],{env:buildEnvironment});
  const [code]=await once(build.child,'exit');await writeFile(path.join(artifacts,'ui-build.log'),build.logs());assert.equal(code,0,build.logs());
}
const temporary=await mkdtemp(path.join(os.tmpdir(),'gateway-app-'));
try{
 for(const delivery of (process.env.GATEWAY_LIFECYCLE==='1'?['none']:['none','all'])){
  const apiPort=await freePort(),uiPort=await freePort(),issuer=`http://127.0.0.1:${await freePort()}`;
  const publicOrigin=`http://127.0.0.1:${apiPort}`;
  const idp=await startProvider(issuer,publicOrigin,{jwtAudience:'gateway-fixture',accessTokenTtl:65});
  let api,ui,browser;
  try{
    if(devMode){
      const {NODE_ENV,...devEnvironment}=environment;
      api=launch(path.resolve(root,'../../target/debug/distributed'),['dev',root],{env:{...devEnvironment,DATABASE_URL:`sqlite:${temporary}/${delivery}.db?mode=rwc`,BIND:`127.0.0.1:${apiPort}`,PUBLIC_ORIGIN:publicOrigin,UI_INTERNAL_ORIGIN:`http://127.0.0.1:${uiPort}`,UI_PORT:String(uiPort),UI_BIND:'127.0.0.1',GATEWAY_DELIVERY:delivery,OIDC_ISSUER:issuer,OIDC_AUDIENCE:'gateway-fixture',OIDC_CLIENT_ID:'gateway-fixture',OIDC_CLIENT_SECRET:'local-fixture-only',AUTH_URL:publicOrigin,AUTH_SECRET:randomBytes(32).toString('hex'),AUTH_USE_SECURE_COOKIES:'false',GRAPHIQL:'0'}});
      ui={logs:api.logs,stop:async()=>{}};
    }else{
    api=launch(path.join(root,'target/debug/e2e-ui'),[],{env:{...environment,DATABASE_URL:`sqlite:${temporary}/${delivery}.db?mode=rwc`,BIND:`127.0.0.1:${apiPort}`,PUBLIC_ORIGIN:publicOrigin,UI_INTERNAL_ORIGIN:`http://127.0.0.1:${uiPort}`,GATEWAY_DELIVERY:delivery,OIDC_ISSUER:issuer,OIDC_AUDIENCE:'gateway-fixture',OIDC_CLIENT_ID:'gateway-fixture',GRAPHIQL:'0'}});
    await ready(publicOrigin+'/health',api);
    ui=launch(process.execPath,['build/index.js'],{cwd:path.join(root,'ui'),env:{...environment,HOST:'127.0.0.1',PORT:String(uiPort),PUBLIC_ORIGIN:publicOrigin,ORIGIN:publicOrigin,AUTH_URL:publicOrigin,AUTH_SECRET:randomBytes(32).toString('hex'),AUTH_USE_SECURE_COOKIES:'false',OIDC_ISSUER:issuer,OIDC_CLIENT_ID:'gateway-fixture',OIDC_CLIENT_SECRET:'local-fixture-only',OIDC_AUDIENCE:'gateway-fixture',E2E_API_ORIGIN:publicOrigin}});
    }
    await ready(publicOrigin,api);
    if(devMode){
      const participant='gateway_ci_lifecycle_probe';
      const response=await fetch(publicOrigin+'/__distributed/lifecycle',{headers:{'x-distributed-participant':participant}});
      assert.equal(response.status,200,'gateway must route lifecycle to the UI');
      assert.equal((await response.json()).phase,'active');
      const heartbeat=JSON.parse(await readFile(path.join(root,'.distributed/lifecycle/dev-control/participants',participant+'.json'),'utf8'));
      assert.ok(Date.now()-heartbeat.seenAtUnixMs<5000);
      const ack=await fetch(publicOrigin+'/__distributed/lifecycle',{method:'POST',headers:{origin:publicOrigin,'content-type':'application/json'},body:JSON.stringify({participantId:participant,transitionId:'gateway_ci_no_transition',ok:true})});
      assert.equal(ack.status,409,'same-origin acknowledgement reaches lifecycle state validation');
    }
    browser=await chromium.launch();const context=await browser.newContext();
    await context.addInitScript(()=>{globalThis.__captureReplicaDiagnostics=true;});
    const page=await context.newPage();
    page.on('console', message=>{if(message.text().startsWith('auth-audit:'))console.log(message.text());});
    const errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(publicOrigin);await page.getByRole('link',{name:/log in|sign in/i}).first().click();
    await page.getByRole('button',{name:'Continue as Alice'}).click();
    await page.waitForURL(url=>url.origin===publicOrigin&&!url.pathname.startsWith('/auth')&&!url.pathname.startsWith('/login'));
    const session=await(await context.request.get(publicOrigin+'/auth/session')).json();assert.equal(session.user.id,'alice');
    await page.goto(publicOrigin+'/todos');await expect(page.getByRole('heading',{name:/todos/i})).toBeVisible();
    const unauthenticated=await fetch(publicOrigin+'/graphql',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({query:'query { todos { todo_id } }'})});
    assert.match(unauthenticated.headers.get('content-type'),/json/);
    assert.ok((await unauthenticated.json()).errors?.length);
    assert.equal((await context.request.post(publicOrigin+'/todo.create',{data:{}})).status(),404);
    let releaseCommand,commandReady;
    const commandBarrier=new Promise(resolve=>releaseCommand=resolve),commandArrived=new Promise(resolve=>commandReady=resolve);
    await page.route('**/graphql',async route=>{
      if(!(route.request().postData()||'').includes('todos_create'))return route.continue();
      const response=await route.fetch();commandReady();await commandBarrier;await route.fulfill({response});
    });
    const title='gateway todo '+delivery;await page.locator('input').first().fill(title);await page.getByRole('button',{name:/^add$/i}).click();
    const todo=page.locator('[data-todo-id]').filter({hasText:title});await expect(todo).toBeVisible();await commandArrived;await expect(todo.locator('.pending-state')).toHaveText('Saving…');releaseCommand();await expect(todo.locator('.pending-state')).toHaveCount(0,{timeout:20000});
    await page.unroute('**/graphql');
    await todo.getByRole('button',{name:/^done$/i}).click();await expect(page.locator('.panel').filter({has:page.getByRole('heading',{name:/^done$/i})}).getByText(title)).toBeVisible();
    await page.goto(publicOrigin+'/blob');await expect(page.getByTestId('blob-start-game')).toBeEnabled();await page.getByTestId('blob-start-game').click();await expect(page.locator('.blob-board')).toBeVisible({timeout:20000});
    await verifyBlobRace(page);
    await verifyLiveRace(page,publicOrigin);
    await verifySessionRefreshContinuity(page,publicOrigin);
    if(process.env.GATEWAY_LIFECYCLE==='1'){
      assert.ok(devMode,'full reload proof requires the CLI dev host');
      const storage=path.join(temporary,'reload-auth.json');await context.storageState({path:storage});
      // The lifecycle fixture owns the browser participants during source edits.
      await browser.close();browser=undefined;
      const reload=launch(process.execPath,['scripts/lifecycle-reload.mjs'],{env:{...environment,E2E_UI_ORIGIN:publicOrigin,E2E_API_ORIGIN:publicOrigin,E2E_RELOAD_STORAGE_STATE:storage}});
      reload.child.stdout.on('data',chunk=>process.stdout.write(chunk));
      const [code]=await once(reload.child,'exit');
      await writeFile(path.join(artifacts,'lifecycle-reload.log'),reload.logs());
      assert.equal(code,0,reload.logs());
      console.log('PASS complete controlled browser lifecycle through the public gateway');
    }else await verifyAuth(context,idp,publicOrigin);
    assert.deepEqual(errors,[]);console.log('PASS actual public-origin application login, Todo Eventual and Blob Atomic with delivery '+delivery);
  }catch(error){await writeFile(path.join(artifacts,delivery+'-failure.txt'),String(error));throw error;}
  finally{await browser?.close();await ui?.stop();await api?.stop();await new Promise(r=>idp.server.close(r));if(api)await writeFile(path.join(artifacts,delivery+'-api.log'),api.logs());if(ui)await writeFile(path.join(artifacts,delivery+'-ui.log'),ui.logs());}
 }
}finally{await rm(temporary,{recursive:true,force:true});}

// Delay a real old HTTP envelope while an Atomic response advances the replica.
// Delivery-enabled requests exercise the cache path; the body/proof are unchanged.
async function verifyBlobRace(page){
  await expect(page.locator('[data-blob-hydrated="1"]')).toBeVisible();
  const player=page.locator('.blob-board .tile-player');
  let queries=0;const count=request=>{if((request.postData()||'').includes('query BlobGames'))queries++;};
  page.on('request',count);
  const move=async(key,label)=>{
    const result=page.waitForResponse(response=>(response.request().postData()||'').includes('blob_games_move'));
    await page.keyboard.press(key);assert.ok((await result).ok());await expect(player).toHaveAttribute('aria-label',label);
  };
  await move('ArrowRight','r0 c1');
  assert.equal(queries,0,'Atomic direct response should paint without an HTTP refetch');
  let release,arrived;const barrier=new Promise(resolve=>release=resolve),ready=new Promise(resolve=>arrived=resolve);
  let held=false;
  await page.route('**/graphql',async route=>{
    if(held||!(route.request().postData()||'').includes('query BlobGames'))return route.continue();
    held=true;const response=await route.fetch();assert.ok((await response.json()).data?.blob_games?.length);arrived();await barrier;await route.fulfill({response});
  });
  const refetch=()=>page.evaluate(()=>globalThis.__distributedBlobRefetch());
  const oldRequest=refetch();await ready;
  const hole=(await page.locator('.cell[aria-label="r0 c2"]').getAttribute('class')).includes('tile-hole');
  const label=hole?'r1 c1':'r0 c2';await move(hole?'ArrowDown':'ArrowRight',label);
  await page.evaluate(()=>{
    globalThis.__gatewaySamples=[];
    globalThis.__gatewayObserver=new MutationObserver(()=>globalThis.__gatewaySamples.push(document.querySelector('.blob-board .tile-player')?.getAttribute('aria-label')??'missing'));
    globalThis.__gatewayObserver.observe(document.querySelector('.blob-page'),{attributes:true,childList:true,subtree:true,characterData:true});
  });
  release();await oldRequest;await refetch();await expect(player).toHaveAttribute('aria-label',label);
  const samples=await page.evaluate(()=>{globalThis.__gatewayObserver.disconnect();return globalThis.__gatewaySamples;});
  assert.ok(samples.every(sample=>sample===label),'late HTTP/cache observation regressed the Atomic board');
  page.off('request',count);await page.unroute('**/graphql');
  await page.reload();await expect(player).toHaveAttribute('aria-label',label);
}
async function verifyAuth(context,idp,origin){
  const cookies=(await context.cookies()).filter(cookie=>cookie.name.startsWith('authjs.session-token'));
  assert.ok(cookies.length&&cookies.every(cookie=>cookie.httpOnly&&cookie.sameSite==='Lax'&&cookie.path==='/'));
  await new Promise(resolve=>setTimeout(resolve,6000));
  const response=await context.request.post(origin+'/api/auth/refresh',{headers:{origin}});
  assert.equal(response.status(),200);assert.equal((await response.json()).authenticated,true);assert.ok(idp.refreshes()>0);
  idp.failRefresh();await new Promise(resolve=>setTimeout(resolve,6000));
  const failed=await context.request.post(origin+'/api/auth/refresh',{headers:{origin}});
  assert.equal(failed.status(),401);assert.equal((await failed.json()).error,'RefreshAccessTokenError');
  const denied=await context.request.get(origin+'/todos',{maxRedirects:0});assert.equal(denied.status(),303);
  const signedOut=await context.request.get(origin+'/signout',{maxRedirects:0});assert.equal(signedOut.status(),303);
  assert.ok(!(await context.cookies()).some(cookie=>cookie.name.startsWith('authjs.session-token')));
}

async function verifyLiveRace(page,origin){
  let oldFrame,downstream,liveUpdates=0;
  await page.routeWebSocket('**/graphql/ws',socket=>{
    const upstream=socket.connectToServer();
    upstream.onMessage(message=>{
      let frame;try{frame=JSON.parse(String(message));}catch{}
      if(frame?.type==='next'&&Array.isArray(frame.payload?.data?.chat_messages)){
        liveUpdates++;if(!oldFrame){oldFrame=message;downstream=socket;}
      }
      socket.send(message);
    });
  });
  await page.goto(origin+'/chat');await expect.poll(()=>liveUpdates,{timeout:20000}).toBeGreaterThan(0);
  const body='gateway live nonregression';
  await page.locator('#chat-body').fill(body);await page.getByRole('button',{name:/send/i}).click();
  await expect(page.getByText(body,{exact:true})).toBeVisible();
  await expect.poll(()=>liveUpdates,{timeout:20000}).toBeGreaterThan(1);
  await page.evaluate(text=>{
    globalThis.__gatewayLiveSamples=[];
    globalThis.__gatewayLiveObserver=new MutationObserver(()=>globalThis.__gatewayLiveSamples.push(document.body.innerText.includes(text)));
    globalThis.__gatewayLiveObserver.observe(document.body,{childList:true,subtree:true,characterData:true});
  },body);
  downstream.send(oldFrame);
  // Give the transport and Svelte render queue an observation window.
  await page.waitForTimeout(150);
  await expect(page.getByText(body,{exact:true})).toBeVisible();
  const samples=await page.evaluate(()=>{globalThis.__gatewayLiveObserver.disconnect();return globalThis.__gatewayLiveSamples;});
  assert.ok(samples.every(Boolean),'late live observation removed the confirmed message');
  const message=page.locator('.ch-msg',{hasText:body});
  await expect(page.locator('.ch-msg-block',{has:message}).locator('.ch-status-footer')).toHaveText('Delivered');
  await page.reload();
  await expect(page.locator('.ch-msg',{hasText:body})).toBeVisible();
}
