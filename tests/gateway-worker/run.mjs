import assert from 'node:assert/strict';
import path from 'node:path';
import {root,freePort,startRuntime} from './runtime.mjs';
const port=await freePort();
const publicOrigin=`http://127.0.0.1:${port}`;
process.env.GATEWAY_TEST_ORIGIN=publicOrigin;
process.chdir(path.join(root,'../gateway-auth'));
const {startFixture,exerciseAuth}=await import('../gateway-auth/run.mjs');
for(const secureCookies of [false,true]){
  process.chdir(path.join(root,'../gateway-auth'));
  const fixture=await startFixture({secureCookies});
  process.chdir(root);
  let runtime;
  try{
    runtime=await startRuntime({port,uiOrigin:fixture.uiOrigin,apiOrigin:fixture.uiOrigin,artifact:secureCookies?'secure-auth-workerd.log':'auth-workerd.log'});
    assert.equal((await fetch(publicOrigin+'/__owned/not-found')).status,404);
    assert.equal((await fetch(publicOrigin+'/',{headers:{authorization:'Bearer invalid'}})).status,401);
    if(secureCookies){
      const response=await fetch(publicOrigin+'/login',{redirect:'manual'});
      assert.equal(response.status,302);const cookies=response.headers.getSetCookie();
      assert.ok(cookies.length>=3);assert.ok(cookies.every(cookie=>/; Secure(?:;|$)/i.test(cookie)));
      console.log('PASS explicit secure-cookie policy survives actual workerd delegation');
    }else{
      await exerciseAuth(fixture);
      console.log('PASS real production Auth.js/OIDC through actual workerd ingress');
    }
  }finally{await runtime?.stop();await fixture.stop();}
}
