import {Miniflare,convertV4MiniflareOptions} from 'miniflare';
import {root,freePort} from './runtime.mjs';
import path from 'node:path';
export async function startShardedRuntime(apiOrigin){
  const port=await freePort();const publicOrigin=`http://127.0.0.1:${port}`;
  const common={modules:[{type:'ESModule',path:path.join(root,'build/index.js')},{type:'CompiledWasm',path:path.join(root,'build/index_bg.wasm')}],modulesRoot:path.join(root,'build'),compatibilityDate:'2026-09-03',compatibilityFlags:['enable_request_signal']};
  const worker=(name)=>({...common,name,bindings:{PUBLIC_ORIGIN:publicOrigin,UI_ORIGIN:apiOrigin,API_ORIGIN:apiOrigin,DELIVERY_MODE:'all',INGRESS_ID:name},durableObjects:{DELIVERY:{className:'DeliveryCoordinator',...(name==='coordinator'?{}:{scriptName:'coordinator'}),useSQLite:true}}});
  const mf=new Miniflare(convertV4MiniflareOptions({cf:false,host:'127.0.0.1',port:await freePort(),workers:[
    {name:'test-distributor',unsafeDirectSockets:[{host:'127.0.0.1',port,proxy:false}],modules:true,compatibilityDate:'2026-09-03',compatibilityFlags:['enable_request_signal'],serviceBindings:{A:'ingress-a',B:'ingress-b'},script:`export default {fetch(request,env){const url=new URL(request.url);const target=url.searchParams.get('__ingress')==='b'?env.B:env.A;url.searchParams.delete('__ingress');const outbound=new Request(url,request);if(request.headers.get('x-fixture-cancel')==='yes'){const controller=new AbortController();setTimeout(()=>controller.abort(),500);return target.fetch(new Request(outbound,{signal:controller.signal}));}return target.fetch(outbound);}}`},
    worker('ingress-a'),worker('ingress-b'),worker('coordinator'),
  ]}));
  try{await mf.ready;}catch(error){await mf.dispose();throw error;}
  return {publicOrigin,stop:()=>mf.dispose(),at:(index,path)=>`${publicOrigin}${path}${path.includes('?')?'&':'?'}__ingress=${index%2?'b':'a'}`};
}
