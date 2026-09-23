import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdir,writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
export const root=path.dirname(fileURLToPath(import.meta.url));
export async function freePort(){const server=createServer();server.listen(0,'127.0.0.1');await once(server,'listening');const port=server.address().port;await new Promise(resolve=>server.close(resolve));return port;}
export async function startRuntime({apiOrigin,uiOrigin=apiOrigin,mode='all',port=0,artifact='query-workerd.log',requestBytes=16*1024*1024}={}){
  port ||= await freePort(); const publicOrigin=`http://127.0.0.1:${port}`;let log='';
  const runner=spawn(process.execPath,['node_modules/wrangler/bin/wrangler.js','dev','--local','--ip','127.0.0.1','--port',String(port),'--var',`PUBLIC_ORIGIN:${publicOrigin}`,'--var',`UI_ORIGIN:${uiOrigin}`,'--var',`API_ORIGIN:${apiOrigin}`,'--var',`DELIVERY_MODE:${mode}`,'--var',`REQUEST_BYTES:${requestBytes}`],{cwd:root,env:{PATH:process.env.PATH,HOME:process.env.HOME,RUSTUP_TOOLCHAIN:process.env.RUSTUP_TOOLCHAIN||'stable',WRANGLER_SEND_METRICS:'false',CI:'1'},stdio:['ignore','pipe','pipe']});
  runner.stdout.on('data',chunk=>{log+=chunk;});runner.stderr.on('data',chunk=>{log+=chunk;});
  const stop=async()=>{if(runner.exitCode===null){runner.kill('SIGTERM');await once(runner,'exit');}await mkdir(path.join(root,'artifacts'),{recursive:true});await writeFile(path.join(root,'artifacts',artifact),log);};
  try{
    for(let n=0;n<600;n++){
      if(runner.exitCode!==null)throw Error(`workerd exited: ${log.slice(-7000)}`);
      try{if((await fetch(`${publicOrigin}/__gateway_health`)).ok)return {publicOrigin,port,stop,logs:()=>log};}catch{}
      await new Promise(resolve=>setTimeout(resolve,200));
    }
    throw Error(`workerd readiness timeout: ${log.slice(-7000)}`);
  }catch(error){await stop();throw error;}
}
