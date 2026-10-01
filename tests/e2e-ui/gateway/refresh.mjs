import assert from 'node:assert/strict';
import {expect} from '../../gateway-auth/node_modules/@playwright/test/index.mjs';

// Exercise actual background OIDC rotation, including the route seed and the
// follow-up SvelteKit invalidation. A final DOM assertion alone misses a flash.
export async function verifySessionRefreshContinuity(page, origin) {
  for (const [route, selector] of [['todos', '[data-todo-id]'], ['chat', '.ch-msg-block']]) {
    await page.goto(origin+'/'+route);
    await expect(page.locator(selector).first()).toBeVisible();
    await page.waitForFunction(()=>globalThis.__distributedReloadState!==undefined);
    await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    const navigations=[];
    const request=r=>{if(r.isNavigationRequest())navigations.push(r.url());};
    page.on('request',request);
    await page.evaluate(selector=>{
      const rows=[...document.querySelectorAll(selector)];
      globalThis.__refreshContinuity={lost:false,events:[],rows,observer:new MutationObserver(records=>{
        if(rows.some(row=>!row.isConnected)||records.some(record=>[...record.removedNodes].some(node=>rows.some(row=>node===row||node.contains(row))))){globalThis.__refreshContinuity.lost=true;globalThis.__refreshContinuity.events.push({time:performance.now(),remaining:rows.filter(row=>row.isConnected).length});}
      })};
      globalThis.__refreshContinuity.observer.observe(document.body,{childList:true,subtree:true});
    },selector);
    let releaseRefresh, seedArrived;
    const release = new Promise(resolve=>releaseRefresh=resolve);
    const seed = new Promise(resolve=>seedArrived=resolve);
    let hold = route==='todos';
    const refreshRoute = async request=>{
      if(!hold)return request.continue();
      hold=false;
      const response=await request.fetch();
      seedArrived();await release;await request.fulfill({response});
    };
    await page.route('**/api/auth/refresh',refreshRoute);
    try {
      for(let rotation=0;rotation<2;rotation++) {
        const responses = Promise.all([
          page.waitForResponse(r=>r.url()===origin+'/api/auth/refresh'&&r.request().method()==='POST',{timeout:20000}),
          page.waitForResponse(r=>r.url().includes('/'+route+'/__data.json'),{timeout:20000})
        ]);
        if(route==='todos'&&rotation===0) {
          await seed;
          // The auth snapshot is now old: confirm a command before delivering it.
          const title='confirmed after auth snapshot '+Date.now();
          await page.locator('#todo-title').fill(title);
          await page.getByRole('button',{name:/^add$/i}).click();
          const todo=page.locator('[data-todo-id]').filter({hasText:title});
          await expect(todo).toBeVisible();
          await expect(todo.locator('.pending-state')).toHaveCount(0);
          await todo.evaluate(row=>globalThis.__refreshContinuity.rows.push(row));
          releaseRefresh();
        }
        const [refresh, data] = await responses;
        assert.equal(refresh.status(),200);
        const refreshed = await refresh.json();
        assert.ok(refreshed.pageData?.distributedAuthority);
        assert.equal(data.status(),200);
        await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
      }
      assert.deepEqual(navigations,[],'session refresh must not navigate the document');
      const lost = await page.evaluate(()=>globalThis.__refreshContinuity.lost);
      if (lost) {
        console.error('Redacted refresh replica diagnostics:', JSON.stringify(await page.evaluate(()=>globalThis.__replicaDiagnosticSnapshot?.())));
      }
      assert.equal(lost,false,route+' rows were removed during token refresh: '+JSON.stringify(await page.evaluate(()=>globalThis.__refreshContinuity.events)));
    } finally {
      releaseRefresh();
      await page.unroute('**/api/auth/refresh',refreshRoute);
      page.off('request',request);
      await page.evaluate(()=>globalThis.__refreshContinuity?.observer.disconnect());
    }
  }
  console.log('PASS Todo and Chat retain DOM rows through OIDC rotation and a late auth snapshot');
}
