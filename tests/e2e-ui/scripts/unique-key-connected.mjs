import assert from 'node:assert/strict';
import { build, transform } from '../../../js/node_modules/esbuild/lib/main.js';
import { chromium, expect } from '../node_modules/@playwright/test/index.mjs';
import { createDistributedSvelteKitServer, defineDistributedBoundaryBinding, defineDistributedBoundaryOperation } from '../../../js/dist/sveltekit/index.js';
import { fileURLToPath } from 'node:url';

const origin = process.argv[2];
assert.match(origin, /^http:\/\/127\.0\.0\.1:\d+$/);
const fixture = await (await fetch(`${origin}/fixture`)).json();
const compiled = await transform(fixture.module, { loader: 'ts', format: 'esm' });
const artifact = (await import(`data:text/javascript;base64,${Buffer.from(compiled.code).toString('base64')}`))[fixture.exportName];
const boundary = defineDistributedBoundaryOperation({ operation: 'References', route: '/references', kind: 'page', discovery: 'route_document' }, artifact, defineDistributedBoundaryBinding(artifact, {}));
const server = createDistributedSvelteKitServer({ boundaries: [boundary], getSession: async () => null, getRole: () => 'user' });
let ssrFetches = 0;
const loaded = await server.load({ locals: {}, route: { id: '/references' }, url: new URL(`${origin}/references`),
    fetch(url, init) { ssrFetches++; return fetch(new URL(url, origin), init); } });
assert.equal(loaded.gqlError, null);
assert.equal(ssrFetches, 1);
const runtime = fileURLToPath(new URL('../../../js/dist/sveltekit/index.js', import.meta.url));
const bundle = await build({ stdin: { contents: `import { createDistributedSvelteKit, defineDistributedBoundaryOperation, defineDistributedBoundaryBinding } from ${JSON.stringify(runtime)};
globalThis.mountProof = (artifact, loaded) => {
  const boundary = defineDistributedBoundaryOperation({ operation: 'References', route: '/references', kind: 'page', discovery: 'route_document' }, artifact, defineDistributedBoundaryBinding(artifact, {}));
  const client = createDistributedSvelteKit({ boundaries: [boundary], hydration: loaded.distributed,
    authority: loaded.distributedAuthority, session: { getAuth: () => ({}) } });
  const view = client.operation(artifact).use();
  const unsubscribe = view.subscribe(snapshot => { document.querySelector('output').textContent = JSON.stringify(snapshot.data); });
  globalThis.closeProof = () => { unsubscribe(); client.destroy(); };
};`, loader: 'js', resolveDir: process.cwd() }, bundle: true, write: false, platform: 'browser', format: 'iife' });
const browser = await chromium.launch();
try {
    const page = await browser.newPage();
    const browserErrors = [];
    page.on('pageerror', error => browserErrors.push(error.message));
    let graphqlFetches = 0;
    const sockets = [];
    let liveFrames = 0;
    let closed = false;
    page.on('request', request => { if (request.url() === `${origin}/graphql`) graphqlFetches++; });
    page.on('websocket', socket => {
        sockets.push(socket);
        socket.on('socketerror', error => browserErrors.push(String(error)));
        socket.on('framereceived', event => { if (JSON.parse(String(event.payload)).type === 'next') liveFrames++; });
        socket.on('close', () => { closed = true; });
    });
    await page.goto(`${origin}/references`);
    await page.addScriptTag({ content: bundle.outputFiles[0].text });
    await page.evaluate(({ artifact, loaded }) => globalThis.mountProof(artifact, loaded), { artifact, loaded });
    await page.waitForFunction(() => document.querySelector('output').textContent.includes('opaque-one'));
    assert.equal(graphqlFetches, 0, 'SSR seed must avoid browser mount fetch');
    await expect.poll(() => liveFrames).toBeGreaterThan(0);
    assert.equal(sockets.length, 1, 'real GraphQL WebSocket opened');
    const published = await fetch(`${origin}/publish`, { method: 'POST' });
    assert.equal(published.status, 200);
    await page.waitForFunction(() => document.querySelector('output').textContent.includes('opaque-two'));
    assert.deepEqual(JSON.parse(await page.locator('output').textContent()), { reference_views: [
        { id: 'reference-stable', target: { id: 'opaque-two', body: 'Content two' } }
    ] });
    assert.equal(graphqlFetches, 0, 'live update must arrive via WebSocket');
    await page.evaluate(() => globalThis.closeProof());
    await expect.poll(() => closed).toBe(true);
    assert.deepEqual(browserErrors, []);
    console.log('Connected SQLite/projector → GraphQL HTTP SSR → Chromium → WebSocket update passed');
} finally { await browser.close(); }
