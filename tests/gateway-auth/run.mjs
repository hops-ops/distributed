import './prepare.mjs';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { randomBytes } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import { startProvider } from './provider.mjs';

async function freePort() {
  const server = createServer(); server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const port = server.address().port; await new Promise(resolve => server.close(resolve)); return port;
}
export async function startFixture({ secureCookies = false } = {}) {
  const uiPort = await freePort();
  const publicOrigin = process.env.GATEWAY_TEST_ORIGIN || `http://127.0.0.1:${uiPort}`;
  const issuer = `http://127.0.0.1:${await freePort()}`;
  const idp = await startProvider(issuer, publicOrigin);
  // Explicit environment: do not load the playground's real credential files.
  const build = spawn(process.execPath, ['node_modules/vite/bin/vite.js', 'build'], { env: { PATH: process.env.PATH, HOME: process.env.HOME, NODE_ENV: 'production' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let buildLog = '';
  build.stdout.on('data', chunk => { buildLog = (buildLog + chunk).slice(-4000); });
  build.stderr.on('data', chunk => { buildLog = (buildLog + chunk).slice(-4000); });
  const [buildCode] = await once(build, 'exit');
  if (buildCode !== 0) { await new Promise(resolve => idp.server.close(resolve)); throw Error('Auth.js build failed: ' + buildLog); }
  const ui = spawn(process.execPath, ['build/index.js'], {
    env: { PATH: process.env.PATH, HOME: process.env.HOME, NODE_ENV: 'production', HOST: '127.0.0.1', PORT: String(uiPort),
      OIDC_ISSUER: issuer, OIDC_CLIENT_ID: 'gateway-fixture', OIDC_CLIENT_SECRET: 'local-fixture-only',
      AUTH_SECRET: randomBytes(32).toString('hex'), AUTH_URL: publicOrigin, ORIGIN: publicOrigin,
      AUTH_USE_SECURE_COOKIES: String(secureCookies) }, stdio: ['ignore', 'pipe', 'pipe']
  });
  let log = ''; ui.stdout.on('data', chunk => { log = (log + chunk).slice(-6000); }); ui.stderr.on('data', chunk => { log = (log + chunk).slice(-6000); });
  const stop = async () => {
    if (ui.exitCode === null) { ui.kill('SIGTERM'); await once(ui, 'exit'); }
    await new Promise(resolve => idp.server.close(resolve));
  };
  try {
    for (let i = 0; i < 100; i++) {
      if (ui.exitCode !== null) throw Error('Auth.js process exited: ' + log);
      try { if ((await fetch(`http://127.0.0.1:${uiPort}/`)).ok) return { publicOrigin, uiOrigin: `http://127.0.0.1:${uiPort}`, issuer, idp, stop, logs: () => log }; } catch {}
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    throw Error('Auth.js readiness timed out: ' + log);
  } catch (error) { await stop(); throw error; }
}

export async function exerciseAuth(fixture) {
  const { publicOrigin, idp } = fixture;
  const browser = await chromium.launch();
  try {
    const context = await browser.newContext();
    const page = await context.newPage();
    assert.equal((await context.request.get(`${publicOrigin}/private`)).status(), 401);
    await page.goto(publicOrigin);
    await page.getByRole('link', { name: 'Log in' }).click();
    await page.getByRole('button', { name: 'Continue as Alice' }).click();
    await page.waitForURL(publicOrigin + '/');
    assert.deepEqual(await (await context.request.get(`${publicOrigin}/private`)).json(), { subject: 'alice' });
    const cookies = await context.cookies();
    const sessionCookies = cookies.filter(c => c.name.startsWith('authjs.session-token'));
    assert.ok(sessionCookies.length > 0);
    assert.ok(sessionCookies.every(c => c.httpOnly && c.sameSite === 'Lax' && c.path === '/'));
    assert.ok(cookies.every(c => !c.name.includes('code_verifier') && !c.name.includes('state') && !c.name.includes('nonce')));
    console.log('PASS callback, PKCE/state/nonce cleanup, session cookie, protected route');
    // The real provider issues a 61-second access token. Cross the app's existing
    // 60-second refresh skew; no fake auth clock or forged session is involved.
    await new Promise(resolve => setTimeout(resolve, 2200));
    const refreshed = await context.request.post(`${publicOrigin}/api/auth/refresh`, { headers: { origin: publicOrigin } });
    assert.equal(refreshed.status(), 200, JSON.stringify({ url: refreshed.url(), body: await refreshed.text(), cookies: (await context.cookies()).map(({name, expires, path}) => ({name, expires, path})), privateStatus: (await context.request.get(`${publicOrigin}/private`)).status() }));
    const refreshBody = await refreshed.json();
    assert.equal(refreshBody.authenticated, true);
    assert.equal(refreshBody.hasRefreshToken, true);
    assert.equal(refreshBody.error, undefined);
    assert.ok(idp.refreshes() > 0);
    console.log('PASS delegated refresh against local OIDC token endpoint');
    const csrf = await context.request.post(`${publicOrigin}/logout`, { headers: { origin: 'https://attacker.invalid', 'content-type': 'application/x-www-form-urlencoded' }, data: '' });
    assert.equal(csrf.status(), 403);
    assert.equal((await context.request.get(`${publicOrigin}/private`)).status(), 200);
    idp.failRefresh();
    await new Promise(resolve => setTimeout(resolve, 2200));
    const failedRefresh = await context.request.post(`${publicOrigin}/api/auth/refresh`, { headers: { origin: publicOrigin } });
    assert.equal(failedRefresh.status(), 401);
    assert.equal((await failedRefresh.json()).error, 'RefreshAccessTokenError');
    assert.equal((await context.request.get(`${publicOrigin}/private`)).status(), 401);
    console.log('PASS failed refresh denies protected UI/API');
    await page.getByRole('button', { name: 'Log out' }).click();
    await page.waitForURL(publicOrigin + '/');
    assert.equal((await context.request.get(`${publicOrigin}/private`)).status(), 401);
    console.log('PASS cross-origin CSRF rejection and delegated logout');
    await context.close();
    idp.allowRefresh();
    // Removing the state/nonce cookies before callback must never establish a session.
    for (const removed of ['state', 'nonce']) {
      const bad = await browser.newContext(); const badPage = await bad.newPage();
      await badPage.goto(`${publicOrigin}/login`);
      await badPage.getByRole('button', { name: 'Continue as Alice' }).waitFor();
      await bad.clearCookies({ name: new RegExp(removed) });
      await badPage.getByRole('button', { name: 'Continue as Alice' }).click();
      await badPage.waitForURL(url => url.origin === publicOrigin);
      assert.equal((await bad.request.get(`${publicOrigin}/private`)).status(), 401);
      await bad.close();
      console.log(`PASS missing ${removed} rejects callback`);
    }
  } finally { await browser.close(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const fixture = await startFixture();
  try { await exerciseAuth(fixture); }
  catch (error) { console.error(fixture.logs()); throw error; }
  finally { await fixture.stop(); }
  const secure = await startFixture({ secureCookies: true });
  try {
    const response = await fetch(`${secure.uiOrigin}/login`, { redirect: 'manual' });
    assert.equal(response.status, 302);
    const cookies = response.headers.getSetCookie();
    assert.ok(cookies.length >= 3);
    assert.ok(cookies.every(cookie => /; Secure(?:;|$)/i.test(cookie)));
    console.log('PASS explicit secure-cookie policy survives Auth.js delegation');
  } finally { await secure.stop(); }
}
