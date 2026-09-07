import { Provider } from 'oidc-provider';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { randomBytes } from 'node:crypto';

// An isolated, in-memory standards implementation. No external IdP or secrets.
export async function startProvider(issuer, publicOrigin) {
  let refreshes = 0;
  let failRefresh = false;
  const provider = new Provider(issuer, {
    clients: [{ client_id: 'gateway-fixture', client_secret: 'local-fixture-only',
      redirect_uris: [`${publicOrigin}/auth/callback/oidc`],
      response_types: ['code'], grant_types: ['authorization_code', 'refresh_token'],
      token_endpoint_auth_method: 'client_secret_basic' }],
    cookies: { keys: [randomBytes(32).toString('hex')] },
    features: { devInteractions: { enabled: false } },
    ttl: { AccessToken: 61 },
    async issueRefreshToken() { return true; },
    claims: { openid: ['sub'], profile: ['name'], email: ['email'] },
    async findAccount(_ctx, id) {
      return { accountId: id, async claims() { return { sub: id, name: 'Alice', email: 'alice@example.invalid' }; } };
    },
    interactions: { url(_ctx, interaction) { return `/interaction/${interaction.uid}`; } },
  });
  provider.use(async (ctx, next) => {
    if (ctx.path === '/token' && ctx.method === 'POST') {
      // The grant event below counts actual successful refreshes.
      if (failRefresh) { ctx.status = 400; ctx.body = { error: 'invalid_grant' }; return; }
    }
    await next();
  });
  provider.on('grant.success', ctx => { if (ctx.oidc.params.grant_type === 'refresh_token') refreshes++; });
  const server = createServer(async (req, res) => {
    try {
      if (req.url.startsWith('/interaction/')) {
        const details = await provider.interactionDetails(req, res);
        if (req.method === 'GET') {
          res.setHeader('content-type', 'text/html');
          res.end('<form method="post"><button>Continue as Alice</button></form>'); return;
        }
        const grant = details.grantId ? await provider.Grant.find(details.grantId) : new provider.Grant({ accountId: 'alice', clientId: details.params.client_id });
        grant.addOIDCScope('openid profile email offline_access');
        const grantId = await grant.save();
        await provider.interactionFinished(req, res, { login: { accountId: 'alice' }, consent: { grantId } }, { mergeWithLastSubmission: true });
        return;
      }
      provider.callback()(req, res);
    } catch { res.statusCode = 500; res.end('fixture provider failure'); }
  });
  server.listen(Number(new URL(issuer).port), '127.0.0.1');
  await once(server, 'listening');
  return { server, refreshes: () => refreshes, failRefresh: () => { failRefresh = true; }, allowRefresh: () => { failRefresh = false; } };
}
