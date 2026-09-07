import { mkdir, copyFile } from 'node:fs/promises';
// Exercise the app's actual Auth.js configuration and refresh handler.
for (const file of ['auth.ts', 'lib/clean-env.ts', 'lib/roles.ts', 'lib/server/oidc-scopes.ts', 'lib/server/oidc-start.ts', 'lib/server/require-auth.ts']) {
  const target = `.generated/${file}`;
  await mkdir(target.substring(0, target.lastIndexOf('/')), { recursive: true });
  await copyFile(`../e2e-ui/ui/src/${file}`, target);
}
await mkdir('src/routes/api/auth/refresh', { recursive: true });
await copyFile('../e2e-ui/ui/src/routes/api/auth/refresh/+server.ts', '.generated/refresh.ts');
