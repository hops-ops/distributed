import { mkdir, copyFile, writeFile } from 'node:fs/promises';
// Exercise the app's actual Auth.js configuration and shared refresh handler.
// This auth-only composition does not bind an application GraphQL route loader.
for (const file of ['auth.ts', 'lib/clean-env.ts', 'lib/roles.ts', 'lib/server/oidc-scopes.ts', 'lib/server/oidc-start.ts', 'lib/server/require-auth.ts', 'lib/server/auth-refresh.ts']) {
  const target = `.generated/${file}`;
  await mkdir(target.substring(0, target.lastIndexOf('/')), { recursive: true });
  await copyFile(`../e2e-ui/ui/src/${file}`, target);
}
await mkdir('src/routes/api/auth/refresh', { recursive: true });
await writeFile('.generated/refresh.ts', `import { createAuthRefreshHandler } from '$lib/server/auth-refresh';
export const POST = createAuthRefreshHandler();
`);
