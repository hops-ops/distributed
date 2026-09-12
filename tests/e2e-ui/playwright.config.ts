import { defineConfig, devices } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));

/** Load KEY=VALUE from e2e-ui.env without treating values as shell. */
function loadEnvFile(file: string) {
	if (!fs.existsSync(file)) return;
	for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
		const t = line.trim();
		if (!t || t.startsWith('#')) continue;
		const eq = t.indexOf('=');
		if (eq < 0) continue;
		const key = t.slice(0, eq).trim();
		let val = t.slice(eq + 1).trim();
		if (
			(val.startsWith('"') && val.endsWith('"')) ||
			(val.startsWith("'") && val.endsWith("'"))
		) {
			val = val.slice(1, -1);
		}
		if (process.env[key] === undefined) process.env[key] = val;
	}
}

loadEnvFile(path.join(root, 'e2e-ui.env'));

const baseURL = process.env.PUBLIC_ORIGIN || process.env.E2E_UI_ORIGIN || process.env.UI_URL || 'http://localhost:8791';

/**
 * Browser tests for e2e-ui.
 *
 * Requires a live stack (Postgres + Zitadel + API + UI):
 *   make up && make run
 * Then:
 *   npm install && npx playwright install chromium
 *   npm run test:browser
 */
export default defineConfig({
	testDir: './e2e',
	fullyParallel: false,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 1 : 0,
	workers: 1,
	timeout: 90_000,
	expect: { timeout: 20_000 },
	reporter: [['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]],
	use: {
		baseURL,
		trace: 'on-first-retry',
		screenshot: 'only-on-failure',
		video: 'retain-on-failure',
		// Local Auth.js cookies over HTTP
		ignoreHTTPSErrors: true
	},
	projects: [
		{
			name: 'setup-alice',
			testMatch: /auth\.alice\.setup\.ts/
		},
		{
			name: 'setup-admin',
			testMatch: /auth\.admin\.setup\.ts/
		},
		{
			name: 'chromium-user',
			dependencies: ['setup-alice'],
			testMatch: /.*\.user\.spec\.ts/,
			use: {
				...devices['Desktop Chrome'],
				storageState: path.join(root, 'e2e/.auth/alice.json')
			}
		},
		{
			name: 'chromium-admin',
			dependencies: ['setup-admin'],
			testMatch: /.*\.admin\.spec\.ts/,
			use: {
				...devices['Desktop Chrome'],
				storageState: path.join(root, 'e2e/.auth/admin.json')
			}
		},
		{
			name: 'chromium-anon',
			testMatch: /.*\.anon\.spec\.ts/,
			use: { ...devices['Desktop Chrome'] }
		}
	]
});
