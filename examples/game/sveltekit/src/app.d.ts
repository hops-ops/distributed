// See https://svelte.dev/docs/kit/types#app.d.ts
declare global {
	namespace App {}
}

declare module '@auth/sveltekit' {
	interface Session {
		/** Bearer for `mobpit-service`, bound to the verified session subject. */
		accessToken?: string;
	}
}

export {};
