import { error } from '@sveltejs/kit';
import { distributed } from '$lib/server/distributed';
import { createAuthRefreshHandler } from '$lib/server/auth-refresh';

export const POST = createAuthRefreshHandler(async (event, session) => {
	const { locals } = event;
	// A refresh may change the bearer credential without changing effective
	// permissions. Supply a new, independently authorized route seed so the
	// browser can prove same-scope reuse before fencing the old credential.
	// Route selection is input, never authority: the generated user surface
	// and GraphQL executor still authorize every selected operation.

	if (event.request.headers.get('content-type')?.includes('application/json')) {
		let route;
		try {
			route = await event.request.json();
		} catch {
			error(400, 'Invalid route');
		}
		if (
			typeof route?.id !== 'string' ||
			typeof route?.path !== 'string' ||
			!route.path.startsWith('/') ||
			route.path.startsWith('//') ||
			route.params === null ||
			typeof route.params !== 'object' ||
			Array.isArray(route.params) ||
			Object.values(route.params).some(value => typeof value !== 'string')
		) {
			error(400, 'Invalid route');
		}
		const url = new URL(route.path, event.url.origin);
		if (url.origin !== event.url.origin) {
			error(400, 'Invalid route');
		}
		return distributed.load({
			...event,
			locals: { ...locals, auth: async () => session },
			isDataRequest: false,
			url,
			route: { id: route.id },
			params: route.params
		});
	}
});
