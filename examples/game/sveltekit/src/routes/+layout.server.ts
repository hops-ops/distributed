export const load = async ({ locals }) => ({ session: await locals.auth() });
