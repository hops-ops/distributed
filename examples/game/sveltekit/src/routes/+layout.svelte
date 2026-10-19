<script lang="ts">
	let { data, children } = $props();
</script>

<header>
	<a href="/">Mob Pit · SvelteKit</a>
	{#if data.session?.user}
		<span data-testid="user">{data.session.user.name}</span>
		<form method="POST" action="/signout"><button>Sign out</button></form>
	{/if}
</header>
{@render children()}

<style>
	:global(body) { margin: 0; background: #0d0b10; color: #e6e1ea; font: 14px/1.4 system-ui, sans-serif; }
	header { display: flex; gap: 1rem; align-items: center; padding: 0.5rem 1rem; border-bottom: 1px solid #2b2533; }
	header a { color: inherit; font-weight: 600; text-decoration: none; margin-right: auto; }
	button { background: #2b2533; color: inherit; border: 1px solid #4a4155; border-radius: 4px; padding: 0.25rem 0.6rem; cursor: pointer; }
</style>
