<script lang="ts">
	let { data } = $props();
</script>

<main>
	{#if data.session?.user}
		<p>Signed in as <strong>{data.session.user.name}</strong>.</p>
		<a href="/pit" data-testid="enter">Enter the pit →</a>
	{:else}
		<h1>Sign in</h1>
		<!-- Auth.js credentials sign-in (dev only, GAME_DEV_AUTH=1). -->
		<form method="POST">
			<input type="hidden" name="providerId" value="dev" />
			<input type="hidden" name="redirectTo" value="/pit" />
			<label>Username <input name="username" required pattern="[a-z][a-z0-9_\-]{'{'}2,15{'}'}" /></label>
			<button data-testid="signin">Sign in</button>
		</form>
	{/if}
</main>

<style>
	main { max-width: 32rem; margin: 3rem auto; padding: 0 1rem; }
	a { color: #ffd166; }
	input { background: #1a1620; color: inherit; border: 1px solid #4a4155; border-radius: 4px; padding: 0.25rem; }
</style>
