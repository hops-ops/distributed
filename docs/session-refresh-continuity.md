# Session refresh continuity

The server loader must pair a credential with independently authorized route
data on both document and SvelteKit data requests. A follow-up `invalidateAll`
request can rotate the credential again, or observe a cookie renewed by another
request. Returning only that credential leaves the browser no proof that its
existing replica scope remains authorized.

Each request executes only its compiler-selected, bounded route/layout loads.
Routes without selections do no GraphQL work. This deliberately removes the
previous blanket data-request shortcut; data navigation remains client-side,
but selected reads run on the server to obtain fresh authority. There is no
token-derived scope, cached-proof reuse, or authorization fallback.

The client keeps its existing rules: a fresh exact-scope transfer can rotate
transports without dropping rows or newer command state; missing, replayed,
tampered or different-scope authority still purges. SSR request isolation and
Auth.js request-local session memoization remain required. The gateway's original
Todo/Chat MutationObserver assertion is unchanged and detects transient removals,
not merely eventual recovery.
