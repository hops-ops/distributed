<script lang="ts">
	/**
	 * Distributed product home — need/story first, then how the framework delivers it.
	 */
	import '$lib/styles/home.css';
	import { page } from '$app/state';
	import Footer from '$lib/components/shared/Footer.svelte';
	import HmrBeacon from '$lib/components/HmrBeacon.svelte';
	import { highlightCode } from '$lib/components/walkthrough/highlight';
	import { env } from '$env/dynamic/public';

	const session = $derived(page.data.session);
	const celldProfile = $derived(env.PUBLIC_E2E_PROFILE === 'celld-nats');
	const signedIn = $derived(!!session?.user);
	const authConfigError = $derived(page.url.searchParams.get('error') === 'Configuration');

	const toc = [
		{ href: '#claim', label: 'The claim' },
		{ href: '#sota', label: 'The bar' },
		{ href: '#author', label: 'Backstory' },
		{ href: '#flow', label: 'How' },
		{ href: '#cqrs', label: 'CQRS' },
		{ href: '#aggregates', label: 'Aggregates' },
		{ href: '#read-models', label: 'Read models' },
		{ href: '#query-api', label: 'Query API' },
		{ href: '#projections', label: 'Projections' },
		{ href: '#service', label: 'Service crates' },
		{ href: '#replica', label: 'Replica' },
		{ href: '#sveltekit', label: 'SvelteKit' },
		{ href: '#oidc', label: 'OIDC' },
		{ href: '#try', label: 'Playground' }
	];

	const demos = [
		{ href: '/chat', title: 'Lobby chat', tag: 'Live + anonymous', blurb: 'A shared room with SSR, live updates, and guest reads. Same post on a Service or a cell — @live stays on GraphQL.' },
		{ href: '/todos', title: 'Todos', tag: 'Eventual · celld', blurb: 'Ownership rules, optimistic commands, projector fill. Same declarations on a Service or a cell.' },
		{ href: '/blob', tag: 'Atomic + WASM', title: 'Blob game', blurb: 'Atomic board in the response. Same domain pure runs as WASM in the replica.' },
		{ href: '/admin', title: 'Admin', tag: 'Surface', blurb: 'Elevated surface — separate client, more power.' },
		{ href: '/session', title: 'Session', tag: 'OIDC', blurb: 'Who you are to the app: tokens, groups, roles.' }
	];

	// —— Code samples from the living playground (trimmed for teaching) ——
	const codeDomain = `#[sourced(
    entity,
    events = "TodoEvent",
    aggregate_type = "todo",
    domain_state = TodoState,
)]
impl Todo {
    pub fn create(
        &mut self,
        todo_id: impl Into<String>,
        owner_id: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<(), TodoError> {
        // …validate…
        self.record_created(todo_id, owner_id, title)?;
        Ok(())
    }

    #[event("todo.created", version = 1, domain)]
    fn record_created(&mut self, todo_id: String, owner_id: String, title: String) {
        self.entity.set_id(&todo_id);
        self.todo_id = todo_id;
        self.owner_id = owner_id;
        self.title = title;
        self.status = TodoStatus::Open;
    }
}`;

	const codeReadModel = `#[derive(Clone, Debug, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
    #[readmodel(id)]
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    pub status: String,
}

impl AuthorizedReadModel for Todos {
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            )
            .grant("admin", read().all_columns())
    }
}`;

	const codeProjection = `// Abbreviated from todos.rs — event → mutation
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        epoch: "e2e-ui-todos-v2",
        model: Todos,
        on {
            events: [
                TodoCreatedDomainEvent,
                TodoCompletedDomainEvent,
                TodoArchivedDomainEvent,
                // … rename, reopen, reassign, force-archive
            ],
            mutation: SaveTodo,
            input: { todo: body },
        },
        on {
            events: [TodoPurgedDomainEvent],
            mutation: DeleteTodo,
            input: { todo_id: aggregate_id },
        },
    };
}`;

	const codeMutationGql = `# Syntax-only IR → MutationProgram (not a public GraphQL field).
# Same program applies to the SQL read model and the browser replica.
# Clients never call this — they send domain commands.
mutation SaveTodo {
  upsert_todos(object: $input.todo)
}`;

	const codeCommand = `// Handler: load aggregate → domain method → commit events
pub async fn handle(
    ctx: &CausalCommandContext<'_, Todo>,
    input: TodoArchiveInput,
) -> Result<PreparedCommand<Eventual<TodoArchivePayload>>, HandlerError> {
    let owner = ctx.user_id()?.to_string();
    let mut todo = ctx.repo()
        .get(&input.todo_id).await?
        .ok_or_else(|| HandlerError::NotFound(input.todo_id.clone()))?;
    todo.archive(&owner).map_err(rejected)?;

    let state = TodoState::from(&*todo);
    ctx.repo().publish_events().commit(todo)?.eventual(TodoArchivePayload {
        todo_id: state.todo_id,
        status: state.status,
    })
}`;

	const codePageQuery = `# Page declares the shape it needs — no hand-written query API
query Todos @load {
  todos(order_by: [{ status: asc }, { todo_id: asc }]) {
    todo_id
    owner_id
    title
    status
  }
}`;

	const codeUi = `// Generated operation + typed commands — no cache recipes in the page
import { Todos, useCommands } from '$distributed';

const query = Todos.use();
const commands = useCommands();
const todos = $derived($query.complete ? $query.data.todos : []);

await commands.todo.create({ title: text });
await commands.todo.complete({ todo_id });
// Replica applies SaveTodo (upsert_todos) to the cache. Page does not.`;

	const codeWasm = `// Advanced optimism: the same domain pure, shipped as WASM.
// blob-domain: typed row, arguments and output.
pub fn preview_move(row: MoveRow, args: MoveArgs) -> Option<MovePreview> {
    simulate_move(&row.map, row.score, args.direction).ok()
}
distributed::wasm_pure! {
    pub const PREVIEW_MOVE = preview_move(MoveRow, MoveArgs) -> MovePreview;
}

// The command: key, argument and patched columns come from the types.
preview_reduce_known_record:
    CommandProjectionPureReduce::known_record::<BlobGames>(PREVIEW_MOVE),

// distributed build compiles it; the generated client hosts it.
// No TypeScript board rules.`;

	const codeLive = `# The nested layout retains one live window across child pages.
query ChatMessages($limit: Int!, $offset: Int!) @load @live {
  chat_messages(
    where: { room_id: { _eq: "lobby" } }
    limit: $limit
    offset: $offset
    order_by: [{ created_at: desc }]
  ) {
    message_id
    body
    author { display_name }
  }
}`;

	const codeCqrs = `// Commands → aggregates (accept / reject business rules)
commands.todo.create({ title })
commands.todo.archive({ todo_id })

// Queries → SQL-shaped read models (never write tables)
query Todos @load {
  todos { todo_id title status }
}`;

	const codeService = `// compose.rs (trimmed). Each module takes the stores and the
// projections mounted from their descriptors.
pub fn build_service<St: ServiceStores>(stores: &St) -> Service {
  let projections = projections();
  Service::new()
    .named("e2e-ui")
    .routes(todo::routes(stores, &projections))
    .routes(chat::routes(stores, &projections))
    .routes(blob::routes(stores))
    .routes(identity::routes(stores, &projections))
}
// The application manifest's modules are derived from this Service.

// Another crate can list the same modules, or only Eventual projectors.
// You write that Service. You do not flip a Runtime::role flag.`;

</script>

<div class="wf-home dist-home">
	<section class="wf-hero">
		<div class="wf-hero-inner">
			{#if authConfigError}
				<div class="wf-auth-banner" role="alert">
					<strong>Identity provider unavailable</strong>
					<p>
						Sign-in needs Zitadel. From <code>tests/e2e-ui</code>: <code>make up</code>, then
						<code>source e2e-ui.env && make run</code>.
					</p>
				</div>
			{/if}

			<HmrBeacon />
			<h1>
				<strong>Distributed</strong> is a
				<em>state-of-the-art</em> framework
				for building distributed systems, and realtime applications.
			</h1>
			<p class="wf-lede">
				An end-to-end cloud native stack — domain, service, query edge, live client, and even gitops —
				so engineers who care about quality code can stay on the model and still ship polished, fast,
				maintainable products.
			</p>
			<p class="wf-lede">
				It is also a toolkit of distributed-systems tools. You do not need the whole path.
				Event-source aggregates and stop there. Use the service bus alone. Take GraphQL reads
				without the replica. Adopt what you need.
			</p>
			<p class="wf-lede">
				Distributed lets you define domain logic cleanly, then compose those pieces like
				blocks into one service or many — whatever suits your size. Change transports and
				sharding later as you grow.
			</p>

			<ul class="wf-hero-stack" aria-label="Stack">
				<li>Rust</li>
				<li>TypeScript</li>
				<li>CQRS / ES</li>
				<li>SvelteKit</li>
				<li>celld</li>
				<li>Kafka</li>
				<li>NATS</li>
				<li>RabbitMQ</li>
				<li>PSQL</li>
				<li>SQLite</li>
				<li>OIDC</li>
				<li>Keycloak</li>
				<li>Authentik</li>
			</ul>

			<div class="wf-actions">
				{#if signedIn}
					<a class="wf-btn wf-btn-primary" href="/chat">Open the playground</a>
					<a class="wf-btn wf-btn-ghost" href="#claim">Prove it</a>
				{:else}
					<a class="wf-btn wf-btn-primary" href="/chat">Browse the lobby</a>
					<a class="wf-btn wf-btn-ghost" href="#claim">Prove it</a>
				{/if}
			</div>

			<p class="dist-hero-note">
				{#if celldProfile}
					This session is <code>tests/e2e-celld</code>. Todo create/complete and lobby posts
					wait-dispatch to a cell. GraphQL <code>@live</code> and Eventual projectors stay in this
					process — that is the chat demo.
				{:else}
					This site is the living playground. Default is one process under
					<code>tests/e2e-ui</code>. The same UI can wait-dispatch Todo and Chat commands to celld
					from <code>tests/e2e-celld</code>.
				{/if}
			</p>

			<nav class="wf-toc" aria-label="On this page">
				{#each toc as item}
					<a href={item.href}>{item.label}</a>
				{/each}
			</nav>
		</div>
	</section>

	<section class="wf-band dist-claim-band" id="claim" aria-label="Open loop">
		<div class="wf-band-inner dist-claim-inner">
			<p class="dist-claim">
				“State of the art” is a strong claim… Here’s how we define it.
			</p>
			<div class="dist-claim-links">
				<a href="#sota">The bar</a>
				<span aria-hidden="true">·</span>
				<a href="#author">Backstory</a>
				<span aria-hidden="true">·</span>
				<a href="#flow">How it delivers</a>
			</div>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="sota">
		<div class="wf-band-inner">
			<div class="wf-section-head wf-section-head-wide">
				<span class="wf-label">State of the art</span>
				<h2>One path from command to live UI</h2>
				<p>
					You never get perfect consistency, always-available writes, and partition tolerance at once
					(<strong>CAP</strong>). Products that stay up accept <strong>eventual consistency</strong>
					on reads — with clear rules about what the user can trust now. The bar for the full
					product is not a glue job of excellent parts. It is one path from domain event to
					optimistic row. You can still take one part and ignore the rest.
				</p>
			</div>

			<div class="dist-teach">
				<div class="dist-teach-block">
					<h3>Event-driven backend</h3>
					<p>
						Command in, domain event out, projections update reads. The UI does not patch tables.
						<strong>CQRS</strong> keeps aggregates for rules and read models for screens.
						<strong>Event sourcing</strong> records what happened as history you can unit-test.
						Identity is OIDC and RBAC on the same claims — not a one-off check per endpoint.
					</p>
				</div>
				<div class="dist-teach-block">
					<h3>Compiler-owned frontend</h3>
					<p>
						You write Rust models, commands, and projections. The <strong>GraphQL schema</strong>,
						filters, typed operations, and command stubs are <strong>generated</strong> from those
						definitions — no resolvers, no hand-written query API. Pages only select fields.
						Writes stay <strong>commands</strong>.
					</p>
				</div>
				<div class="dist-teach-block">
					<h3>Replica cache + one effect</h3>
					<p>
						A client <strong>replica</strong> is a cache of the authorized slice. Auto-optimism
						applies the <strong>projection mutation</strong> to that cache — the same program the
						server projector runs against SQL. When the next row needs a known-record calculation,
						ship the domain <strong>pure as WASM</strong>; the generated client hosts it.
					</p>
				</div>
				<div class="dist-teach-block">
					<h3>Same blocks, few or many processes</h3>
					<p>
						Domain, modules, and projections are packages — not a deploy shape. A
						<strong>service crate</strong> lists the modules this process runs. Today this playground
						is one host. Later you write another Service from the same modules. Eventual projectors
						can move; Atomic seals stay with commands. The same Rust pures can compile to WASM for
						the replica.
					</p>
				</div>
			</div>
			<p class="dist-teach-foot">
				<strong>Distributed</strong> is that path when you want the whole product — one system so
				generation can keep the DX simple. The same crates stay usable as tools: aggregates,
				bus, outbox, locks, GraphQL, replica. Feature flags keep unused pieces out of the binary.
			</p>
			<div class="wf-actions dist-vehicle-actions">
				<a class="wf-btn wf-btn-primary" href="#author">How this project got here</a>
				<a class="wf-btn wf-btn-ghost" href="#flow">How it delivers</a>
			</div>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="author">
		<div class="wf-band-inner">
			<div class="wf-section-head wf-section-head-wide">
				<span class="wf-label">Backstory</span>
				<h2>Built by someone who’s lived the glue</h2>
				<p>
					I’m a multi-time CTO and long-time consultant on microservices and DevOps. Articles I’ve
					written have been read over a million times; thousands of people took free DevOps classes I
					ran. I’ve maintained <strong>sourced</strong> and <strong>servicebus</strong> in the Node
					ecosystem for nearly a decade, and I’ve been a student of domain-driven design since before
					CQRS/ES was the usual name for the write path — I was in the Google group when the name
					shifted, went to the conferences, and have helped teams from startups through enterprises
					actually ship this style of system.
				</p>
				<p>
					Early on I wired pieces together. <strong>Matt Walters</strong> was a mentor who taught me
					a great deal; he authored Node <code>sourced</code> and <code>servicebus</code>, which
					inspired parts of what this library does. Later, Knative Eventing replaced much of
					hand-rolled service-bus plumbing — messaging became more declarative, and language choice
					less tied to one library. For reads I used Hasura-style SQL: joins, RBAC, generated query
					APIs, opinionated for CQRS. It worked. It was still a kit of parts.
				</p>
				<p>
					I started <strong>Distributed</strong> in late 2024 (first commits October 2024) as AI
					became usable for real systems work — and as models got good enough that building the dream
					framework (everything in one coherent place) stopped being a multi-year solo fantasy. This
					playground is that system: domain through live UI, with the DX we always wanted.
				</p>
			</div>
			<div class="wf-actions dist-vehicle-actions">
				<a class="wf-btn wf-btn-primary" href="#flow">How the framework delivers</a>
				<a class="wf-btn wf-btn-ghost" href="#try">See the demos</a>
			</div>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="flow">
		<div class="wf-band-inner">
			<div class="wf-section-head wf-section-head-wide dist-how-head">
				<span class="wf-label">How Distributed provides it</span>
				<h2>One unidirectional loop — end to end</h2>
				<p>
					That’s the bar. Here’s the path that closes it: write the domain once, compose it into
					one Service or several, then generate the client. Each section below is a stage with real
					code from this playground.
				</p>
			</div>
			<article class="wf-story-step dist-flow-step">
				<div class="wf-story-copy">
					<span class="wf-label">01 · Unidirectional</span>
					<h2 class="wf-step-title">Changes go one way. There is order.</h2>
					<p class="wf-why">
						Front-end developers know this from Redux: dispatch in, state updates on a defined path,
						UI reads the result. Distributed is that idea for the <strong>whole system</strong>.
					</p>
					<p class="wf-why">
						Client → <strong>command</strong> → <strong>aggregate</strong> state change →
						<strong>domain event</strong> → <strong>projection</strong> → <strong>read model</strong>
						→ client. No dual-write from the UI. CAP and eventual consistency sit on the read side;
						optimistic UI is how the front end meets that honestly.
					</p>
				</div>
				<figure
					class="dist-flow-diagram"
					aria-label="Unidirectional system flow: client through GraphQL gateway, then clockwise commands, aggregate, domain event, projection, read model, back to gateway"
				>
					<div class="dist-flow-circle">
						<!--
							viewBox 360×420 · ring center (180, 220) r=118
							Nodes on the ring · short gap arcs (pad ±18°)
						-->
						<svg class="dist-flow-circle-svg" viewBox="0 0 360 420" aria-hidden="true">
							<defs>
								<marker
									id="dist-flow-arrow"
									viewBox="0 0 12 12"
									refX="10"
									refY="6"
									markerWidth="7"
									markerHeight="7"
									orient="auto"
									markerUnits="userSpaceOnUse"
								>
									<path d="M1 1.5 L11 6 L1 10.5 Z" class="dist-flow-arrowhead" />
								</marker>
							</defs>

							<!-- Client → gateway -->
							<path
								class="dist-flow-connector"
								d="M 180 48 L 180 84"
								marker-end="url(#dist-flow-arrow)"
							/>

							<!-- Gateway → Commands (18°→42°) -->
							<path
								class="dist-flow-connector"
								d="M 216.46 107.79 A 118 118 0 0 1 258.99 132.33"
								marker-end="url(#dist-flow-arrow)"
							/>
							<!-- Commands → Aggregate (78°→102°) -->
							<path
								class="dist-flow-connector"
								d="M 295.41 195.45 A 118 118 0 0 1 295.41 244.55"
								marker-end="url(#dist-flow-arrow)"
							/>
							<!-- Aggregate → Domain event (138°→162°) -->
							<path
								class="dist-flow-connector"
								d="M 258.99 307.67 A 118 118 0 0 1 216.46 332.21"
								marker-end="url(#dist-flow-arrow)"
							/>
							<!-- Domain event → Projection (198°→222°) -->
							<path
								class="dist-flow-connector"
								d="M 143.54 332.21 A 118 118 0 0 1 101.01 307.67"
								marker-end="url(#dist-flow-arrow)"
							/>
							<!-- Projection → Read model (258°→282°) -->
							<path
								class="dist-flow-connector"
								d="M 64.59 244.55 A 118 118 0 0 1 64.59 195.45"
								marker-end="url(#dist-flow-arrow)"
							/>
							<!-- Read model → Gateway (318°→342°) -->
							<path
								class="dist-flow-connector"
								d="M 101.01 132.33 A 118 118 0 0 1 143.54 107.79"
								marker-end="url(#dist-flow-arrow)"
							/>
						</svg>

						<span class="dist-flow-orbit dist-flow-orbit-client">
							<span class="dist-flow-chip dist-flow-chip-client">Client</span>
						</span>
						<span class="dist-flow-entry-meta" aria-hidden="true">query · command · live</span>
						<span class="dist-flow-orbit dist-flow-orbit-0">
							<span class="dist-flow-chip dist-flow-chip-hub">GraphQL gateway</span>
						</span>
						<span class="dist-flow-orbit dist-flow-orbit-1">
							<span class="dist-flow-chip">Commands</span>
						</span>
						<span class="dist-flow-orbit dist-flow-orbit-2">
							<span class="dist-flow-chip">Aggregate</span>
						</span>
						<span class="dist-flow-orbit dist-flow-orbit-3">
							<span class="dist-flow-chip">Domain event</span>
						</span>
						<span class="dist-flow-orbit dist-flow-orbit-4">
							<span class="dist-flow-chip">Projection</span>
						</span>
						<span class="dist-flow-orbit dist-flow-orbit-5">
							<span class="dist-flow-chip dist-flow-chip-rm">Read model</span>
						</span>

						<span class="dist-flow-circle-core" aria-hidden="true">
							<span class="dist-flow-core-label">one way</span>
						</span>
					</div>
				</figure>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="cqrs">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">02 · CQRS</span>
					<h2 class="wf-step-title">Decisions and views are different models</h2>
					<p class="wf-why">
						In the business, “complete this todo” is a decision with rules. “Show my open todos”
						is a question about a list. CQRS keeps those as separate models: commands load
						aggregates; queries hit a SQL-shaped read model. You avoid forcing both into “update a
						row,” so domain code stays about rules and screens stay about presentation — and both
						get simpler.
					</p>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>commands vs queries</span>
							<em>api shape</em>
						</div>
						<pre><code>{@html highlightCode(codeCqrs)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="aggregates">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">03 · Event-sourced aggregates</span>
					<h2 class="wf-step-title">Business rules as plain types — with history</h2>
					<p class="wf-why">
						Express the business as ordinary Rust structs and methods: who may do what, what state
						is allowed next. Under the hood that’s event sourcing — repository, append-only
						events, optional upcasters — so you get a timeline and easy unit tests without putting
						rules in SQL or HTTP. The domain stays the place where expertise lives.
					</p>
					<span class="wf-sample-path">tests/e2e-ui/crates/todo-domain/src/models/todo.rs</span>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>todo.rs</span>
							<em>domain</em>
						</div>
						<pre><code>{@html highlightCode(codeDomain)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="read-models">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">04 · SQL read models + RBAC</span>
					<h2 class="wf-step-title">What the user is allowed to see</h2>
					<p class="wf-why">
						Screens need tables: lists, filters, joins. Read models are that query shape, with
						row/column permissions next to the model — “owner sees only their todos,” “admin sees
						all.” SQL does what it’s good at; auth is not a second story bolted on later. Queries
						and commands share the same idea of who the actor is.
					</p>
					<span class="wf-sample-path">tests/e2e-ui/crates/readmodels/src/models/todos.rs</span>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>todos.rs</span>
							<em>read model</em>
						</div>
						<pre><code>{@html highlightCode(codeReadModel)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="query-api">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">05 · Inferred query API</span>
					<h2 class="wf-step-title">Rust models generate GraphQL</h2>
					<p class="wf-why">
						The read model, permissions, and command contracts in Rust are the source. Distributed
						<strong>generates</strong> the GraphQL schema — filters, order, pagination, joins,
						RBAC, and command mutations. You do not write resolvers or a REST endpoint per screen.
					</p>
					<p class="wf-why">
						The page file only selects fields against that generated schema. Commands stay domain
						verbs on the write side. The typed TypeScript client is generated from the same
						inventory.
					</p>
					<span class="wf-sample-path">tests/e2e-ui/ui/src/routes/todos/+page.graphql</span>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>+page.graphql</span>
							<em>selection</em>
						</div>
						<pre><code>{@html highlightCode(codePageQuery)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="projections">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">06 · Projections</span>
					<h2 class="wf-step-title">One mutation. Two runtimes.</h2>
					<p class="wf-why">
						After a command succeeds, events describe what happened. A projection names the
						<strong>effect</strong>: on these events, run this mutation program
						(<code>upsert_todos</code>, <code>delete_todos_by_pk</code>). That program is the
						update — not a second cache language on the page.
					</p>
					<p class="wf-why">
						The same mutation runs in two places: the <strong>server projector</strong> writes the
						SQL read model; the <strong>client replica</strong> applies it to the cache for
						auto-optimism. The mutation file looks like GraphQL but is internal IR, not a public
						client field. Pages still send domain commands.
					</p>
					<span class="wf-sample-path"
						>tests/e2e-ui/crates/projections/src/todos.rs · mutations/save_todo.mutation.graphql</span
					>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>todos.rs</span>
							<em>projection</em>
						</div>
						<pre><code>{@html highlightCode(codeProjection)}</code></pre>
					</div>
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>save_todo.mutation.graphql</span>
							<em>mutation ir</em>
						</div>
						<pre><code>{@html highlightCode(codeMutationGql)}</code></pre>
					</div>
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>todo_archive.rs</span>
							<em>command handler</em>
						</div>
						<pre><code>{@html highlightCode(codeCommand)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="service">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">07 · Service crates</span>
					<h2 class="wf-step-title">Compose the process. Keep the domain still.</h2>
					<p class="wf-why">
						A module mounts one bounded context — commands, guards, projectors. A
						<strong>service crate</strong> lists those modules. That list <em>is</em> the process:
						this playground is one Service, one host, one runner that only reads env and calls
						<code>run</code>. You do not set a runtime role flag.
					</p>
					<p class="wf-why">
						Todo commands are <code>command!</code> declarations in
						<code>todo-domain</code>. This playground mounts them on a local Service. The sibling
						example <code>tests/e2e-celld</code> mounts the same declarations and wait-dispatches
						create, complete, and <code>chat.post</code> to a <strong>cell</strong> (one private
						SQLite per todo or message). GraphQL <code>@live</code> and Eventual projectors stay off
						the cell.
					</p>
					<p class="wf-why">
						The same packages can back a different Service later: all modules in one binary, or
						commands here and Eventual projectors there. <strong>Atomic</strong> work (blob’s board
						seal) stays with the command process. <strong>Eventual</strong> work can split. Topology
						is explicit composition — not a hidden matrix.
					</p>
					<span class="wf-sample-path">tests/e2e-ui/crates/service/src/modules/compose.rs</span>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>compose.rs</span>
							<em>service</em>
						</div>
						<pre><code>{@html highlightCode(codeService)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="replica">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">08 · Browser replica</span>
					<h2 class="wf-step-title">Auto-optimism is a cache update</h2>
					<p class="wf-why">
						The generated client is a <strong>replica cache</strong> of the authorized read-model
						slice, plus typed commands. The page reads <code>query.use()</code> and calls
						<code>commands.todo…</code>. It does not patch arrays or write
						<code>setState</code> recipes.
					</p>
					<p class="wf-why">
						When a command fires, the replica applies the <strong>same projection mutation</strong>
						to the cache immediately. The server later writes SQL with that program; live/causal
						confirmation reconciles. Most rows are input + defaults + claims. When the next row
						needs the known record (blob’s next board), ship the domain <strong>pure function as
						WASM</strong>. Gen-client hosts it. Do not write a TypeScript twin.
					</p>
					<span class="wf-sample-path"
						>tests/e2e-ui/ui/src/routes/todos/+page.svelte · crates/service/src/modules/blob.rs</span
					>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>+page.svelte</span>
							<em>ui</em>
						</div>
						<pre><code>{@html highlightCode(codeUi)}</code></pre>
					</div>
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>blob.rs</span>
							<em>wasm pure</em>
						</div>
						<pre><code>{@html highlightCode(codeWasm)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="sveltekit">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">09 · SvelteKit GraphQL islands</span>
					<h2 class="wf-step-title">Put each query where its UI ownership lives</h2>
					<p class="wf-why">
						A page island is replaced with its page. This chat <strong>layout island</strong> stays
						retained across <code>/chat</code> and <code>/chat/about</code>, so one
						<code>@load @live</code> operation owns SSR, hydration, navigation, and its live feed.
					</p>
					<p class="wf-why">
						Reusable components can own islands too. The blob demo imports
						<code>SelectedBlobGame.graphql</code>; generation promotes it to the nearest static
						route boundary and binds <code>[[gameId]]</code> once for SSR and component reads.
					</p>
					<span class="wf-sample-path">tests/e2e-ui/ui/src/routes/chat/+layout.graphql · src/lib/components/blob/SelectedBlobGame.graphql</span>
				</div>
				<div class="wf-code-stack">
					<div class="wf-code">
						<div class="wf-code-bar">
							<span>+layout.graphql</span>
							<em>retained load + live</em>
						</div>
						<pre><code>{@html highlightCode(codeLive)}</code></pre>
					</div>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="oidc">
		<div class="wf-band-inner">
			<article class="wf-story-step">
				<div class="wf-story-copy">
					<span class="wf-label">10 · OIDC</span>
					<h2 class="wf-step-title">Who the user is — in the model and the UI</h2>
					<p class="wf-why">
						Real products need real identity. OIDC is first-class (Zitadel here; Keycloak and
						Authentik in tests). Sessions and JWTs become claims the domain already uses for
						ownership and roles — the same claims that scope the client replica.
					</p>
				</div>
				<div class="dist-pillars">
					<article class="dist-pillar">
						<h3>Claims → RBAC</h3>
						<p>Row filters and command handlers share claims like <code>x-user-id</code> and roles.</p>
					</article>
					<article class="dist-pillar">
						<h3>Surfaces</h3>
						<p>User, admin, and public clients stay separate so elevated power does not leak.</p>
					</article>
					<article class="dist-pillar">
						<h3>Try it</h3>
						<p>
							<a class="dist-inline-link" href="/session">Session</a> ·
							<a class="dist-inline-link" href="/signin?callbackUrl=/todos">sign in</a>
							(alice / bob / admin)
						</p>
					</article>
				</div>
			</article>
		</div>
	</section>

	<section class="wf-band wf-band-dark" id="try">
		<div class="wf-band-inner">
			<div class="wf-section-head">
				<span class="wf-label">Playground</span>
				<h2>Small apps, full patterns</h2>
				<p>
					Real features under <code>tests/e2e-ui</code> — chat, todos, a game, admin. Each has
					<strong>How it is built</strong>: query, then command, then handler, then domain, then events, then service and host.
					Todos and Chat also run against celld from <code>tests/e2e-celld</code> with the same
					domain crates. Chat is the small cell that still proves <code>@live</code>.
				</p>
			</div>
			<div class="dist-demo-grid">
				{#each demos as d}
					<a class="dist-demo-card" href={d.href}>
						<span class="dist-demo-tag">{d.tag}</span>
						<h3>{d.title}</h3>
						<p>{d.blurb}</p>
						<span class="dist-demo-go">Open →</span>
					</a>
				{/each}
			</div>
		</div>
	</section>

	<section class="wf-band wf-band-light" id="run">
		<div class="wf-band-inner dist-run">
			<div class="wf-section-head">
				<span class="wf-label">Local</span>
				<h2>Run the playground</h2>
				<p>From the repository:</p>
			</div>
			<div class="dist-run-code">
				<pre><code>{`# Default: one process (SQLite or Postgres + bus)
cd tests/e2e-ui
make up                    # Postgres + Zitadel → e2e-ui.env
source e2e-ui.env && make run

# Optional: same UI, Todo + chat.post on celld (@live stays on GraphQL)
cd tests/e2e-ui && make up && make up-celld-nats
cd ../e2e-celld && make run
# UI  http://localhost:5180
# API http://127.0.0.1:8791`}</code></pre>
			</div>
			<p class="dist-run-hint dist-run-hint-light">
				Demo logins after <code>make up</code>: <code>alice</code> / <code>bob</code> /
				<code>admin</code> · <code>Password1!</code>
			</p>
		</div>
	</section>

	<section class="wf-band wf-band-dark dist-closing">
		<div class="wf-band-inner">
			<div class="wf-section-head">
				<span class="wf-label">Next</span>
				<h2>Model the domain. Leave the rest to the stack.</h2>
				<p>
					Pick a demo close to your problem — todos for ownership and rules, chat for live rooms —
					and reuse the shapes. The vehicle is one framework when you want the full path. It is
					also a toolkit: start with aggregates, or the bus, and leave the rest. Fleet hosting
					(<strong>ops.com.ai</strong>) is on the roadmap.
				</p>
			</div>
			<div class="wf-actions">
				<a class="wf-btn wf-btn-primary" href="/chat">Start with chat</a>
				<a class="wf-btn wf-btn-ghost" href="/todos">Or todos</a>
			</div>
		</div>
	</section>

	<Footer />
</div>
