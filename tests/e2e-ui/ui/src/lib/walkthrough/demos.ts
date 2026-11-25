import type { DemoWalkthrough, WalkthroughSample } from './types';

/**
 * Tab order is the teaching order for every demo. Write panel copy in ASD-STE100.
 * One idea in each sentence. Use the active voice. Do not use metaphor or idiom.
 * Tell a developer or agent: what this layer does, which file to write, what not to do.
 *
 * 1. Query / live subscription (read-model shape + read RBAC)
 * 2. Commands (replica preview vs Atomic / pure reduce + write RBAC)
 * 3. Command handlers (principal, repository, aggregate, commit)
 * 4. Domain model (Rust aggregate; blob also has a pure core)
 * 5. Domain events and projections
 * 6. Service, host, and runner (e2e-service modules/compose/host; e2e-runner binary)
 *
 * Samples must be real shapes from the fixture. Do not show comment-only stubs.
 * Query tabs must include the read-model struct, not only permissions or GraphQL.
 *
 * Service tab must always show:
 * - this demo modules/*.rs slice (routes)
 * - compose.build_service (how modules join one Service)
 * - host.run (database, bus, workers, GraphQL serve)
 * - runner main (environment → run)
 *
 * Same mutation program. Different apply site:
 * - Eventual: the event handler applies the IR after commit. The client shows a
 *   preview until projection obligations complete. There is no response row.
 * - Atomic: the command handler applies the IR in the same transaction. The
 *   response includes the row. The client can wait for that row.
 * - Known-record pure (blob move): the client runs the domain pure function in
 *   WASM to update the replica. Atomic commit on the server is still authority.
 */

/** Shared host/runner samples — same process story on every demo’s Service tab. */
const hostAndRunnerSamples: WalkthroughSample[] = [
	{
		file: 'e2e-service · host.rs · run',
		caption: 'Call `run` to start one process. The host selects SQLite or Postgres, builds the service, starts the workers, and serves GraphQL with OIDC.',
		code: `// host.rs — library API used by the runner binary
pub async fn run(
  database_url: &str,
  options: HostOptions, // bind, identity, origins, gateway delivery
) -> Result<(), Box<dyn Error + Send + Sync>> {
  // postgres:// → PostgresStores, else SqliteStores; one host for both.
  host(SqliteStores::connect(database_url).await?, options).await
}

async fn host<St: SqlStores>(stores: St, options: HostOptions) -> Result<(), BoxError> {
  stores.bootstrap_read_models(read_models().catalog()).await?;
  // with_bus also drains the outbox to the bus.
  let service = build_service(&stores).with_bus(stores.bus("e2e-ui"));
  let gql = build_graphql_engine_with_delivery(
    stores.graphql_pool(), &service, options.identity.clone(), Some(change_rx), versions,
  )?;
  let service = Arc::new(service.try_with_graphql(gql)?);

  spawn_service_consumer_loop(move || build_service(&stores).with_bus(stores.bus("e2e-ui")));
  spawn_zitadel_scrape(service.clone()); // only when Zitadel env is set

  serve(service, &options).await
}`
	},
	{
		file: 'e2e-runner · main.rs',
		caption: 'The runner is a small binary. It reads the environment. Then it calls `run`. Do not put domain or module code in the runner.',
		code: `// crates/runner — bin e2e-ui
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
  let database_url = env::var("DATABASE_URL")
    .unwrap_or_else(|_| "sqlite:./e2e-ui.db?mode=rwc".into());
  let bind = env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8791".into());
  run(
    &database_url,
    HostOptions {
      bind,
      identity: identity_from_env(), // OidcBearer or DevHeaders
      public_origin: env::var("PUBLIC_ORIGIN")
        .unwrap_or_else(|_| "http://localhost:8791".into()),
      ui_origin: env::var("UI_INTERNAL_ORIGIN")
        .unwrap_or_else(|_| "http://localhost:5180".into()),
      delivery, // GATEWAY_DELIVERY=none|all
    },
  )
  .await
}`
	}
];

export const todosWalkthrough: DemoWalkthrough = {
	id: 'todos',
	href: '/todos',
	title: 'Todos',
	kicker: 'Query, then command, then domain, then projection',
	summary:
		'This page loads one GraphQL query into the client replica. Todo commands are Eventual. The client shows a safe preview from the command input and the domain transition. The event handler applies the same mutation program later. The client waits for projection obligations. The client does not wait for a response row. The same command! declarations mount on a local Service (tests/e2e-ui) or wait-dispatch every Todo transition to a cell (tests/e2e-celld).',
	tabs: [
{
			id: 'query',
			label: '1 · Query',
			lede: 'The browser reads a declared read model through one GraphQL document. The `@load` directive loads the data for SSR. The generated hook binds the same document to the replica. Put the row shape and the row filters on the read model. Do not write a WHERE clause in the UI.',
			principle: 'Use one replica for user data.',
			samples: [
				{
					file: 'routes/todos/+page.graphql',
					caption: 'The server loads this document once. `Todos.use()` continues to watch the replica.',
					code: `query Todos @load {
  todos(order_by: [{ status: asc }, { todo_id: asc }]) {
    todo_id
    owner_id
    title
    status
  }
}`
				},
				{
					file: 'readmodels/models/todos.rs · Todos',
					caption: 'This is the query row. The plural name sets the table to `todos`. `belongs_to` joins `AuthUsers`.',
					code: `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
  #[readmodel(id)]
  pub todo_id: String,
  pub owner_id: String,
  pub title: String,
  /// open | completed | archived
  pub status: String,
  pub assignee_id: Option<String>,
  #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
  pub owner: Option<AuthUsers>,
}`
				},
				{
					file: 'readmodels/models/todos.rs · permissions',
					caption: 'A user can read only the rows that the user owns. An admin can read all rows.',
					code: `impl AuthorizedReadModel for Todos {
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
}`
				},
				{
					file: 'routes/todos/+page.svelte',
					caption: 'The UI reads from the client replica. Do not create a local store for this list.',
					code: `import { Todos, useCommands } from '$distributed';

const query = Todos.use();
const todos = $derived($query.complete ? $query.data.todos : []);`
				}
			]
		},
{
			id: 'commands',
			label: '2 · Commands',
			lede: 'The UI writes through generated commands. Todo commands are Eventual. The client writes a safe preview into the replica. The UI updates from that replica. The preview becomes confirmed when the projection obligation completes. Domain crates declare the commands. Hosts only mount them.',
			principle: 'Let the service declare how the UI updates after a command.',
			samples: [
				{
					file: 'routes/todos/+page.svelte',
					caption: 'Call the generated command. The replica cache updates the UI. Do not change local component state for this list.',
					code: `const commands = useCommands();

await commands.todo.create({ title: text });
await commands.todo.complete({ todo_id });`
				},
				{
					file: 'todo-domain/src/commands/complete.rs',
					caption: 'A thin portable command is shard + invoke + Eventual. The host does not write a handler body.',
					code: `command! {
  name: "todo.complete",
  transition: domain_commands::Complete,
  aggregate: Todo,
  input: TodoCompleteInput,
  outcome: Eventual<TodoStatusPayload>,
  shard: |input| input.todo_id.clone(),
  load: required,
  roles: ["user", "admin"],
  field: "todos_complete",
  invoke: |todo, _input, principal| todo.complete(principal),
  payload: |todo, _principal| TodoStatusPayload::from_todo(&**todo),
}`
				},
				{
					file: 'modules/todo.rs · mount',
					caption: 'The service (or cell) mounts the same domain values. Roles and handlers live in todo-domain.',
					code: `Routes::for_aggregate(stores)
  .mount(todo_domain::commands::create())
  .mount(todo_domain::commands::complete())
  .mount(todo_domain::commands::force_archive())`
				}
			]
		},
{
			id: 'handlers',
			label: '3 · Handlers',
			lede: 'Todo commands are thin: load or create, invoke one domain method, commit Eventual. The framework writes that handler. A command that needs more, like `chat.post`, names a handle function. Either way, ctx.repo() is the capability. The host is SQLite, Postgres, or a cell.',
			principle: 'A command changes the write model. A table is only for reads.',
			samples: [
				{
					file: 'todo-domain/src/commands/complete.rs · thin',
					caption: 'No CausalCommandContext body. The mount loads by shard, invokes the domain, and commits Eventual.',
					code: `command! {
  name: "todo.complete",
  // …
  load: required,
  invoke: |todo, _input, principal| todo.complete(principal),
  payload: |todo, _principal| TodoStatusPayload::from_todo(&**todo),
}`
				},
				{
					file: 'todo-domain/src/commands/create.rs · thin',
					caption: 'Still thin. `load: create` rejects an id that already has a todo. The principal comes from the session, never from the input.',
					code: `command! {
  name: "todo.create",
  // …
  load: create,
  guard: authenticated_user,
  invoke: |todo, input, principal| todo.create(&input.todo_id, principal, &input.title),
  payload: |todo, _principal| { /* TodoCreatePayload from the todo */ },
  defaults: command_input_defaults! {
    input: TodoCreateInput;
    default input.todo_id = uuid_v7();
  },
}`
				}
			]
		},
{
			id: 'domain',
			label: '4 · Domain',
			lede: 'The write model is a Rust aggregate. The fields are the consistency boundary. Public methods enforce the rules. Private `#[event]` functions record history. You can unit-test this crate without HTTP or SQL.',
			principle: 'Start with the domain. Do not start with the database.',
			samples: [
				{
					file: 'todo-domain · Todo',
					caption: 'This aggregate is the consistency boundary for one todo.',
					code: `#[derive(Debug, Clone, Default, Serialize, Deserialize, Snapshot)]
#[snapshot(id = "todo_id")]
pub struct Todo {
  #[serde(skip, default)]
  pub entity: Entity,
  pub todo_id: String,
  pub owner_id: String,
  pub title: String,
  pub status: TodoStatus, // open | completed | archived
  pub assignee_id: Option<String>,
  purged: bool,
  snapshot_generation: u64,
}

#[sourced(
  entity,
  events = "TodoEvent",
  aggregate_type = "todo",
  domain_state = TodoState,
)]
impl Todo { /* create, complete, rename, archive, … */ }`
				},
				{
					file: 'todo-domain · Todo::complete',
					caption: 'One command path: validate the rules, then record the domain event.',
					code: `pub fn complete(&mut self, owner_id: &str) -> Result<(), TodoError> {
  self.ensure_owner(owner_id)?;
  self.require_mutable()?;
  if matches!(self.status, TodoStatus::Completed) {
    return Err(TodoError::AlreadyCompleted);
  }
  self.record_completed()?;
  Ok(())
}

#[event("todo.completed", version = 1, domain)]
fn record_completed(&mut self) {
  self.status = TodoStatus::Completed;
  self.advance_snapshot_generation();
}`
				}
			]
		},
{
			id: 'events',
			label: '5 · Events',
			lede: 'Domain methods emit events. A projection maps each event to a mutation program. The mutation program is GraphQL syntax only. It becomes MutationProgram IR. The IR upserts or deletes read-model rows. The UI must not write the `todos` table.',
			principle: 'Keep writes on the command side. Keep reads on the read-model side.',
			samples: [
				{
					file: 'projections/todos.rs',
					caption: 'Each `on` arm names the events and the mutation program.',
					code: `projection! {
  pub const TODOS: ProjectionDescriptor<EventualOnly> = {
    name: "project_todos",
    version: 1,
    epoch: "e2e-ui-todos-v2",
    model: Todos,
    on {
      events: [
        TodoCreatedDomainEvent,
        TodoRenamedDomainEvent,
        TodoCompletedDomainEvent,
        TodoReopenedDomainEvent,
        TodoReassignedDomainEvent,
        TodoArchivedDomainEvent,
        TodoForceArchivedDomainEvent,
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
}`
				},
				{
					file: 'projections/mutations/save_todo.mutation.graphql',
					caption: 'This is not a public schema field. The projector compiles it to MutationProgram IR.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation SaveTodo {
  upsert_todos(object: $input.todo)
}`
				},
				{
					file: 'projections/mutations/delete_todo.mutation.graphql',
					caption: 'The purge path deletes the row by primary key. The key comes from the event aggregate id.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation DeleteTodo {
  delete_todos_by_pk(todo_id: $input.todo_id)
}`
				},
				{
					file: 'handlers/events/project_todos.rs',
					caption: 'The event handler applies the named projection.',
					code: `pub async fn handle(
  context: CausalProjectorContext,
  projection: ModeledProjection,
) -> Result<(), HandlerError> {
  projection.apply(TODOS, &context).await
}`
				}
			]
		},
{
			id: 'service',
			label: '6 · Service + host',
			lede: 'The domain crate is the same on both hosts. `tests/e2e-ui` is one process: SQLite or Postgres, a bus, GraphQL, and Eventual projectors. `tests/e2e-celld` is the same UI and GraphQL process, but every Todo transition wait-dispatches to a cell (one private SQLite per todo id). GraphQL, `@live`, and projectors are not cell methods. Chat posts also wait-dispatch to a Chat cell on that path; Blob stays in-process.',
			principle: 'Compose modules into one Service. Let the host start the process. Keep the runner small.',
			samples: [
				{
					file: 'modules/todo.rs · routes()',
					caption: 'This module lists the Todo commands and the Eventual projection. Both hosts call the same mounts.',
					code: `pub fn routes<St: ServiceStores>(
  stores: &St,
  projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, Todo>> {
  Routes::for_aggregate(stores)
    .mount(todo_domain::commands::create())
    .mount(todo_domain::commands::complete())
    // … rename, reopen, archive, force_archive, purge …
    .consume_projection(projections.projector(TODOS))
}`
				},
				{
					file: 'modules/compose.rs · build_service',
					caption: 'List each module here. Do not hide composition in infrastructure code.',
					code: `Service::new()
  .named("e2e-ui")
  .routes(todo::routes(stores, &projections))
  .routes(chat::routes(stores, &projections))
  .routes(blob::routes(stores))
  .routes(identity::routes(stores, &projections))`
				},
				{
					file: 'graphql.rs · application',
					caption: 'The application manifest is derived from the Service. Its modules are the command namespaces. Do not list them by hand.',
					code: `pub fn application(service: &Service) -> Result<Application, ApplicationError> {
  let surface = SurfaceSpec::from_surface(E2E_UI_APPLICATION, &full_surface(service)?)?;
  service.application(E2E_UI_APPLICATION, surface)
}
// modules: blob | chat | todo | zitadel`
				},
				...hostAndRunnerSamples,
				{
					file: 'e2e-celld · runner/src/host.rs · route_aggregate',
					caption: 'The celld example is one runner crate over this service crate: the same `build_service`, another command host. `route_aggregate::<Todo>()` routes every mounted Todo command to POST {CELLD_URL}/todo/{todo_id}/{command}: the cell kind is the aggregate type, the shard each declaration\'s `shard:`. The cell answers with the command\'s typed payload, returned as-is; a payload that is not the declared output fails the command. Commands with no route run on the local Service. The cell outbox reaches the projectors through the celld Queue relay and NATS. There is no dual write.',
					code: `// tests/e2e-celld/crates/runner/src/host.rs
let service = build_service(&stores).with_bus(nats);  // e2e-service, unchanged
CelldCommandHost::new(celld_url, service, internal_secret)?
  .route_aggregate::<Todo>()?         // kind "todo", shard input.todo_id
  .route_aggregate::<ChatMessage>()?  // kind "chat_message", shard input.message_id

// cell class (tests/celld/worker): command HTTP + sealed GET only
POST /todo/:id/todo.create   { commandId, input }
GET  /todo/:id               // sealed row
// cell outbox → celld Queue → /internal/celld-queue/relay → NATS → projectors`
				}
			]
		}
	]
};

export const chatWalkthrough: DemoWalkthrough = {
	id: 'chat',
	href: '/chat',
	title: 'Lobby chat',
	kicker: 'Live query, then Eventual post',
	summary:
		'This nested layout owns one GraphQL island across `/chat` and `/chat/about`. `@load` fills the first HTML. `@live` retains the same query and WebSocket subscription while child pages change. A post is an Eventual command. The client writes an optimistic row into the shared replica. Guests read through the `e2e-ui-public` surface. The same `chat.post` declaration mounts on a local Service (`tests/e2e-ui`) or wait-dispatches to a small Chat cell (`tests/e2e-celld`). `@live` stays on GraphQL either way — that is the point of this demo.',
	tabs: [
{
			id: 'query',
			label: '1 · Query / live',
			lede: 'One layout-owned GraphQL operation loads the room and continues as a live subscription. The compiler retains its boundary while navigation swaps `/chat` and `/chat/about`. The operation reads the `ChatMessages` read model. Users, admins, and anonymous guests can read. Guests use the `e2e-ui-public` surface.',
			principle: 'Put shared data on the layout that owns its lifetime. Do not reopen the same stream per child page.',
			samples: [
				{
					file: 'routes/chat/+layout.graphql',
					caption: '`@load` fills SSR. `@live` sends WebSocket frames for the same retained layout document.',
					code: `query ChatMessages($limit: Int! = 25, $offset: Int! = 0) @load @live {
  chat_messages(
    where: { room_id: { _eq: "lobby" } }
    limit: $limit
    offset: $offset
    order_by: [{ created_at: desc }]
  ) {
    message_id
    room_id
    author_id
    body
    created_at
    author { user_id display_name email }
  }
}`
				},
				{
					file: 'readmodels/models/chat_messages.rs · ChatMessages',
					caption: 'This row is insert-shaped. `author` joins `AuthUsers` with `belongs_to`.',
					code: `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["message_id"])]
pub struct ChatMessages {
  #[readmodel(id)]
  pub message_id: String,
  pub room_id: String,
  pub author_id: String,
  pub body: String,
  pub created_at: String,
  #[readmodel(belongs_to = "AuthUsers", foreign_key = "author_id")]
  pub author: Option<AuthUsers>,
}`
				},
				{
					file: 'readmodels/models/chat_messages.rs · permissions',
					caption: 'Users, admins, and anonymous guests can read all columns.',
					code: `impl AuthorizedReadModel for ChatMessages {
  fn permissions() -> ModelPermissions<Self> {
    ModelPermissions::new()
      .grant("user", read().all_columns())
      .grant("admin", read().all_columns())
      .grant("anonymous", read().all_columns())
  }
}`
				},
				{
					file: 'routes/chat/+page.svelte',
					caption: 'The hook binds the replica. The page reverses the list for display.',
					code: `const lobby = ChatMessages.use();
const livePage = $derived.by(() => {
  const pageMessages = Array.isArray($lobby.data?.chat_messages)
    ? $lobby.data.chat_messages
    : [];
  return [...pageMessages].reverse();
});`
				}
			]
		},
{
			id: 'commands',
			label: '2 · Commands',
			lede: 'A post is a generated command. Only signed-in surfaces have this command. The replica cache applies a modeled optimistic message. Eventual confirmation follows the projector.',
			principle: 'Use one replica for user data.',
			samples: [
				{
					file: 'routes/chat/+page.svelte',
					caption: 'The shared replica cache feeds every bound view. Wait on `receipt.projected` for the obligation.',
					code: `const commands = useCommands();
const receipt = await commands.chat.post({
  message_id,
  body,
  room_id: LOBBY_ROOM,
  created_at: String(now),
});
if (receipt.projected !== undefined) {
  await receipt.projected;
}`
				},
				{
					file: 'modules/chat.rs · chat_messages_post',
					caption: 'Write roles and a session guard are on the domain-owned mount. The public client has no commands.',
					code: `.mount(chat_domain::commands::post())`
				},
				{
					file: 'generated/public/commands.ts',
					caption: 'The `e2e-ui-public` command inventory is empty.',
					code: `export const COMMAND_ARTIFACTS = [] as const;
export type GeneratedCommands = Readonly<Record<never, never>>;`
				}
			]
		},
{
			id: 'handlers',
			label: '3 · Handlers',
			lede: 'The handler creates the chat aggregate through the repository. Then it applies the domain `post`. Then it commits Eventual. The author is the session principal. The mount guard already admitted the session.',
			principle: 'Use the signed-in principal. Do not trust the author field in the request body.',
			samples: [
				{
					file: 'chat-domain/src/commands/post.rs · handle_post',
					caption: 'Get the principal. Reject a duplicate id. Create the aggregate. Call `post`. Commit Eventual.',
					code: `pub async fn handle_post(
  ctx: &CausalCommandContext<'_, ChatMessage>,
  input: ChatPostInput,
) -> Result<PreparedCommand<Eventual<ChatPostPayload>>, HandlerError> {
  let author = principal(ctx)?;
  let created_at = canonical_near_unix_millis(&input.created_at)?;
  let repo = ctx.repo();

  if repo.get(&input.message_id).await?.is_some() {
    return Err(HandlerError::Rejected(format!(
      "message {} already exists",
      input.message_id
    )));
  }

  let mut msg = repo.create();
  msg.post(
    &input.message_id,
    &input.room_id,
    &author,
    &input.body,
    &created_at,
  )
  .map_err(rejected)?;

  let state = ChatMessageState::from(&*msg);
  repo.publish_events().commit(msg)?.eventual(ChatPostPayload {
    message_id: state.message_id,
    room_id: state.room_id,
    author_id: state.author_id,
    body: state.body,
    created_at: state.created_at,
  })
}`
				}
			]
		},
{
			id: 'domain',
			label: '4 · Domain',
			lede: 'The chat write model is a Rust aggregate. One message is one consistency boundary. Public methods enforce the rules. Private `#[event]` functions record history. The model has no GraphQL.',
			principle: 'Start with the domain. Do not start with the database.',
			samples: [
				{
					file: 'chat-domain · ChatMessage',
					caption: 'This aggregate is the consistency boundary for one message.',
					code: `#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatMessage {
  pub entity: Entity,
  pub message_id: String,
  pub room_id: String,
  pub author_id: String,
  pub body: String,
  /// RFC3339 timestamp (string for portable projections / SQLite text).
  pub created_at: String,
  snapshot_delivery_generation: u64,
}

#[sourced(
  entity,
  events = "ChatMessageEvent",
  aggregate_type = "chat_message",
  domain_state = ChatMessageState,
)]
impl ChatMessage { /* post, … */ }`
				},
				{
					file: 'chat-domain · ChatMessage::post',
					caption: 'Validate the body. Then record the domain event. That event is history.',
					code: `pub fn post(
  &mut self,
  message_id: impl Into<String>,
  room_id: impl Into<String>,
  author_id: impl Into<String>,
  body: impl Into<String>,
  created_at: impl Into<String>,
) -> Result<(), ChatError> {
  if self.is_posted() {
    return Err(ChatError::AlreadyExists);
  }
  let body = body.into();
  let body = body.trim();
  if body.is_empty() {
    return Err(ChatError::EmptyBody);
  }
  self.record_posted(
    message_id.into(),
    room_id.into(),
    author_id.into(),
    body.to_string(),
    created_at.into(),
  )?;
  Ok(())
}

#[event("chat_message.posted", version = 1, domain)]
fn record_posted(
  &mut self,
  message_id: String,
  room_id: String,
  author_id: String,
  body: String,
  created_at: String,
) {
  self.entity.set_id(&message_id);
  self.message_id = message_id;
  self.room_id = room_id;
  self.author_id = author_id;
  self.body = body;
  self.created_at = created_at;
}`
				}
			]
		},
{
			id: 'events',
			label: '5 · Events',
			lede: 'The domain event drives the `chat_messages` projection. The projection names the `SaveChatMessage` mutation program. ChangeHub notifies `@live` subscribers when the row is written.',
			principle: 'A command changes the write model. A table is only for reads.',
			samples: [
				{
					file: 'projections/chat.rs',
					caption: 'One event arm. The arm names `SaveChatMessage`.',
					code: `projection! {
  pub const CHAT_MESSAGES: ProjectionDescriptor<DirectCandidate> = {
    name: "project_chat_messages",
    version: 1,
    epoch: "e2e-ui-chat-v2",
    model: ChatMessages,
    on {
      events: [ChatMessagePostedDomainEvent],
      mutation: SaveChatMessage,
      input: { message: body },
    },
  };
}`
				},
				{
					file: 'projections/mutations/save_chat_message.mutation.graphql',
					caption: 'This mutation is syntax only. The browser must not call it.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation SaveChatMessage {
  upsert_chat_messages(object: $input.message)
}`
				}
			]
		},
{
			id: 'service',
			label: '6 · Service + host',
			lede: 'The chat module mounts lobby posts. The module mounts the chat projector. `build_service` adds these routes to one Service. On `tests/e2e-celld`, `chat.post` wait-dispatches to a Chat cell; the cell outbox reaches this projector through the celld Queue relay and NATS, so `@live` still runs. The host starts the process. The runner only reads the environment.',
			principle: 'Compose modules into one Service. Let the host start the process. Keep the runner small.',
			samples: [
				{
					file: 'modules/chat.rs · routes()',
					caption: 'This module mounts ChatMessage commands and the chat projector.',
					code: `pub fn routes<St: ServiceStores>(
  stores: &St,
  projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, ChatMessage>> {
  Routes::for_aggregate(stores)
    .mount(chat_domain::commands::post())
    .consume_projection(projections.projector(CHAT_MESSAGES))
}`
				},
				{
					file: 'modules/compose.rs · build_service',
					caption: 'The module list is explicit: todo, chat, blob, and identity.',
					code: `Service::new()
  .named("e2e-ui")
  .routes(todo::routes(/* … */))
  .routes(chat::routes(/* … */))  // ← this module
  .routes(blob::routes(/* … */))
  .routes(identity::routes(/* … */))`
				},
				...hostAndRunnerSamples,
				{
					file: 'e2e-celld · runner/src/host.rs · route_aggregate',
					caption: 'The celld example routes chat.post to POST {CELLD_URL}/chat_message/{message_id}/chat.post through the same `CelldCommandHost`, derived from the mounted declaration. The cell outbox reaches the chat projector through the celld Queue relay and NATS, so SQL updates and GraphQL @live fires. GraphQL is not a cell method.',
					code: `// tests/e2e-celld/crates/runner/src/host.rs
CelldCommandHost::new(celld_url, service, internal_secret)?
  .route_aggregate::<ChatMessage>()?  // shard: |input| input.message_id.clone()

// cell class (tests/celld/worker): command HTTP + sealed GET only
POST /chat_message/:id/chat.post   { commandId, input }
GET  /chat_message/:id             // sealed row
// cell outbox → celld Queue → relay → NATS → chat projector → @live`
				}
			]
		}
	]
};

export const blobWalkthrough: DemoWalkthrough = {
	id: 'blob',
	href: '/blob',
	title: 'Blob game',
	kicker: 'Pure-function preview, then Atomic commit',
	summary:
		'The page owns the board list, while a reusable component owns the selected-game GraphQL island. Generation promotes that component island to the optional `[[gameId]]` route boundary and binds the parameter once. A move is Atomic. The next board is a pure function of the known row and the direction. The client runs the `blob-domain` rules in WASM and updates the replica immediately. The handler runs the same pure function, stages the row, and commits Atomic. Do not write TypeScript game rules.',
	tabs: [
{
			id: 'query',
			label: '1 · Query',
			lede: 'The route document lists games from the `BlobGames` read model. A reusable component document selects the active game from `[[gameId]]`. The compiler discovers that component through the Svelte import graph and emits one binding used by SSR, hydration, navigation, and `SelectedBlobGame.use()`. The board still comes from the same replica.',
			principle: 'Co-locate a query with the page, layout, or reusable component that owns its lifetime. Bind route state once.',
			samples: [
				{
					file: 'routes/blob/[[gameId]]/+page.graphql',
					caption: 'This document loads the board list. The replica is the source for the active board.',
					code: `query BlobGames @load {
  blob_games(order_by: [{ game_id: asc }]) {
    game_id
    score
    map_json
    owner { user_id display_name }
  }
}`
				},
				{
					file: 'readmodels/models/blob_games.rs · BlobGames',
					caption: 'This is the query row for one board. `map_json` is the serialized level. `owner` joins `AuthUsers`.',
					code: `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["game_id"])]
pub struct BlobGames {
  #[readmodel(id)]
  pub game_id: String,
  pub owner_id: String,
  pub score: i64,
  pub player_dead: bool,
  pub current_level: i64,
  pub current_level_completed: bool,
  pub map_json: String,
  /// active | dead | level_complete
  pub status: String,
  #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
  pub owner: Option<AuthUsers>,
}`
				},
				{
					file: 'readmodels/models/blob_games.rs · permissions',
					caption: 'A user can read only owned rows. An admin can read all rows. This is the same claim pattern as todos.',
					code: `impl AuthorizedReadModel for BlobGames {
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
}`
				},
				{
					file: 'routes/blob/[[gameId]]/+page.svelte',
					caption: 'The page reads the list from the replica and renders the reusable selected-game island.',
					code: `const query = BlobGames.use();
const games = $derived(
  $query.complete ? $query.data.blob_games : []
);

<SelectedBlobGame />`
				},
				{
					file: 'components/blob/SelectedBlobGame.graphql · .svelte',
					caption: 'The generated boundary resolves the optional route parameter once; the component receives a typed operation.',
					code: `query SelectedBlobGame($gameId: String) @load {
  blob_games(where: { game_id: { _eq: $gameId } }, limit: 1) {
    game_id
    score
    status
  }
}

const query = SelectedBlobGame.use(
  gameId === undefined ? {} : { gameId }
);`
				}
			]
		},
{
			id: 'commands',
			label: '2 · Commands + pure',
			lede: 'Auto-optimism from input cannot calculate the next map. The next map needs the known board. A pure reduce is the contract: given this cached row and this direction, calculate the assign fields. One ruleset lives in the domain core. There is no TypeScript twin. If the pure function returns null, keep the old board. Atomic still writes the authoritative row. You need the row in the cache. You need WASM ready. The WASM export is the same Rust function that the aggregate uses.',
			principle: 'Predict from the known row with the same pure function that the server runs. Do not invent authority.',
			samples: [
				{
					file: 'Why pure reduce (not only Atomic seal)',
					caption: 'Optimism from input cannot calculate `map_json`. A known-row pure function can calculate it.',
					code: `// Without a pure function: the UI waits for the Atomic body.
// With a pure function: known BlobGames row + direction → simulate_move
//   → assign map_json, score, player_dead, status immediately.
// After commit: the Atomic response overwrites with the server row.
// If the row is missing: the pure function returns null. Do not invent.`
				},
				{
					file: 'routes/blob/[[gameId]]/+page.svelte',
					caption: 'The input is only `game_id` and `direction`. Load the generated WASM hosts before the first move.',
					code: `onMount(() => {
  void ensurePureFunctionsReady(); // generated/user/pures.ts
});

await commands.blob.move({ game_id, direction });
// pure may patch map_json/score/… from known row;
// Atomic seal confirms or corrects`
				},
				{
					file: 'blob-domain/src/commands/move_dir.rs · mount',
					caption: 'The domain mount names the pure function, the WASM package, the keys, the args, and the assign fields.',
					code: `.mount(blob_domain::commands::move_dir())
// preview: blob.simulate_move → blobSimulateMove WASM`
				},
				{
					file: 'generated/user/pures.ts',
					caption: 'The generated client hosts WASM. The app has no TypeScript simulate file.',
					code: `const pureHost_0 = createWasmJsonPure({
  load: () => import('$lib/blob/pkg/blob_wasm.js'),
  exportName: 'blobSimulateMove',
});
export const PURE_FUNCTIONS = {
  'blob.simulate_move': pureHost_0.pure,
} as const;
export async function ensurePureFunctionsReady() {
  await pureHost_0.ensureReady();
}`
				},
				{
					file: 'Runtime shape (framework)',
					caption: 'The replica pure function always has this shape: (record, args) → assign fields or null.',
					code: `// createWasmJsonPure JSON-roundtrips to WASM:
//   pure(record, args) {
//     return JSON.parse(wasm.blobSimulateMove(
//       JSON.stringify(record), JSON.stringify(args)
//     )) ?? null;
//   }
// Validation (direction, map parse, edge) lives in the domain pure.`
				}
			]
		},
{
			id: 'handlers',
			label: '3 · Handlers',
			lede: 'The handler gets the aggregate. Then it calls the domain move. The domain move uses the same pure function as the WASM export. Then the handler stages the row from the mutation program. Then it commits Atomic. Aggregate, ledger, and query row share one transaction. Parse the input here. The mount guard already admitted the session. The pure function does not replace this path. The pure function only predicts the row for the UI.',
			principle: 'A command changes the write model. A table is only for reads.',
			samples: [
				{
					file: 'blob-domain/src/commands/move_dir.rs · handle_move',
					caption: 'Load the game. Call `move_dir`. Stage `SaveBlobGame`. Commit Atomic. The Atomic row is replica authority.',
					code: `pub async fn handle_move(
  ctx: &CausalCommandContext<'_, BlobGame>,
  input: BlobMoveInput,
) -> Result<PreparedCommand<Atomic<BlobGames>>, HandlerError> {
  let owner = principal(ctx)?;
  let dir = Direction::parse(&input.direction).ok_or_else(|| {
    HandlerError::Rejected(format!(
      "invalid direction \`{}\` (use up|down|left|right)",
      input.direction
    ))
  })?;

  let repo = ctx.repo();
  let mut game = repo
    .get(&input.game_id)
    .await?
    .ok_or_else(|| HandlerError::NotFound(input.game_id.clone()))?;
  // move_dir → core::simulate_move (same pure as client WASM)
  game.move_dir(&owner, dir).map_err(rejected)?;

  // sealed_row = SaveBlobGame().from_state(&BlobGameState::from(game))
  let row = sealed_row(&*game)?;
  repo.readmodel(row).publish_events().commit(game)?.atomic()
  // sealed row → replica authority
}`
				}
			]
		},
{
			id: 'domain',
			label: '4 · Domain + pure core',
			lede: 'The `blob-domain` crate has two faces. `core` holds the pure board rules. Those rules can compile to WASM. `models` holds the sourced aggregate. The pure function has no ownership and no Entity. Those stay on the aggregate. The client and the server share `core`. Optimism cannot diverge from the Atomic commit.',
			principle: 'Start with the domain. Share the pure function. Do not write a second ruleset.',
			samples: [
				{
					file: 'blob-domain/src/core · simulate_move',
					caption: 'This function is pure. Input is map, score, and direction. Output is the next board. There is no Entity, no ownership, and no I/O.',
					code: `pub fn simulate_move(
  map: &[Vec<u8>],
  score: i64,
  direction: Direction,
) -> Result<MovePreview, SimulateError> {
  // step player, mark visited, hole/suicide/score, level complete…
  Ok(MovePreview { map, score, player_dead, level_complete })
}`
				},
				{
					file: 'blob-domain · BlobGame::move_dir',
					caption: 'The aggregate checks ownership. Then it calls the pure function. Then it records the domain event.',
					code: `pub fn move_dir(&mut self, owner_id: &str, direction: Direction)
  -> Result<(), BlobError>
{
  self.ensure_owner(owner_id)?;
  if self.player_dead { return Err(BlobError::PlayerDead); }
  let preview = simulate_move(&self.map, self.score, direction)
    .map_err(map_simulate_err)?;
  self.record_moved(
    preview.score,
    preview.player_dead,
    preview.level_complete,
    preview.map,
    direction.as_str().to_string(),
  )?;
  Ok(())
}`
				},
				{
					file: 'blob-domain · wasm export',
					caption: 'The WASM adapter takes record JSON and args JSON. It returns assign JSON or None. None is fail-closed.',
					code: `#[wasm_bindgen(js_name = blobSimulateMove)]
pub fn blob_simulate_move(record_json: &str, args_json: &str)
  -> Option<String>
{
  // parse map_json / score / direction → simulate_move → assign fields
  // None = fail closed (illegal move or bad input)
}`
				},
				{
					file: 'blob-domain · features + distributed build',
					caption: 'The server builds the default `domain` feature. `distributed build` (and `distributed dev`) compiles the declared pure with wasm-pack and `--features wasm` into `$lib/blob/pkg`. There is no separate make target.',
					code: `// Cargo.toml
// default = ["domain"]  → aggregate + levels + distributed
// wasm                 → blobSimulateMove
//
// move_dir.rs declares the package path:
// CommandProjectionPureReduce::wasm("blob.simulate_move", "blob/pkg/blob_wasm", …)`
				}
			]
		},
{
			id: 'events',
			label: '5 · Events',
			lede: 'Domain events still record history. For blob, the handler runs the `SaveBlobGame` mutation program in the same commit as the event. The response can include the row. If you used Eventual placement, an event handler would run that IR. The waiting client would have no response row.',
			principle: 'Atomic applies the mutation program in the command transaction. Eventual applies it in an event handler.',
			samples: [
				{
					file: 'projections/blob.rs',
					caption: 'The same `SaveBlobGame` program is available for direct and Eventual placement.',
					code: `projection! {
  pub const BLOB_GAMES: ProjectionDescriptor<DirectCandidate> = {
    name: "project_blob",
    version: 1,
    epoch: "e2e-ui-blob-v2",
    model: BlobGames,
    on {
      events: [
        BlobInitializedDomainEvent,
        BlobLevelStartedDomainEvent,
        BlobStartedDomainEvent,
        BlobMovedDomainEvent,
      ],
      mutation: SaveBlobGame,
      input: { game: body },
    },
  };
}`
				},
				{
					file: 'projections/mutations/save_blob_game.mutation.graphql',
					caption: 'This upsert is syntax only. The direct path and the Eventual path use the same program.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation SaveBlobGame {
  upsert_blob_games(object: $input.game)
}`
				},
				{
					file: 'service · projection binding',
					caption: 'Direct placement binds the mutation to the command commit. The handler stages the row and calls `atomic()`.',
					code: `let blob_binding = ProjectionBinding::materialize_direct(
  BLOB_GAMES.direct(),
  source(),
  owner("project_blob"),
  /* … */,
  vec![projection_output::<BlobGames>()],
  /* … */,
)?;
// Handler stages the row via readmodel(row).commit()?.atomic()`
				}
			]
		},
{
			id: 'service',
			label: '6 · Service + host',
			lede: 'The blob module mounts only Atomic BlobGame commands. The handler writes the board in the same transaction. There is no async board projector. `build_service` adds these routes to the same Service as todos and chat. The host starts the process. The runner only reads the environment.',
			principle: 'Compose modules into one Service. Let the host start the process. Keep the runner small.',
			samples: [
				{
					file: 'modules/blob.rs · routes()',
					caption: 'This module mounts start, move, and start_level. All three are Atomic. Move declares the WASM pure function.',
					code: `pub fn routes<St: ServiceStores>(stores: &St) -> Routes<AggregateDependencies<St, BlobGame>> {
  Routes::for_aggregate(stores)
    .mount(blob_domain::commands::start())
    .mount(blob_domain::commands::move_dir())
    .mount(blob_domain::commands::start_level())
}`
				},
				{
					file: 'modules/compose.rs · blob routes',
					caption: 'This is the same Service composition as todos and chat.',
					code: `Service::new()
  .named("e2e-ui")
  .routes(todo::routes(stores, &projections))
  .routes(chat::routes(stores, &projections))
  .routes(blob::routes(stores))  // ← this module
  .routes(identity::routes(stores, &projections))`
				},
				...hostAndRunnerSamples
			]
		}
	]
};

export const adminWalkthrough: DemoWalkthrough = {
	id: 'admin',
	href: '/admin',
	title: 'Admin surface',
	kicker: 'Second client, then elevated command',
	summary:
		'This page uses the `e2e-ui-admin` surface. Force-archive is an Eventual command on that surface only. The admin replica still applies an optimistic cache. The server still loads the aggregate through the repository and commits.',
	tabs: [
{
			id: 'query',
			label: '1 · Query',
			lede: 'The admin list uses a different generated client. The client reads the same `Todos` read model. The GraphQL engine is the same. The surface privilege is different. The admin grant returns the todos of every owner.',
			principle: 'Roles and surfaces are real. Treat them as security boundaries.',
			samples: [
				{
					file: 'routes/admin/+page.svelte',
					caption: 'Import the admin client. Do not import `$distributed` user hooks on this page.',
					code: `import { AdminAllTodos, useCommands } from '$distributed/admin';

const query = AdminAllTodos.use();
const commands = useCommands();
const todos = $derived($query.complete ? $query.data.todos : []);`
				},
				{
					file: 'readmodels/models/todos.rs · Todos',
					caption: 'This is the same query model as `/todos`. The admin surface is not a second table.',
					code: `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
  #[readmodel(id)]
  pub todo_id: String,
  pub owner_id: String,
  pub title: String,
  pub status: String,
  pub assignee_id: Option<String>,
  #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
  pub owner: Option<AuthUsers>,
}`
				},
				{
					file: 'readmodels/models/todos.rs · admin grant',
					caption: 'The `e2e-ui-admin` privilege pack uses the admin grant. That grant reads all rows.',
					code: `impl AuthorizedReadModel for Todos {
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
}`
				},
				{
					file: 'modules/graphql.rs · build_engine surfaces',
					caption: 'One engine opens three surfaces: user, admin, and public. Each surface names its eligible roles and its privilege pack. Client export reads the same table.',
					code: `for surface in CLIENT_SURFACES {
  b = b.client_application_surface(
    surface.name,                       // "e2e-ui" | "e2e-ui-admin" | "e2e-ui-public"
    surface.eligible_roles.iter().copied(),
    surface.schema_roles.iter().copied(), // portable privilege pack
  );
}`
				}
			]
		},
{
			id: 'commands',
			label: '2 · Commands',
			lede: 'Elevated mutations exist only on the admin command tree. They still update the admin replica cache. This page reads that cache.',
			principle: 'Put authorization on the surface, the roles list, and the guard. Do not invent a second HTTP API.',
			samples: [
				{
					file: 'routes/admin/+page.svelte',
					caption: 'Call `force_archive` from the admin command tree.',
					code: `await commands.todo.force_archive({ todo_id });`
				},
				{
					file: 'modules/todo.rs · todos_force_archive',
					caption: 'Write RBAC is admin only. The guard is `causal_is_admin`. The user client inventory does not include this field.',
					code: `.command_transition::<
  domain_commands::ForceArchive,
  TodoForceArchiveInput,
  Eventual<TodoForceArchivePayload>,
>(todo_force_archive::COMMAND)
.field_name("todos_force_archive")
.roles(["admin"])
.guarded(causal_is_admin, todo_force_archive::handle)`
				},
				{
					file: 'routes/admin/+layout.server.ts',
					caption: 'This layout rejects the request before GraphQL if the session is not admin.',
					code: `getRole: (session) => {
  const role = engineRoleFromGroups(session?.user?.groups);
  if (!isAdminEngineRole(role)) {
    error(403, 'Admin role required — sign in as admin');
  }
  return role;
}`
				}
			]
		},
{
			id: 'handlers',
			label: '3 · Handlers',
			lede: 'The command loads the aggregate. Then it calls `force_archive`. Then it commits Eventual. Authorization is the role, the surface, and the mount guard. There is no special HTTP path.',
			principle: 'Use the signed-in principal. Do not trust the request body for identity.',
			samples: [
				{
					file: 'todo-domain/src/commands/force_archive.rs · thin',
					caption: 'Admins only. Load the todo. Call the domain. The payload names the admin from the session.',
					code: `command! {
  name: "todo.force_archive",
  // …
  load: required,
  roles: ["admin"],
  guard: admin_user,
  invoke: |todo, _input, _principal| todo.force_archive(),
  payload: |todo, principal| {
    let state = TodoState::from(&**todo);
    TodoForceArchivePayload {
      todo_id: state.todo_id,
      owner_id: state.owner_id,
      status: state.status,
      archived_by: principal.to_string(),
    }
  },
}`
				}
			]
		},
{
			id: 'domain',
			label: '4 · Domain',
			lede: 'This is the same `Todo` aggregate as `/todos`. Elevated methods live on the domain type. They do not live in the GraphQL layer. There is one write model. There are many surfaces.',
			principle: 'Start with the domain. Do not start with the database.',
			samples: [
				{
					file: 'todo-domain · Todo',
					caption: 'This is the same aggregate shape as the user surface.',
					code: `#[derive(Debug, Clone, Default, Serialize, Deserialize, Snapshot)]
#[snapshot(id = "todo_id")]
pub struct Todo {
  #[serde(skip, default)]
  pub entity: Entity,
  pub todo_id: String,
  pub owner_id: String,
  pub title: String,
  pub status: TodoStatus,
  pub assignee_id: Option<String>,
  purged: bool,
  snapshot_generation: u64,
}

#[sourced(
  entity,
  events = "TodoEvent",
  aggregate_type = "todo",
  domain_state = TodoState,
)]
impl Todo { /* create, complete, force_archive, … */ }`
				},
				{
					file: 'todo-domain · Todo::force_archive',
					caption: 'The admin path still records a domain event on the same aggregate.',
					code: `/// Administrator intervention — separate event from owner archival.
pub fn force_archive(&mut self) -> Result<(), TodoError> {
  if !self.is_created() {
    return Err(TodoError::NotCreated);
  }
  self.record_force_archived()?;
  Ok(())
}

#[event("todo.force_archived", version = 1, domain)]
fn record_force_archived(&mut self) {
  self.status = TodoStatus::Archived;
  self.advance_snapshot_generation();
}`
				}
			]
		},
{
			id: 'events',
			label: '5 · Events',
			lede: 'Force-archive emits `TodoForceArchivedDomainEvent`. That event uses the same todos projection as owner archive. It uses the same `SaveTodo` mutation program. Every surface replica converges on one query model.',
			principle: 'Declare the read model once. Use it on every surface that needs it.',
			samples: [
				{
					file: 'projections/todos.rs · force archive arm',
					caption: 'Owner archive and admin force-archive share this arm and `SaveTodo`.',
					code: `projection! {
  pub const TODOS: ProjectionDescriptor<EventualOnly> = {
    name: "project_todos",
    version: 1,
    epoch: "e2e-ui-todos-v2",
    model: Todos,
    on {
      events: [
        TodoCreatedDomainEvent,
        /* … */
        TodoArchivedDomainEvent,
        TodoForceArchivedDomainEvent, // admin path
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
}`
				},
				{
					file: 'projections/mutations/save_todo.mutation.graphql',
					caption: 'This is the same mutation program as owner complete and archive. Force-archive is one more event on this arm.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation SaveTodo {
  upsert_todos(object: $input.todo)
}`
				},
				{
					file: 'projections/mutations/delete_todo.mutation.graphql',
					caption: 'The purge arm uses the same delete program.',
					code: `# Syntax-only read-model mutation → MutationProgram IR.
mutation DeleteTodo {
  delete_todos_by_pk(todo_id: $input.todo_id)
}`
				}
			]
		},
{
			id: 'service',
			label: '6 · Service + host',
			lede: 'Admin is not a second binary. Admin is a second GraphQL surface named `e2e-ui-admin`. The surface runs on the same host and the same runner. Force-archive is mounted once in `modules/todo.rs`. Only the admin client inventory includes that field. Compose, host, and runner are the same as the user demos.',
			principle: 'Roles and surfaces are real. Modules and the host stay shared.',
			samples: [
				{
					file: 'modules/todo.rs · force_archive mount',
					caption: 'This is the same todo module that serves `/todos`. The elevated field is on the same Routes list.',
					code: `.command_transition::<
  domain_commands::ForceArchive,
  /* … */,
>(todo_force_archive::COMMAND)
.field_name("todos_force_archive")
.roles(["admin"])
.guarded(causal_is_admin, todo_force_archive::handle)`
				},
				{
					file: 'modules/graphql.rs · dual surfaces',
					caption: 'The service crate opens the user, admin, and public surfaces.',
					code: `.client_application_surface(
  "e2e-ui",
  ["admin", "user"],
  ["user"],
)
.client_application_surface("e2e-ui-admin", ["admin"])
.client_application_surface("e2e-ui-public", ["anonymous"])`
				},
				{
					file: 'application.rs · client surfaces',
					caption: 'Each surface is declared once. The engine, client generation and host identity use it.',
					code: `pub const CLIENT_SURFACE: ClientSurface = ClientSurface {
  name: "e2e-ui", eligible_roles: &["admin", "user"], schema_roles: &["user"],
};
pub const ADMIN_CLIENT_SURFACE: ClientSurface = ClientSurface {
  name: "e2e-ui-admin", eligible_roles: &["admin"], schema_roles: &["admin"],
};
pub const PUBLIC_CLIENT_SURFACE: ClientSurface = ClientSurface {
  name: "e2e-ui-public", eligible_roles: &["anonymous"], schema_roles: &["anonymous"],
};`
				},
				...hostAndRunnerSamples
			]
		}
	]
};

export const sessionWalkthrough: DemoWalkthrough = {
	id: 'session',
	href: '/session',
	title: 'Session',
	kicker: 'Browser identity, then OIDC, then engine roles',
	summary:
		'This page shows the signed-in person. Tokens and groups become the Bearer session. They also become the `x-roles` set. Every query and command on the other pages uses that set.',
	tabs: [
		{
			id: 'query',
			label: '1 · Session UI',
			lede: 'This page has no GraphQL list. The session comes from Auth.js. The layout already loaded it. Groups map to engine roles. That principal drives RBAC on every other page.',
			principle: 'Use the signed-in principal. Do not trust the request body for identity.',
			samples: [
				{
					file: 'routes/session/+page.svelte',
					caption: 'Read the session that the layout loaded. Map groups to an engine role in the UI.',
					code: `const session = $derived(data.session as SessionLike | null | undefined);
const user = $derived(session?.user);
const engineRole = $derived(
  engineRoleFromGroups(user?.groups)
);`
				},
				{
					file: 'lib/roles.ts',
					caption: 'The UI and SSR map identity-provider groups to `admin` or `user` before GraphQL.',
					code: `export function engineRoleFromGroups(
  groups: string[] | undefined,
): 'admin' | 'user' {
  if (!groups?.length) return 'user';
  if (groups.includes('admin') || groups.includes('admins')) {
    return 'admin';
  }
  return 'user';
}

export function isAdminEngineRole(
  role: string | null | undefined,
): boolean {
  return role === 'admin';
}`
				}
			]
		},
		{
			id: 'commands',
			label: '2 · Tokens',
			lede: 'Commands and queries do not invent identity. They carry the access token. The engine maps claims to `x-user-id` and `x-roles`. The role set is set-only. Grants and `.roles([…])` read that set.',
			principle: 'Roles and surfaces are real. Treat them as security boundaries.',
			samples: [
				{
					file: 'routes/+layout.svelte',
					caption: 'The layout gives the session and the replica authority to every page.',
					code: `const pageData = createPageDataSessionSource(initialData);
const client = provideDistributed({
  session: pageData.session,
  browser,
  hydration: initialData.distributed,
  authority: initialData.distributedAuthority,
});`
				},
				{
					file: 'modules/graphql.rs · oidc_bearer_config',
					caption: 'This claim map sets the allowed engine roles on the session.',
					code: `oidc.claim_map.engine_roles = vec![
  "user".into(),
  "admin".into(),
];
oidc.claim_map.role_claims = vec![
  "groups".into(),
  "roles".into(),
  "realm_access.roles".into(),
  "urn:zitadel:iam:org:project:roles".into(),
];
// Empty identity allowed for e2e-ui-public (anonymous).
oidc.require_auth = false;
IdentityConfig::oidc_bearer(oidc)`
				}
			]
		},
		{
			id: 'handlers',
			label: '3 · Edge map',
			lede: 'OidcBearer validates the JWT. Then it injects session variables. Handlers call `ctx.user_id()`. The repository pattern uses that principal.',
			principle: 'Identity is set-only. The surface privilege decides if the command can run.',
			samples: [
				{
					file: 'src/graphql/identity/resolve.rs',
					caption: 'If there is no bearer token and `require_auth` is false, the session is anonymous. That path is for `e2e-ui-public`.',
					code: `(IdentityConfig::OidcBearer(oidc), Some(validator)) => {
  match extract_bearer(headers)? {
    Some(token) => {
      let (session, principal) =
        validator.validate_and_map_principal_async(&token).await?;
      Ok(ResolvedIdentity::verified(session, principal))
    }
    None if oidc.require_auth => Err(AuthError::Unauthorized),
    None => Ok(ResolvedIdentity::unverified(Session::new())),
  }
}`
				},
				{
					file: 'engine · principal_may_open_application',
					caption: 'A principal can open a surface only when an asserted role is in the eligible list.',
					code: `fn principal_may_open_application(
  asserted: &[String],
  eligible: &[String],
) -> bool {
  if asserted.is_empty() {
    return eligible.iter().any(|role| role == "anonymous");
  }
  asserted.iter().any(|role| {
    eligible
      .binary_search_by(|c| c.as_str().cmp(role.as_str()))
      .is_ok()
  })
}`
				}
			]
		},
		{
			id: 'claims',
			label: '4 · Claims',
			lede: 'The identity provider sends groups. The browser maps groups to engine roles. The engine maps claims again. There is no session aggregate. Identity is transport data.',
			principle: 'Keep identity on the transport. Do not put a session aggregate in the domain.',
			samples: [
				{
					file: 'ui/src/auth.ts · group claims',
					caption: 'Auth.js stores the tokens. It also copies Zitadel project role keys into `session.user.groups`.',
					code: `const DEFAULT_GROUP_CLAIMS = [
  'groups',
  'roles',
  'urn:zitadel:iam:org:project:roles',
  'urn:zitadel:iam:org:projects:roles',
];
// Auth.js jwt/session callbacks store access + refresh
// and flatten Zitadel project role keys into session.user.groups`
				}
			]
		},
		{
			id: 'service',
			label: '5 · Service + host',
			lede: 'Identity is not a domain aggregate. Identity is service-crate wiring. That wiring includes OIDC or dev headers, claim maps, and the identity module Zitadel ingress. The ingress fills `AuthUsers`. The host starts the Service. The host also starts the scrape and consumer loops; the bus drains the outbox. The runner only supplies `DATABASE_URL`, `BIND`, and OIDC environment variables.',
			principle: 'Keep identity on the transport. Compose modules. Keep domain crates pure.',
			samples: [
				{
					file: 'modules/compose.rs · build_service',
					caption: 'Identity is a module with no aggregate. It mounts the Zitadel commands and the `AuthUsers` projection. Its commands are `zitadel.*`, so the derived manifest names that module `zitadel`.',
					code: `Service::new()
  .named("e2e-ui")
  .routes(todo::routes(stores, &projections))
  .routes(chat::routes(stores, &projections))
  .routes(blob::routes(stores))
  .routes(identity::routes(stores, &projections))`
				},
				{
					file: 'modules/identity.rs · Zitadel ingress',
					caption: 'These are typed commands with no aggregate. Each user profile they publish commits with its command ledger receipt. Each command has its own HTTP edge: it checks the shared secret and dispatches as the ingestor. No user role can call them, and every other command path stays closed.',
					code: `Routes::for_stores(stores.events().clone(), stores.read_models().clone())
  .typed_command(zitadel::command())
  .handle(zitadel::handle)
  .typed_command(zitadel_scrape::command())
  .handle(zitadel_scrape::Scrape(stores.read_models().clone()))
  .consume_projection(projections.projector(AUTH_USERS))

// compose.rs
Service::new()
  // ...
  .with_http_command(zitadel::COMMAND, zitadel::http_edge)
  .with_http_command(zitadel_scrape::COMMAND, zitadel_scrape::http_edge)`
				},
				{
					file: 'modules/graphql.rs · identity_from_env',
					caption: 'The host selects OidcBearer or DevHeaders from the environment. The binary is the same.',
					code: `// identity_from_env() → OIDC_ISSUER/AUDIENCE/JWKS or DevHeaders
// serve_with_oidc(service, identity, bind)`
				},
				...hostAndRunnerSamples
			]
		},
		{
			id: 'events',
			label: '6 · Directory',
			lede: 'People appear as `AuthUsers` rows. Zitadel ingest and scrape publish user profiles as external facts, and a projection fills those rows. Chat author and blob owner join this directory. Do not store a second display name.',
			principle: 'Fill `AuthUsers` from identity events. Do not fill it from user commands.',
			samples: [
				{
					file: 'readmodels/models/auth_users.rs · AuthUsers',
					caption: 'This is an imported identity-provider directory row. The primary key is the OIDC `sub` / session `x-user-id`. Ingest writes this row. User commands do not write this row.',
					code: `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("auth_users")]
pub struct AuthUsers {
  #[id("user_id")]
  pub user_id: String,
  pub email: String,
  pub display_name: String,
  /// human | machine
  pub user_kind: String,
  /// pending | approved | rejected
  pub approval_status: String,
  /// active | deactivated
  pub status: String,
  pub updated_at: String,
}`
				},
				{
					file: 'projections/auth_users.rs · AUTH_USERS',
					caption: 'The ingestor publishes a `ZitadelUserProfile` with `publish_external`, identified by the delivery it came from. The projection writes it to `AuthUsers` as it is.',
					code: `#[derive(Serialize, Deserialize, distributed::DomainEvent)]
#[domain_event(name = "zitadel.user_profile", version = 1)]
pub struct ZitadelUserProfile { /* the AuthUsers columns */ }

distributed::projection! {
  pub const AUTH_USERS: ProjectionDescriptor<EventualOnly> = {
    name: "project_auth_users",
    version: 1,
    epoch: "e2e-ui-auth-users-v1",
    model: AuthUsers,
    on {
      events: [ZitadelUserProfile],
      mutation: SaveAuthUser,
      input: { user: body },
    },
  };
}`
				}
			]
		}
	]
};
