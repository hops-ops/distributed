struct Aggregate;
struct Input;

#[distributed::command(
    id = "todo.create",
    id = "todo.rename",
    roles(user),
    input = Input,
    outcome = distributed::command::Succeeded<Input>
)]
async fn duplicate_option(
    _context: &distributed::microsvc::CausalCommandContext<'_, Aggregate>,
    _input: Input,
) -> Result<
    distributed::command::PreparedCommand<distributed::command::Succeeded<Input>>,
    distributed::microsvc::HandlerError,
> {
    unreachable!()
}

fn main() {}
