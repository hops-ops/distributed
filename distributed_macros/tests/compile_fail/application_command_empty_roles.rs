struct Aggregate;
struct Input;

#[distributed::command(
    id = "todo.create",
    roles(),
    input = Input,
    outcome = distributed::command::Succeeded<Input>
)]
async fn empty_roles(
    _context: &distributed::microsvc::CausalCommandContext<'_, Aggregate>,
    _input: Input,
) -> Result<
    distributed::command::PreparedCommand<distributed::command::Succeeded<Input>>,
    distributed::microsvc::HandlerError,
> {
    unreachable!()
}

fn main() {}
