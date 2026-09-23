struct WrongAggregate;
struct WrongInput;

#[distributed::command(
    id = "todo.create",
    roles(user),
    input = WrongInput,
    outcome = distributed::command::Succeeded<WrongInput>
)]
async fn wrong_handler(
    _context: &distributed::microsvc::CausalCommandContext<'_, WrongAggregate>,
    _input: WrongInput,
) -> Result<(), distributed::microsvc::HandlerError> {
    Ok(())
}

fn main() {}
