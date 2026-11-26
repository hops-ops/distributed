/// Spec-shaped authoring: **mutations** + **event-first projections**.
///
/// One or more `on { events, mutation, input }` arms. `input` declares how the
/// occurrence fills mutation inputs (`body` = event body object root;
/// `aggregate_id` = envelope id for delete-by-pk).
///
/// ```ignore
/// projection! {
///     pub const TODOS: ProjectionDescriptor<EventualOnly> = {
///         name: "project_todos",
///         version: 1,
///         epoch: "e2e-ui-todos-v2",
///         model: Todos,
///         on {
///             events: [
///                 TodoCreatedDomainEvent,
///                 TodoCompletedDomainEvent,
///             ],
///             mutation: SaveTodo,
///             input: { todo: body },
///         },
///         on {
///             events: [TodoPurgedDomainEvent],
///             mutation: DeleteTodo,
///             input: { todo_id: aggregate_id },
///         },
///     };
/// }
/// ```
///
/// A `program:` arm remains as an escape hatch for custom partition / multi-binding
/// factories. Prefer the event-first form when partition is unit.
#[macro_export]
macro_rules! projection {
    // Event-first: one or more `on { events, mutation, input }` arms.
    (
        $vis:vis const $id:ident : $desc_ty:ty = {
            name: $name:literal,
            version: $version:expr,
            epoch: $epoch:literal,
            model: $model:ty,
            $(source: $source:ident,)?
            $(
                on {
                    events: [ $($event_ty:ty),+ $(,)? ],
                    mutation: $mutation:path,
                    input: { $input_key:ident : $input_src:ident },
                    $(,)?
                }
            ),+ $(,)?
        } $(;)?
    ) => {
        $vis const $id: $desc_ty = {
            fn __program() -> ::core::result::Result<
                &'static $crate::projection::ProjectionProgram,
                $crate::projection::ProjectionProgramError,
            > {
                static PROGRAM: $crate::projection::lower::ProjectionProgramCell =
                    $crate::projection::lower::ProjectionProgramCell::new();
                PROGRAM.get_or_build(|| {
                    use $crate::domain_event::DomainEventContract;
                    let mut handlers = ::std::vec::Vec::new();
                    $(
                        {
                            let mutation = $mutation().program()?;
                            let input_source = $crate::__projection_input_source!($input_src);
                            $(
                                handlers.push($crate::mutation::bind_event_apply_mutation(
                                    &<$event_ty>::descriptor(),
                                    mutation.clone(),
                                    ::core::stringify!($input_key),
                                    input_source,
                                )?);
                            )+
                        }
                    )+
                    let program = $crate::mutation::compile_projection(
                        $name,
                        $version,
                        $crate::projection::ProjectionPartition::Unit,
                        handlers,
                    )?;
                    $(let program = program.with_source_ordering(
                        $crate::__projection_source_ordering!($source),
                    )?;)?
                    Ok(program)
                })
            }
            $crate::__projection_descriptor!($name, $version, $epoch, $model, __program)
        };
    };

    // Escape hatch: custom program factory (partition / multi-binding / etc.).
    // Prefer the event-first arms above when partition is unit.
    (
        $vis:vis const $id:ident : $desc_ty:ty = {
            name: $name:literal,
            version: $version:expr,
            epoch: $epoch:literal,
            model: $model:ty,
            program: $program:path $(,)?
        } $(;)?
    ) => {
        $vis const $id: $desc_ty = {
            fn __program() -> ::core::result::Result<
                &'static $crate::projection::ProjectionProgram,
                $crate::projection::ProjectionProgramError,
            > {
                static PROGRAM: $crate::projection::lower::ProjectionProgramCell =
                    $crate::projection::lower::ProjectionProgramCell::new();
                PROGRAM.get_or_build($program)
            }
            $crate::__projection_descriptor!($name, $version, $epoch, $model, __program)
        };
    };
}

/// The descriptor of a single-model projection whose program `$program` returns.
#[doc(hidden)]
#[macro_export]
macro_rules! __projection_descriptor {
    ($name:expr, $version:expr, $epoch:expr, $model:ty, $program:ident) => {{
        fn __lower(
            plan: &$crate::projection::ResolvedProjectionPlan,
        ) -> ::core::result::Result<
            $crate::projection::lower::LoweredProjectionPlan,
            $crate::projection::lower::ProjectionLoweringError,
        > {
            $crate::mutation::lower_single_model::<$model>(plan)
        }
        fn __inventory() -> ::core::result::Result<
            $crate::projection::lower::ProjectionOutputInventory,
            $crate::projection::lower::ProjectionLoweringError,
        > {
            $crate::mutation::inventory_single_model::<$model>()
        }
        $crate::projection::lower::ProjectionDescriptor::new(
            $name,
            $version,
            $epoch,
            $program,
            __lower,
            __inventory,
        )
    }};
}

/// Map `source: aggregate_snapshot | external_snapshot` for [`projection!`].
#[doc(hidden)]
#[macro_export]
macro_rules! __projection_source_ordering {
    (aggregate_snapshot) => {
        $crate::projection::SourceOrdering::AggregateSnapshot
    };
    (external_snapshot) => {
        $crate::projection::SourceOrdering::ExternalSnapshot
    };
}

/// Map `input: { key: body | aggregate_id }` keywords for [`projection!`].
#[macro_export]
#[doc(hidden)]
macro_rules! __projection_input_source {
    (body) => {
        $crate::mutation::ProjectionInputSource::Body
    };
    (aggregate_id) => {
        $crate::mutation::ProjectionInputSource::AggregateId
    };
}
