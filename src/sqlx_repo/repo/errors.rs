use super::*;

pub(crate) fn repository_storage_error<DB: SqlxRepoBackend>(
    operation: &str,
    err: sqlx::Error,
) -> RepositoryError {
    crate::sqlx_repo::repository_storage_error(DB::BACKEND, operation, err)
}
