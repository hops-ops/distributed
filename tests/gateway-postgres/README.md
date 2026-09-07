# Physical standby freshness fixture

Run `python3 tests/gateway-postgres/run.py`. Requires Docker and Rust. The script
creates its own network and two digest-pinned PostgreSQL containers with empty
in-memory data directories, binds only loopback ephemeral ports, and removes only
those owned resources on exit. No application database or credentials are read.

The test uses `pg_wal_replay_pause` and waits for the actual paused state before
writing the primary. It checks selected GraphQL responses and unavailable-primary
behavior, then resumes replay. PostgreSQL documents the distinction between
[replay pause and receive](https://www.postgresql.org/docs/16/functions-admin.html)
and the [`pg_basebackup -R` standby setup](https://www.postgresql.org/docs/16/app-pgbasebackup.html).
This is a test fixture, not a production WAL routing policy.
