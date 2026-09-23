//! Matrix axes for the aggregate load suite.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepoKind {
    Memory,
    Sqlite,
    Postgres,
}

impl RepoKind {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "memory" | "mem" | "in-memory" => Ok(Self::Memory),
            "sqlite" => Ok(Self::Sqlite),
            "postgres" | "pg" | "postgresql" => Ok(Self::Postgres),
            other => Err(format!(
                "unknown --repo {other:?} (expected memory, sqlite, or postgres)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchKind {
    Direct,
    Http,
    Grpc,
    Bus,
}

impl DispatchKind {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "direct" => Ok(Self::Direct),
            "http" => Ok(Self::Http),
            "grpc" => Ok(Self::Grpc),
            "bus" => Ok(Self::Bus),
            other => Err(format!(
                "unknown --dispatch {other:?} (expected direct, http, grpc, or bus)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::Bus => "bus",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusKind {
    Memory,
    Sqlite,
    Postgres,
    Nats,
    Kafka,
    Rabbitmq,
}

impl BusKind {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "memory" | "mem" | "in-memory" => Ok(Self::Memory),
            "sqlite" => Ok(Self::Sqlite),
            "postgres" | "pg" => Ok(Self::Postgres),
            "nats" => Ok(Self::Nats),
            "kafka" => Ok(Self::Kafka),
            "rabbitmq" | "amqp" | "rabbit" => Ok(Self::Rabbitmq),
            other => Err(format!(
                "unknown --bus {other:?} (expected memory, sqlite, postgres, nats, kafka, or rabbitmq)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::Nats => "nats",
            Self::Kafka => "kafka",
            Self::Rabbitmq => "rabbitmq",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockKind {
    Memory,
    Sqlite,
    Postgres,
}

impl LockKind {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "memory" | "mem" | "in-memory" => Ok(Self::Memory),
            "sqlite" => Ok(Self::Sqlite),
            "postgres" | "pg" => Ok(Self::Postgres),
            other => Err(format!(
                "unknown --lock {other:?} (expected memory, sqlite, or postgres)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

pub fn compatible_lock(repo: RepoKind, lock: LockKind) -> Result<(), String> {
    match (repo, lock) {
        (_, LockKind::Memory) => Ok(()),
        (RepoKind::Sqlite, LockKind::Sqlite) => Ok(()),
        (RepoKind::Postgres, LockKind::Postgres) => Ok(()),
        (repo, lock) => Err(format!(
            "lock={} is not valid with repo={} (sqlite lock needs sqlite repo, postgres lock needs postgres repo)",
            lock.as_str(),
            repo.as_str()
        )),
    }
}
