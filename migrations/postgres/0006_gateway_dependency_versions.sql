-- Private gateway dependency versions. Runtime cache activation installs
-- per-table transactional hooks after application read-model migrations.
CREATE TABLE IF NOT EXISTS distributed_gateway_versions (
    namespace TEXT NOT NULL,
    table_name TEXT NOT NULL,
    epoch TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    PRIMARY KEY (namespace, table_name)
);
