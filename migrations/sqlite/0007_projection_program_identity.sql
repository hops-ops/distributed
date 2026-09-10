-- Preserve the semantic program that authored each causal proof row.
-- NULL is intentional for history written before program identities existed;
-- readers must treat it as unversioned rather than infer the active program.
ALTER TABLE projection_changes ADD COLUMN program_id TEXT;
ALTER TABLE projection_observations ADD COLUMN program_id TEXT;
