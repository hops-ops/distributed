//! Leaderboard section of the client manifest: server-ranked board queries
//! (`top`, `around_me`, `my_rank`, `rank`) generated into `leaderboards.ts`.
//! They are not read-model roots and never enter the replica cache.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::*;

/// The only row type the framework emits for leaderboards.
pub(crate) const LEADERBOARD_ENTRY_TYPENAME: &str = "LeaderboardEntry";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestLeaderboard {
    pub(crate) name: String,
    pub(crate) entry_typename: String,
    pub(crate) order: String,
    pub(crate) boards: String,
    pub(crate) default_first: u32,
    pub(crate) max_first: u32,
    pub(crate) max_radius: u32,
    pub(crate) roots: Vec<ManifestLeaderboardRoot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestLeaderboardRoot {
    pub(crate) name: String,
    pub(crate) operation: String,
    pub(crate) list: bool,
    pub(crate) live: bool,
    pub(crate) arguments: Vec<ManifestLeaderboardArgument>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestLeaderboardArgument {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) nullable: bool,
}

fn invalid(message: impl Into<String>) -> ClientCompileError {
    ClientCompileError::manifest("client.manifest.leaderboard", message)
}

/// Validate the leaderboard section against itself and the read-model roots.
pub(crate) fn validate_leaderboards(
    leaderboards: &[ManifestLeaderboard],
    roots: &std::collections::BTreeMap<(RootOperation, String), ManifestRoot>,
    typenames: &BTreeSet<String>,
) -> Result<(), ClientCompileError> {
    if !leaderboards.is_empty() && typenames.contains(LEADERBOARD_ENTRY_TYPENAME) {
        return Err(invalid(format!(
            "model typename `{LEADERBOARD_ENTRY_TYPENAME}` collides with the leaderboard entry type"
        )));
    }
    let mut names = BTreeSet::new();
    let mut root_names = roots
        .keys()
        .map(|(_, name)| name.clone())
        .collect::<BTreeSet<_>>();
    for leaderboard in leaderboards {
        if !is_graphql_name(&leaderboard.name) || !names.insert(leaderboard.name.clone()) {
            return Err(invalid(format!(
                "invalid or duplicate leaderboard `{}`",
                leaderboard.name
            )));
        }
        if leaderboard.entry_typename != LEADERBOARD_ENTRY_TYPENAME {
            return Err(invalid(format!(
                "leaderboard `{}` entry type must be `{LEADERBOARD_ENTRY_TYPENAME}`",
                leaderboard.name
            )));
        }
        if !matches!(leaderboard.order.as_str(), "high_first" | "low_first")
            || !matches!(leaderboard.boards.as_str(), "single" | "family")
        {
            return Err(invalid(format!(
                "leaderboard `{}` has an unknown order or board kind",
                leaderboard.name
            )));
        }
        if leaderboard.default_first == 0
            || leaderboard.default_first > leaderboard.max_first
            || leaderboard.max_radius == 0
        {
            return Err(invalid(format!(
                "leaderboard `{}` has inconsistent limits",
                leaderboard.name
            )));
        }
        if leaderboard.roots.is_empty() {
            return Err(invalid(format!(
                "leaderboard `{}` exposes no roots",
                leaderboard.name
            )));
        }
        let family = leaderboard.boards == "family";
        for root in &leaderboard.roots {
            if !is_graphql_name(&root.name) || !root_names.insert(root.name.clone()) {
                return Err(invalid(format!(
                    "leaderboard root `{}` is invalid or collides with another root",
                    root.name
                )));
            }
            let (list, live, extra): (bool, bool, Option<(&str, &str, bool)>) =
                match root.operation.as_str() {
                    "top" => (true, root.live, Some(("first", "Int", true))),
                    "around_me" => (true, root.live, Some(("radius", "Int", true))),
                    "my_rank" => (false, root.live, None),
                    "rank" => (false, false, Some(("member", "String", false))),
                    other => {
                        return Err(invalid(format!(
                            "leaderboard root `{}` has unknown operation `{other}`",
                            root.name
                        )))
                    }
                };
            let mut expected = Vec::new();
            if family {
                expected.push(("board", "String", false));
            }
            expected.extend(extra);
            let actual = root
                .arguments
                .iter()
                .map(|argument| {
                    (
                        argument.name.as_str(),
                        argument.type_name.as_str(),
                        argument.nullable,
                    )
                })
                .collect::<Vec<_>>();
            if root.list != list || root.live != live || actual != expected {
                return Err(invalid(format!(
                    "leaderboard root `{}` does not match its `{}` contract",
                    root.name, root.operation
                )));
            }
        }
    }
    Ok(())
}
