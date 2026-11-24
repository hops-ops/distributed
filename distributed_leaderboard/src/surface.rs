//! Leaderboard surfaces: sorted-set boards exposed through the generated
//! GraphQL surface, client manifest and TS client as an
//! [`ExternalReadSource`].
//!
//! A [`LeaderboardSurface`] declares one board (or one board *family*, keyed
//! by a `board` argument) and per-role grants. The pool-free client manifest
//! export (`Surface::with_external_sources`) and the runtime engine
//! ([`LeaderboardRoots`](crate::LeaderboardRoots)) share one declaration.
//! For a surface named `xp` the roots are:
//!
//! | Root | Grant | Arguments | Result |
//! |---|---|---|---|
//! | `xpLeaderboard` | `top` | `board` (family), `first` | `[LeaderboardEntry!]!` |
//! | `xpLeaderboardAround` | `around_me` | `board` (family), `radius` | `[LeaderboardEntry!]!` |
//! | `xpMyRank` | `my_rank` | `board` (family) | `LeaderboardEntry` |
//! | `xpRank` | `rank_any_member` | `board` (family), `member` | `LeaderboardEntry` |
//!
//! `xpLeaderboardAround` and `xpMyRank` rank the **viewer**: the member is the
//! verified session claim [`LeaderboardSurface::member_claim`] (default
//! `x-user-id`), never an argument. Ranking an arbitrary member needs the
//! separate `rank_any_member` grant, which adds `xpRank(member:)`.
//! `xpLeaderboard`, `xpLeaderboardAround` and `xpMyRank` also exist as
//! subscription roots for `@live`; they refresh when the board's change key
//! ([`leaderboard_change_key`]) or [`ALL_BOARDS_CHANGE_KEY`](crate::ALL_BOARDS_CHANGE_KEY)
//! is published.

use std::collections::BTreeMap;
use std::sync::Arc;

use distributed::graphql::surface::{
    ExternalField, ExternalReadSource, ExternalRoot, ExternalType,
};
use distributed::microsvc::USER_ID_KEY;

use crate::manifest::{ClientLeaderboard, ClientLeaderboardArgument, ClientLeaderboardRoot};
use crate::{Board, ScoreOrder, LEADERBOARDS_SECTION};

/// GraphQL object type of one ranked row.
pub const LEADERBOARD_ENTRY_TYPE: &str = "LeaderboardEntry";
/// Default `first` of a top-N query.
pub const LEADERBOARD_DEFAULT_FIRST: u32 = 10;
/// Default cap on `first`.
pub const LEADERBOARD_MAX_FIRST: u32 = 100;
/// Default cap on `radius`.
pub const LEADERBOARD_MAX_RADIUS: u32 = 25;
/// Longest board family key.
pub const MAX_LEADERBOARD_KEY_LEN: usize = 64;

/// The live-invalidation key of a board: `leaderboard:{board name}`. Reads
/// also listen on [`ALL_BOARDS_CHANGE_KEY`](crate::ALL_BOARDS_CHANGE_KEY).
pub fn leaderboard_change_key(board: &str) -> String {
    format!("leaderboard:{board}")
}

/// Which boards a surface serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderboardBoards {
    /// Exactly this board; roots take no `board` argument.
    Single { board: String },
    /// Boards `{prefix}:{key}`; roots take `board: String!` = `key`
    /// (1..=64 bytes of `[A-Za-z0-9._-]`), e.g. a season id.
    Family { prefix: String },
}

/// What one role may ask of a leaderboard surface.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaderboardGrant {
    pub top: bool,
    pub around_me: bool,
    pub my_rank: bool,
    pub rank_any_member: bool,
}

impl LeaderboardGrant {
    /// Top-N, the viewer's neighbourhood and the viewer's own rank.
    pub fn read() -> Self {
        Self {
            top: true,
            around_me: true,
            my_rank: true,
            rank_any_member: false,
        }
    }

    /// Top-N only.
    pub fn top_only() -> Self {
        Self {
            top: true,
            ..Self::default()
        }
    }

    /// Also allow ranking any member by id (moderation, support tools).
    pub fn rank_any_member(mut self) -> Self {
        self.rank_any_member = true;
        self
    }

    fn is_empty(&self) -> bool {
        !(self.top || self.around_me || self.my_rank || self.rank_any_member)
    }

    fn intersect(self, other: Self) -> Self {
        Self {
            top: self.top && other.top,
            around_me: self.around_me && other.around_me,
            my_rank: self.my_rank && other.my_rank,
            rank_any_member: self.rank_any_member && other.rank_any_member,
        }
    }
}

/// One leaderboard root operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeaderboardOperation {
    Top,
    AroundMe,
    MyRank,
    Rank,
}

impl LeaderboardOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Top => "top",
            Self::AroundMe => "around_me",
            Self::MyRank => "my_rank",
            Self::Rank => "rank",
        }
    }

    /// `true` for list results, `false` for a single nullable entry.
    pub fn is_list(self) -> bool {
        matches!(self, Self::Top | Self::AroundMe)
    }

    /// Whether the operation has a subscription (`@live`) twin.
    pub fn is_live(self) -> bool {
        !matches!(self, Self::Rank)
    }

    /// Whether the member comes from the viewer's session claim.
    pub fn ranks_viewer(self) -> bool {
        matches!(self, Self::AroundMe | Self::MyRank)
    }
}

/// A leaderboard (or board family) exposed through GraphQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderboardSurface {
    pub(crate) name: String,
    pub(crate) boards: LeaderboardBoards,
    pub(crate) order: ScoreOrder,
    pub(crate) member_claim: String,
    pub(crate) default_first: u32,
    pub(crate) max_first: u32,
    pub(crate) max_radius: u32,
    pub(crate) grants: BTreeMap<String, LeaderboardGrant>,
}

impl LeaderboardSurface {
    /// Serve exactly `board` under GraphQL base name `name` (lowerCamel,
    /// `[A-Za-z][A-Za-z0-9]*`).
    pub fn single(name: impl Into<String>, board: &Board) -> Self {
        Self::new(
            name,
            LeaderboardBoards::Single {
                board: board.name().to_string(),
            },
            board.order(),
        )
    }

    /// Serve the family `{prefix}:{key}`; queries pass `board: key`.
    pub fn family(name: impl Into<String>, prefix: impl Into<String>, order: ScoreOrder) -> Self {
        Self::new(
            name,
            LeaderboardBoards::Family {
                prefix: prefix.into(),
            },
            order,
        )
    }

    fn new(name: impl Into<String>, boards: LeaderboardBoards, order: ScoreOrder) -> Self {
        Self {
            name: name.into(),
            boards,
            order,
            member_claim: USER_ID_KEY.to_string(),
            default_first: LEADERBOARD_DEFAULT_FIRST,
            max_first: LEADERBOARD_MAX_FIRST,
            max_radius: LEADERBOARD_MAX_RADIUS,
            grants: BTreeMap::new(),
        }
    }

    /// Session claim that names the viewer's member (default `x-user-id`;
    /// e.g. a character-id claim for character-keyed boards).
    pub fn member_claim(mut self, claim: impl Into<String>) -> Self {
        self.member_claim = claim.into();
        self
    }

    /// Default and maximum `first`, and maximum `radius`.
    pub fn limits(mut self, default_first: u32, max_first: u32, max_radius: u32) -> Self {
        self.default_first = default_first;
        self.max_first = max_first;
        self.max_radius = max_radius;
        self
    }

    /// Grant `role` these operations. A later grant for a role replaces it.
    pub fn grant(mut self, role: impl Into<String>, grant: LeaderboardGrant) -> Self {
        self.grants.insert(role.into(), grant);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn boards(&self) -> &LeaderboardBoards {
        &self.boards
    }

    pub fn order(&self) -> ScoreOrder {
        self.order
    }

    pub fn get_member_claim(&self) -> &str {
        &self.member_claim
    }

    pub fn default_first(&self) -> u32 {
        self.default_first
    }

    pub fn max_first(&self) -> u32 {
        self.max_first
    }

    pub fn max_radius(&self) -> u32 {
        self.max_radius
    }

    pub fn grants(&self) -> &BTreeMap<String, LeaderboardGrant> {
        &self.grants
    }

    /// Operations this surface exposes: on a role or application selection
    /// its one grant, on the catalog the union of every role's grant.
    pub(crate) fn selected_grant(&self) -> LeaderboardGrant {
        self.grants
            .values()
            .fold(LeaderboardGrant::default(), |all, grant| LeaderboardGrant {
                top: all.top || grant.top,
                around_me: all.around_me || grant.around_me,
                my_rank: all.my_rank || grant.my_rank,
                rank_any_member: all.rank_any_member || grant.rank_any_member,
            })
    }

    /// Resolve the board a query names (`key` is the `board` argument).
    pub fn board(&self, key: Option<&str>) -> Result<Board, String> {
        let name = match (&self.boards, key) {
            (LeaderboardBoards::Single { board }, None) => board.clone(),
            (LeaderboardBoards::Family { prefix }, Some(key)) => {
                if key.is_empty()
                    || key.len() > MAX_LEADERBOARD_KEY_LEN
                    || !key
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
                {
                    return Err(format!(
                        "board must be 1..={MAX_LEADERBOARD_KEY_LEN} bytes of [A-Za-z0-9._-]"
                    ));
                }
                format!("{prefix}:{key}")
            }
            (LeaderboardBoards::Single { .. }, Some(_)) => {
                return Err(format!(
                    "leaderboard `{}` takes no board argument",
                    self.name
                ))
            }
            (LeaderboardBoards::Family { .. }, None) => {
                return Err(format!(
                    "leaderboard `{}` requires a board argument",
                    self.name
                ))
            }
        };
        Board::new(name, self.order).map_err(|error| error.message().to_string())
    }

    /// Roots for `grant`, with their operations ([`LeaderboardOperation::is_live`]
    /// ones are `live`).
    pub fn roots_for(&self, grant: LeaderboardGrant) -> Vec<(LeaderboardOperation, ExternalRoot)> {
        let family = matches!(self.boards, LeaderboardBoards::Family { .. });
        let mut roots = Vec::new();
        let mut push =
            |suffix: &str, operation: LeaderboardOperation, extra: Option<ExternalField>| {
                let mut arguments = Vec::new();
                if family {
                    arguments.push(ExternalField::new("board", "String", false));
                }
                arguments.extend(extra);
                roots.push((
                    operation,
                    ExternalRoot {
                        name: format!("{}{suffix}", self.name),
                        arguments,
                        type_name: LEADERBOARD_ENTRY_TYPE.to_string(),
                        list: operation.is_list(),
                        live: operation.is_live(),
                    },
                ));
            };
        if grant.top {
            push(
                "Leaderboard",
                LeaderboardOperation::Top,
                Some(ExternalField::new("first", "Int", true)),
            );
        }
        if grant.around_me {
            push(
                "LeaderboardAround",
                LeaderboardOperation::AroundMe,
                Some(ExternalField::new("radius", "Int", true)),
            );
        }
        if grant.my_rank {
            push("MyRank", LeaderboardOperation::MyRank, None);
        }
        if grant.rank_any_member {
            push(
                "Rank",
                LeaderboardOperation::Rank,
                Some(ExternalField::new("member", "String", false)),
            );
        }
        roots
    }

    /// Validate the declaration (names, limits, board, roles).
    fn check(&self) -> Result<(), String> {
        let mut chars = self.name.chars();
        if !chars.next().is_some_and(|c| c.is_ascii_lowercase())
            || !chars.all(|c| c.is_ascii_alphanumeric())
        {
            return Err(format!(
                "leaderboard surface name `{}` must be lowerCamel [a-z][A-Za-z0-9]*",
                self.name
            ));
        }
        if self.member_claim.trim().is_empty() {
            return Err(format!("leaderboard `{}` needs a member claim", self.name));
        }
        if self.default_first == 0 || self.default_first > self.max_first || self.max_radius == 0 {
            return Err(format!(
                "leaderboard `{}` limits must satisfy 0 < default_first <= max_first and max_radius > 0",
                self.name
            ));
        }
        match &self.boards {
            LeaderboardBoards::Single { board } => Board::new(board.clone(), self.order)
                .map(|_| ())
                .map_err(|error| error.message().to_string())?,
            LeaderboardBoards::Family { prefix } => Board::new(
                format!("{prefix}:{}", "k".repeat(MAX_LEADERBOARD_KEY_LEN)),
                self.order,
            )
            .map(|_| ())
            .map_err(|error| error.message().to_string())?,
        }
        for (role, grant) in &self.grants {
            if role.trim().is_empty() {
                return Err(format!("leaderboard `{}` grants an empty role", self.name));
            }
            if grant.is_empty() {
                return Err(format!(
                    "leaderboard `{}` grants role `{role}` nothing",
                    self.name
                ));
            }
        }
        Ok(())
    }

    /// The declaration as `roles` jointly see it (the intersection of their
    /// grants), granted to `grantee`; `None` when nothing is common.
    fn selected(&self, grantee: &str, roles: &[String]) -> Option<Self> {
        let mut common: Option<LeaderboardGrant> = None;
        for role in roles {
            let grant = *self.grants.get(role)?;
            common = Some(common.map_or(grant, |common| common.intersect(grant)));
        }
        let common = common.filter(|grant| !grant.is_empty())?;
        Some(Self {
            grants: BTreeMap::from([(grantee.to_string(), common)]),
            ..self.clone()
        })
    }

    fn canonical_value(&self) -> serde_json::Value {
        let grants = self
            .grants
            .iter()
            .map(|(role, grant)| {
                (
                    role.clone(),
                    serde_json::json!({
                        "top": grant.top,
                        "around_me": grant.around_me,
                        "my_rank": grant.my_rank,
                        "rank_any_member": grant.rank_any_member,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        serde_json::json!({
            "name": self.name,
            "boards": match &self.boards {
                LeaderboardBoards::Single { board } => serde_json::json!({"single": board}),
                LeaderboardBoards::Family { prefix } => serde_json::json!({"family": prefix}),
            },
            "order": order_str(self.order),
            "member_claim": self.member_claim,
            "default_first": self.default_first,
            "max_first": self.max_first,
            "max_radius": self.max_radius,
            "grants": grants,
        })
    }
}

/// `high_first` / `low_first`.
fn order_str(order: ScoreOrder) -> &'static str {
    match order {
        ScoreOrder::HighFirst => "high_first",
        ScoreOrder::LowFirst => "low_first",
    }
}

impl ExternalReadSource for LeaderboardSurface {
    fn section(&self) -> &str {
        LEADERBOARDS_SECTION
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn roles(&self) -> Vec<String> {
        self.grants.keys().cloned().collect()
    }

    fn select(&self, grantee: &str, roles: &[String]) -> Option<Arc<dyn ExternalReadSource>> {
        self.selected(grantee, roles)
            .map(|selected| Arc::new(selected) as Arc<dyn ExternalReadSource>)
    }

    fn types(&self) -> Vec<ExternalType> {
        vec![ExternalType {
            name: LEADERBOARD_ENTRY_TYPE.to_string(),
            fields: vec![
                ExternalField::new("rank", "Int", false),
                ExternalField::new("member", "String", false),
                ExternalField::new("score", "Float", false),
            ],
        }]
    }

    fn roots(&self) -> Vec<ExternalRoot> {
        self.roots_for(self.selected_grant())
            .into_iter()
            .map(|(_, root)| root)
            .collect()
    }

    fn claims(&self) -> Vec<String> {
        vec![self.member_claim.clone()]
    }

    fn contract(&self) -> serde_json::Value {
        self.canonical_value()
    }

    fn client_manifest(&self, subscriptions: bool) -> serde_json::Value {
        let manifest = ClientLeaderboard {
            name: self.name.clone(),
            entry_typename: LEADERBOARD_ENTRY_TYPE.to_string(),
            order: order_str(self.order).to_string(),
            boards: match self.boards {
                LeaderboardBoards::Single { .. } => "single",
                LeaderboardBoards::Family { .. } => "family",
            }
            .to_string(),
            default_first: self.default_first,
            max_first: self.max_first,
            max_radius: self.max_radius,
            roots: self
                .roots_for(self.selected_grant())
                .into_iter()
                .map(|(operation, root)| ClientLeaderboardRoot {
                    live: subscriptions && root.live,
                    list: root.list,
                    operation: operation.as_str().to_string(),
                    arguments: root
                        .arguments
                        .into_iter()
                        .map(|argument| ClientLeaderboardArgument {
                            name: argument.name,
                            type_name: argument.type_name,
                            nullable: argument.nullable,
                        })
                        .collect(),
                    name: root.name,
                })
                .collect(),
        };
        serde_json::to_value(manifest).expect("a client leaderboard serializes")
    }

    fn validate(&self) -> Result<(), String> {
        self.check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xp() -> LeaderboardSurface {
        LeaderboardSurface::single("xp", &Board::high_first("mobpit.xp").unwrap())
            .grant("user", LeaderboardGrant::read())
            .grant("moderator", LeaderboardGrant::read().rank_any_member())
    }

    #[test]
    fn roots_follow_the_grant() {
        let user = xp().roots_for(LeaderboardGrant::read());
        assert_eq!(
            user.iter()
                .map(|(_, root)| root.name.as_str())
                .collect::<Vec<_>>(),
            ["xpLeaderboard", "xpLeaderboardAround", "xpMyRank"]
        );
        assert!(user
            .iter()
            .all(|(_, root)| root.arguments.iter().all(|arg| arg.name != "member")));
        let moderator = xp().roots_for(LeaderboardGrant::read().rank_any_member());
        let (operation, rank) = moderator.last().unwrap();
        assert_eq!(*operation, LeaderboardOperation::Rank);
        assert_eq!(rank.name, "xpRank");
        assert!(!rank.live);
        assert_eq!(rank.arguments[0].name, "member");
    }

    #[test]
    fn families_take_a_validated_board_key() {
        let season = LeaderboardSurface::family("season", "season.xp", ScoreOrder::HighFirst)
            .grant("user", LeaderboardGrant::top_only());
        let roots = season.roots_for(LeaderboardGrant::top_only());
        assert_eq!(roots[0].1.arguments[0].name, "board");
        assert_eq!(season.board(Some("s3")).unwrap().name(), "season.xp:s3");
        assert!(season.board(None).is_err());
        assert!(season.board(Some("bad key")).is_err());
        assert!(season.board(Some("")).is_err());
        assert!(xp().board(Some("s3")).is_err());
        assert_eq!(xp().board(None).unwrap().name(), "mobpit.xp");
    }

    #[test]
    fn role_and_application_selection_narrow_the_grant() {
        assert!(xp().selected("guest", &["guest".into()]).is_none());
        let user = xp().selected("user", &["user".into()]).unwrap();
        assert_eq!(user.selected_grant(), LeaderboardGrant::read());
        let app = xp()
            .selected("app", &["user".into(), "moderator".into()])
            .unwrap();
        assert_eq!(app.grants().keys().collect::<Vec<_>>(), ["app"]);
        assert_eq!(app.selected_grant(), LeaderboardGrant::read());
        assert!(xp().selected("app", &["guest".into()]).is_none());
    }

    #[test]
    fn rejects_bad_declarations() {
        assert!(
            LeaderboardSurface::single("Xp", &Board::high_first("b").unwrap())
                .check()
                .is_err()
        );
        assert!(xp().limits(0, 10, 5).check().is_err());
        assert!(xp().limits(20, 10, 5).check().is_err());
        assert!(xp()
            .grant("nobody", LeaderboardGrant::default())
            .check()
            .is_err());
        assert!(xp().check().is_ok());
    }
}
