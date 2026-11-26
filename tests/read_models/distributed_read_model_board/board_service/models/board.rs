use distributed::{sourced, Entity, Snapshot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardState {
    pub card_id: String,
    pub column: String,
    pub title: String,
    pub labels: Vec<String>,
    pub assignee: Option<String>,
}

#[derive(Default, Snapshot)]
pub struct Board {
    pub entity: Entity,
    pub name: String,
    pub cards: Vec<CardState>,
}

#[sourced(entity, aggregate_type = "board")]
impl Board {
    #[event("opened")]
    pub fn open(&mut self, id: String, name: String) {
        self.entity.set_id(&id);
        self.name = name;
    }

    #[event("card_added")]
    pub fn add_card(
        &mut self,
        card_id: String,
        column: String,
        title: String,
        labels: Vec<String>,
        assignee: Option<String>,
    ) {
        if !self.cards.iter().any(|card| card.card_id == card_id) {
            self.cards.push(CardState {
                card_id,
                column,
                title,
                labels,
                assignee,
            });
        }
    }

    #[event("card_moved")]
    pub fn move_card(&mut self, card_id: String, column: String) {
        if let Some(card) = self.cards.iter_mut().find(|card| card.card_id == card_id) {
            card.column = column;
        }
    }

    #[event("card_removed")]
    pub fn remove_card(&mut self, card_id: String) {
        self.cards.retain(|card| card.card_id != card_id);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, distributed::CommandInput)]
pub struct OpenBoard {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, distributed::CommandInput)]
pub struct AddCard {
    pub id: String,
    pub card_id: String,
    pub column: String,
    pub title: String,
    pub labels: Vec<String>,
    pub assignee: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, distributed::CommandInput)]
pub struct MoveCard {
    pub id: String,
    pub card_id: String,
    pub column: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, distributed::CommandInput)]
pub struct RemoveCard {
    pub id: String,
    pub card_id: String,
}
