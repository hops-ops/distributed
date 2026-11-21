//! Compiling is the test: the dependency is called `framework`, so a macro
//! that still writes `distributed::` does not build here.

use framework::command::Succeeded;
use framework::{command, Aggregate, Entity, ReadModel, RelationalReadModel, Snapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, Snapshot)]
#[snapshot(id = "note_id")]
struct Note {
    #[serde(skip, default)]
    entity: Entity,
    note_id: String,
    body: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, framework::DomainState)]
#[domain_state(version = 1)]
struct NoteState {
    note_id: String,
    body: String,
}

impl From<&Note> for NoteState {
    fn from(note: &Note) -> Self {
        Self {
            note_id: note.note_id.clone(),
            body: note.body.clone(),
        }
    }
}

#[framework::sourced(entity, events = "NoteEvent", aggregate_type = "note", domain_state = NoteState)]
impl Note {
    pub fn write(&mut self, note_id: &str, body: &str) -> Result<(), framework::EventRecordError> {
        self.written(note_id.to_string(), body.to_string())
    }

    #[event("note.written", version = 1, domain)]
    fn written(&mut self, note_id: String, body: String) {
        self.entity.set_id(&note_id);
        self.note_id = note_id;
        self.body = body;
    }
}

#[derive(Debug, Deserialize, framework::CommandInput)]
struct WriteInput {
    note_id: String,
    body: String,
}

#[derive(Debug, Serialize, framework::CommandOutput)]
struct WritePayload {
    note_id: String,
}

command! {
    name: "note.write",
    transition: domain_commands::Write,
    aggregate: Note,
    input: WriteInput,
    outcome: Succeeded<WritePayload>,
    shard: |input| input.note_id.clone(),
    load: or_create,
    roles: ["user"],
    field: "notes_write",
    invoke: |note, input, _principal| note.write(&input.note_id, &input.body),
    payload: |note, _principal| WritePayload { note_id: note.note_id.clone() },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("notes")]
struct Notes {
    #[id("note_id")]
    note_id: String,
    body: String,
}

#[test]
fn generated_paths_follow_a_renamed_dependency() {
    let mut note = Note::default();
    note.write("note-1", "hello").unwrap();
    assert_eq!(Note::aggregate_type(), "note");
    assert_eq!(note.snapshot().note_id, "note-1");
    assert_eq!(Notes::schema().table_name, "notes");
    assert_eq!(Write::COMMAND, "note.write");
    let save = framework::mutation! {
        mutation SaveNote {
            upsert_notes(object: $input.note)
        }
    };
    assert_eq!(save.program().unwrap().name(), "SaveNote");
}
