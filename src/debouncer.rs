use std::collections::HashMap;
use std::convert::identity;
use std::path::{Path, PathBuf};

/// Some kind of filesystem update to a single path.
#[derive(Clone, PartialEq, Eq)]
pub enum Event {
    Create(PathBuf),
    Delete(PathBuf),
    Modify(PathBuf),
    Rename(PathBuf, PathBuf),
}
impl Event {
    /// Gets the path this event operates on. For rename events, this will be the old path.
    fn path(&self) -> &Path {
        match self {
            Event::Create(p) => p,
            Event::Delete(p) => p,
            Event::Modify(p) => p,
            Event::Rename(p, _) => p,
        }
    }
}

/// Debounces two events, which are assumed to act on the same path. This is where we define
/// fundamental debouncing rules.
///
/// Nearly all cases will produce a single event as output, though create-then-delete will of
/// course produce none, and modify-then-rename (or vice versa) will produce two events.
///
/// This performs modifications in-place for the caller's convenience.
fn debounce_two(x: usize, y: usize, events: &mut Vec<Option<Event>>) {
    // Extract the two events, replacing them with `None`s, and determine what they should become
    let event_1 = std::mem::take(&mut events[x]);
    let event_2 = std::mem::take(&mut events[y]);
    let (new_1, new_2) = match (event_1, event_2) {
        (None, None) => (None, None),
        (None, Some(e)) => (None, Some(e)),
        (Some(e), None) => (None, Some(e)),
        (Some(e1), Some(e2)) => match (e1, e2) {
            // Create-then-delete is nothing
            (Event::Create(_), Event::Delete(_)) => (None, None),
            // Create-then-modify is just a create (we haven't observed the pre-modification state)
            (Event::Create(_), Event::Modify(p)) => (None, Some(Event::Create(p))),
            // Create-then-rename == create at new path
            (Event::Create(_), Event::Rename(_, new)) => (None, Some(Event::Create(new))),
            // Double create is just create
            (Event::Create(_), Event::Create(p)) => (None, Some(Event::Create(p))),

            // Delete-then-create is a modification
            (Event::Delete(_), Event::Create(p)) => (None, Some(Event::Modify(p))),
            // Delete-then-modify shouldn't be possible, but it would basically be a modification
            (Event::Delete(_), Event::Modify(p)) => (None, Some(Event::Modify(p))),
            // Delete-then-rename shouldn't be possible, but it would basically be a rename
            (Event::Delete(_), Event::Rename(old, new)) => (None, Some(Event::Rename(old, new))),
            // Double delete is just delete
            (Event::Delete(_), Event::Delete(p)) => (None, Some(Event::Delete(p))),

            // Modify-then-create shouldn't be possible, but it would basically be a modification
            (Event::Modify(_), Event::Create(p)) => (None, Some(Event::Modify(p))),
            // Modify-then-delete is just a deletion
            (Event::Modify(_), Event::Delete(p)) => (None, Some(Event::Delete(p))),
            // Modify-then-rename shuld have the rename hoisted to the front. This will only occur for
            // a path with no creation event (i.e. one already tracked), which means renames will be
            // reliably hoisted to the top of the event list.
            (Event::Modify(_), Event::Rename(old, new)) => (
                Some(Event::Rename(old, new.clone())),
                Some(Event::Modify(new)),
            ),
            // Double modify is just one modify
            (Event::Modify(_), Event::Modify(p)) => (None, Some(Event::Modify(p))),

            // NOTE: Paths of the second events here will be the `new` path from the rename
            //
            // Rename-then-create shouldn't be possible, but it would just be a rename
            (Event::Rename(old, new), Event::Create(_)) => (None, Some(Event::Rename(old, new))),
            // Rename-then-delete is a deletion of the old path
            (Event::Rename(old, _), Event::Delete(_)) => (None, Some(Event::Delete(old))),
            // Rename-then-modify is exactly the sequence we want (as for modify-then-rename, this can
            // only happen for already-tracked paths)
            (Event::Rename(old, new), Event::Modify(new_m)) => {
                (Some(Event::Rename(old, new)), Some(Event::Modify(new_m)))
            }
            // Double rename is a rename across the two paths
            (Event::Rename(old, _), Event::Rename(_, new)) => (None, Some(Event::Rename(old, new))),
        },
    };
    events[x] = new_1;
    events[y] = new_2;
}

/// A series of debounced updates to the filesystem, organised by event type. Modifications,
/// creations, and deletions are independent, and may be applied in any order *after* renames, as
/// all rename events are automatically hoisted (e.g. modify-then-rename -> rename-then-modify).
pub struct DebouncedEvents {
    /// Renames of existing files. These must be accounted for **before** any other types of
    /// events.
    pub renames: Vec<(PathBuf, PathBuf)>,
    /// Modifications of existing files.
    pub modifications: Vec<PathBuf>,
    /// Deletions of existing files.
    pub deletions: Vec<PathBuf>,
    /// Creations of new files.
    pub creations: Vec<PathBuf>,
}
impl DebouncedEvents {
    /// Debounces a series of sequential updates into an organised set of debounced updates.
    pub fn from_sequential(events: Vec<Event>) -> Self {
        // First, collate events for each path, resolving renames automatically as we go
        let mut events_by_file: HashMap<PathBuf, Vec<Event>> = HashMap::new();
        for event in events {
            // We'll put this event with other events operating on the same file unless it's a
            // rename, in which case we'll move all the events that happened on `from` to a new
            // place for `to` (any subsequent ones on `from` are acting on a different file)
            let operative_path = if let Event::Rename(ref from, ref to) = event {
                let file_events = events_by_file.remove(from).unwrap_or_default();
                events_by_file.insert(to.to_path_buf(), file_events);
                to.to_path_buf()
            } else {
                event.path().to_path_buf()
            };
            events_by_file
                .entry(operative_path.to_path_buf())
                .and_modify(|events| events.push(event.clone()))
                .or_insert(vec![event]);
        }

        // Now we have a series of totally independent per-file events
        let mut debounced_global = Self {
            renames: Vec::new(),
            modifications: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
        };
        for events in events_by_file.into_values() {
            // Debounce the events on this file in a window of twos (if there's only one, leave it
            // be). For new paths, this will coalesce all events into a creation, and for existing
            // paths, this will coalesce all events into a possible rename and then a
            // modification/deletion. Either way, renames will be hoisted to the top automatically.
            let debounced: Vec<Event> = if events.len() >= 2 {
                // Wrap every event in `Some(_)`, we're about to `None`ify a few of them!
                let mut events = events.into_iter().map(|e| Some(e)).collect::<Vec<_>>();
                for i in 1..events.len() {
                    // This handles all edge cases automatically
                    debounce_two(i - 1, i, &mut events);
                }
                events.into_iter().filter_map(identity).collect()
            } else {
                events.into_iter().collect()
            };

            for event in debounced {
                match event {
                    // Renames are automatically hoisted by `debounce_two`, so this will always
                    // work!
                    Event::Rename(from, to) => debounced_global.renames.push((from, to)),
                    Event::Modify(p) => debounced_global.modifications.push(p),
                    Event::Delete(p) => debounced_global.deletions.push(p),
                    Event::Create(p) => debounced_global.creations.push(p),
                }
            }
        }

        debounced_global
    }
}
