// TODO: Can this system handle rename, modify, rename? The second rename would never touch the
// first...

use std::collections::HashMap;
use std::convert::identity;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

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
            // Modify-then-rename shuld have the rename hoisted to the front. This will only occur
            // for a path with no creation event (i.e. one already tracked), which means renames
            // will be reliably hoisted to the top of the event list.
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
            // Rename-then-modify is exactly the sequence we want (as for modify-then-rename, this
            // can only happen for already-tracked paths)
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

/// A series of debounced filesystem events, organised as a map from paths to the events which
/// have occurred on them. Generally, only the values in this map will be used.
///
/// Note that, for renamed paths, the final path will be used as the key in the map.
pub struct DebouncedEvents {
    inner: HashMap<PathBuf, Vec<Event>>,
}
impl DebouncedEvents {
    /// Creates a new instance of [`DebouncedEvents`], with no events yet.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    /// Creates a new instance of [`DebouncedEvents`], with the given events, debounced.
    pub fn from_sequential(events: Vec<Event>) -> Self {
        let mut debounced = Self::new();
        debounced.extend_from_sequential(events);
        debounced
    }
    /// Creates a [`DebouncedEvents`] object of creation events from all the readable paths in a
    /// directory. This will skip paths which cannot be read.
    pub fn start_from_dir(dir: &Path) -> Self {
        Self {
            inner: WalkDir::new(dir)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .map(|entry| {
                    (
                        entry.path().to_path_buf(),
                        vec![Event::Create(entry.path().to_path_buf())],
                    )
                })
                .collect(),
        }
    }
    /// Debounces a series of sequential updates into an organised set of debounced updates,
    /// extending the existing set of debounced events.
    ///
    /// The new events are assumed to have come *after* those previously debounced, and renames
    /// will be treated as such (i.e. operations on files that have been renamed, using the old
    /// path, will be considered operations on different files).
    pub fn extend_from_sequential(&mut self, events: Vec<Event>) {
        // First, collate events for each path, resolving renames automatically as we go
        for event in events {
            // We'll put this event with other events operating on the same file unless it's a
            // rename, in which case we'll move all the events that happened on `from` to a new
            // place for `to` (any subsequent ones on `from` are acting on a different file)
            let operative_path = if let Event::Rename(ref from, ref to) = event {
                let file_events = self.inner.remove(from).unwrap_or_default();
                self.inner.insert(to.to_path_buf(), file_events);
                to.to_path_buf()
            } else {
                event.path().to_path_buf()
            };
            self.inner
                .entry(operative_path.to_path_buf())
                .and_modify(|events| events.push(event.clone()))
                .or_insert(vec![event]);
        }

        for events in self.inner.values_mut() {
            // Debounce the events on this file in a window of twos (if there's only one, leave it
            // be). For new paths, this will coalesce all events into a creation, and for existing
            // paths, this will coalesce all events into a possible rename and then a
            // modification/deletion. Either way, renames will be hoisted to the top automatically.
            if events.len() >= 2 {
                let events_tmp = std::mem::take(events);
                // Wrap every event in `Some(_)`, we're about to `None`ify a few of them!
                let mut events_tmp = events_tmp.into_iter().map(|e| Some(e)).collect::<Vec<_>>();
                for i in 1..events_tmp.len() {
                    // This handles all edge cases automatically
                    debounce_two(i - 1, i, &mut events_tmp);
                }
                *events = events_tmp.into_iter().filter_map(identity).collect()
            }
        }
    }
    /// Consumes this instance of [`DebouncedEvents`], returning all the events on every file.
    pub fn into_vec(self) -> Vec<Event> {
        self.inner
            .into_iter()
            .flat_map(|(_, events)| events)
            .collect()
    }
}
impl Deref for DebouncedEvents {
    type Target = HashMap<PathBuf, Vec<Event>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for DebouncedEvents {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
