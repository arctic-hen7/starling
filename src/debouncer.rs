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
    /// Updates the path on this event. For rename events, the old path will be changed and the new
    /// path left unaltered.
    fn with_path(self, p: PathBuf) -> Self {
        match self {
            Event::Create(_) => Event::Create(p),
            Event::Delete(_) => Event::Delete(p),
            Event::Modify(_) => Event::Modify(p),
            Event::Rename(_, to) => Event::Rename(p, to),
        }
    }
}

/// Debounces two events, which are assumed to act on the same path. This is where we define
/// fundamental debouncing rules.
///
/// Apart from the case of creation then deletion, all combinations of two events will produce a
/// single event.
///
/// This also takes the last path the events apply to, extracted from a traversal of all renames.
/// This avoids cumbersome rename combination and allows renames to be instantly handled. Neither
/// of the provided events should be renames.
fn debounce_two(event_1: Option<Event>, event_2: Event, curr_path: PathBuf) -> Option<Event> {
    match (&event_1, &event_2) {
        (None, _) => Some(event_2),
        (Some(event_1), event_2) => match (event_1, event_2) {
            // Create-then-delete is nothing
            (Event::Create(_), Event::Delete(_)) => None,
            // Create-then-modify is just a create (we haven't observed the pre-modification state)
            (Event::Create(_), Event::Modify(_)) => Some(Event::Create(curr_path)),
            // Double create is just create
            (Event::Create(_), Event::Create(_)) => Some(Event::Create(curr_path)),
            (Event::Create(_), Event::Rename(_, _)) => unreachable!(),

            // Delete-then-create is a modification
            (Event::Delete(_), Event::Create(_)) => Some(Event::Modify(curr_path)),
            // Delete-then-modify shouldn't be possible, but it would basically be a modification
            (Event::Delete(_), Event::Modify(_)) => Some(Event::Modify(curr_path)),
            // Double delete is just delete
            (Event::Delete(_), Event::Delete(_)) => Some(Event::Delete(curr_path)),
            (Event::Delete(_), Event::Rename(_, _)) => unreachable!(),

            // Modify-then-create shouldn't be possible, but it would basically be a modification
            (Event::Modify(_), Event::Create(_)) => Some(Event::Modify(curr_path)),
            // Modify-then-delete is just a deletion
            (Event::Modify(_), Event::Delete(_)) => Some(Event::Delete(curr_path)),
            // Double modify is just one modify
            (Event::Modify(_), Event::Modify(_)) => Some(Event::Modify(curr_path)),
            (Event::Modify(_), Event::Rename(_, _)) => unreachable!(),

            (Event::Rename(_, _), _) => unreachable!(),
        },
    }
}

/// A series of debounced filesystem events, organised as a map from new paths to their old paths
/// and the single event that has occurred on that path.
///
/// For paths which have not been renamed, the old path in the value will be `None`; for those
/// which have been renamed, the path in the key will be the new path and the path in the value
/// will be the old path.
///
/// The event inside the value of each entry would be `None` if the only thing that happened to the
/// path in question was a rename.
pub struct DebouncedEvents {
    /// A map from *new* paths (after every rename) to the old path (if there was a rename) and all
    /// the events which have occurred on that path. Outside of [`Self::extend_from_sequential`],
    /// there will only ever be zero or one event per path. But, for accumulating, we need to be
    /// able to add many.
    inner: HashMap<PathBuf, (Option<PathBuf>, Vec<Event>)>,
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
                        (None, vec![Event::Create(entry.path().to_path_buf())]),
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
        // First, collate events for each path into a map following the same structure as the final
        // one, but using a list of events instead of just one
        for event in events {
            if let Event::Rename(from, to) = event {
                if let Some((oldest_path, existing_events)) = self.inner.remove(&from) {
                    // We'll insert back under the new path, using the previous path as the old
                    // path if there haven't been any prior renames, or the `from` path from the
                    // earliest of them if there have been (ensuring the original path can be
                    // found). This essentially condenses all renames into one.
                    self.inner
                        .insert(to, (Some(oldest_path.unwrap_or(from)), existing_events));
                } else {
                    // This is a rename of a path we haven't seen any other events for
                    self.inner.insert(to, (Some(from), Vec::new()));
                }
            } else {
                self.inner
                    .entry(event.path().to_path_buf())
                    .and_modify(|(_, events)| events.push(event.clone()))
                    .or_insert((None, vec![event]));
            }
        }

        // Now go through and debounce all those
        for (new_path, (_, sequential_events)) in self.inner.iter_mut() {
            *sequential_events = sequential_events
                .drain(..)
                .fold(None, |acc, ev| debounce_two(acc, ev, new_path.clone()))
                // Convert an `Option<T>` into a `Vec<T>`
                .map(|ev| vec![ev])
                .unwrap_or_default();
        }
    }
    /// Consumes this set of [`DebouncedEvents`], returning a series of entries of new paths, old
    /// paths, and an event that might have occurred there.
    pub fn into_iter(self) -> impl Iterator<Item = (PathBuf, Option<PathBuf>, Option<Event>)> {
        self.inner
            .into_iter()
            .map(|(new_path, (old_path, mut events))| {
                // There will only ever be one event or none
                let event = if events.is_empty() {
                    None
                } else if events.len() == 1 {
                    // `.pop()` goes from the back, but we've only got one
                    Some(events.pop().unwrap())
                } else {
                    unreachable!()
                };
                (new_path, old_path, event)
            })
    }
}
