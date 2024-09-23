use std::collections::HashMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Some kind of filesystem update to a single path.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Event {
    Create(PathBuf),
    Delete(PathBuf),
    Modify(PathBuf),
    Rename(PathBuf, PathBuf),
}
impl Event {
    /// Gets the path this event operates on. For rename events, this will be the old path.
    pub fn path(&self) -> &Path {
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
fn debounce_two(event_1: Option<Event>, event_2: Event, curr_path: PathBuf) -> Event {
    match (&event_1, &event_2) {
        (None, _) => event_2,
        (Some(event_1), event_2) => match (event_1, event_2) {
            // Create-then-delete is nothing, but we store it as a deletion because otherwise a
            // create-rename-delete would be stored as just a rename. For most purposes, this is
            // fine, but for conflict detection, this could cause re-creation. Better to record
            // absolutely that a deletion has occurred.
            (Event::Create(_), Event::Delete(_)) => Event::Delete(curr_path),
            // Create-then-modify is just a create (we haven't observed the pre-modification state)
            (Event::Create(_), Event::Modify(_)) => Event::Create(curr_path),
            // Double create is just create
            (Event::Create(_), Event::Create(_)) => Event::Create(curr_path),
            (Event::Create(_), Event::Rename(_, _)) => unreachable!(),

            // Delete-then-create is a modification
            (Event::Delete(_), Event::Create(_)) => Event::Modify(curr_path),
            // Delete-then-modify shouldn't be possible, but it would basically be a modification
            (Event::Delete(_), Event::Modify(_)) => Event::Modify(curr_path),
            // Double delete is just delete
            (Event::Delete(_), Event::Delete(_)) => Event::Delete(curr_path),
            (Event::Delete(_), Event::Rename(_, _)) => unreachable!(),

            // Modify-then-create shouldn't be possible, but it would basically be a modification
            (Event::Modify(_), Event::Create(_)) => Event::Modify(curr_path),
            // Modify-then-delete is just a deletion
            (Event::Modify(_), Event::Delete(_)) => Event::Delete(curr_path),
            // Double modify is just one modify
            (Event::Modify(_), Event::Modify(_)) => Event::Modify(curr_path),
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebouncedEvents {
    /// A map from *new* paths (after every rename) to the old path (if there was a rename) and an
    /// event that has occurred on that path, if there is one.
    ///
    /// It is guaranteed that there will not be a `(None, None)` value in any of these entries.
    /// Those with only renames will just have a path, those with no rename will just have another
    /// event, and those with both will have both. Those with neither will not be recorded.
    inner: HashMap<PathBuf, (Option<PathBuf>, Option<Event>)>,
}
impl DebouncedEvents {
    /// Creates a new instance of [`DebouncedEvents`], with no events yet.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
    /// Creates a new instance of [`DebouncedEvents`], with the given events, debounced.
    pub fn from_sequential(events: impl Iterator<Item = Event>) -> Self {
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
                        (None, Some(Event::Create(entry.path().to_path_buf()))),
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
    pub fn extend_from_sequential(&mut self, events: impl Iterator<Item = Event>) {
        for event in events {
            self.push(event);
        }
    }
    /// Pushes a single event into this set of [`DebouncedEvents`], debouncing it.
    pub fn push(&mut self, event: Event) {
        if let Event::Rename(from, to) = event {
            if let Some((oldest_path, event)) = self.inner.remove(&from) {
                // We'll insert back under the new path, using the previous path as the old
                // path if there haven't been any prior renames, or the `from` path from the
                // earliest of them if there have been (ensuring the original path can be
                // found). This essentially condenses all renames into one.
                self.inner.insert(
                    to.clone(),
                    (
                        Some(oldest_path.unwrap_or(from)),
                        // Shift the event to happening on the new path (always valid)
                        event.map(|e| e.with_path(to)),
                    ),
                );
            } else {
                // This is a rename of a path we haven't seen any other events for
                self.inner.insert(to, (Some(from), None));
            }
        } else {
            self.inner
                .entry(event.path().to_path_buf())
                .and_modify(|(_, curr_event_ref)| {
                    let curr_event = std::mem::take(curr_event_ref);
                    *curr_event_ref = Some(debounce_two(
                        curr_event,
                        event.clone(),
                        event.path().to_path_buf(),
                    ));
                })
                .or_insert((None, Some(event)));
        }
    }
    /// Combines this set of [`DebouncedEvents`] with another, which is assumed to come after this
    /// one.
    pub fn combine(&mut self, other: &DebouncedEvents) {
        // We apply renames first, and all other events later so we only rename things in the
        // current set of debounced events, not in our own. If we saw a rename *after* a recreation
        // event in `other`, for example, the rename would apply to it, corrupting that path. As
        // such, we apply renames in the moment and store the rest for later.
        let mut non_renames = Vec::with_capacity(other.inner.len());
        for (new_path, old_path, event) in other.iter() {
            if let Some(old_path) = old_path {
                // We need to apply
                self.push(Event::Rename(old_path.clone(), new_path.clone()));
            }
            if let Some(event) = event {
                // The event will be registered on the new path, and if we needed to rename we just
                // have
                non_renames.push(event.clone());
            }
        }
        for ev in non_renames {
            self.push(ev);
        }
    }
    /// Consumes this set of [`DebouncedEvents`], returning a series of entries of new paths, old
    /// paths, and an event, if one occurred there.
    ///
    /// All paths are guaranteed to have either an old path or an event, or both. Note that
    /// create-then-deletes will be registered as deletes of previously nonexistent paths for
    /// clarity.
    pub fn into_iter(self) -> impl Iterator<Item = (PathBuf, Option<PathBuf>, Option<Event>)> {
        self.inner
            .into_iter()
            .map(|(new_path, (old_path, event))| (new_path, old_path, event))
    }
    pub fn iter(&self) -> impl Iterator<Item = (&PathBuf, &Option<PathBuf>, &Option<Event>)> {
        self.inner
            .iter()
            .map(|(new_path, (old_path, event))| (new_path, old_path, event))
    }
}
