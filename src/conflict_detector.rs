use crate::debouncer::{DebouncedEvents, Event};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// A conflict detection system that ensures the writes emerging from processing a filesystem
/// update don't conflict with later filesystem updates, and that out-of-band modifications don't
/// conflict with the filesystem either. This does *not* handle conflicts between two out-of-band
/// modifications, they will simply occur in-order.
///
/// This system does not perform conflict *resolution*, it merely warns of when there is a
/// conflict.
pub struct ConflictDetector {
    /// A map of patch identifiers to information about the patches.
    patch_table: HashMap<u32, PatchTableEntry>,
    /// The index of the next patch that will come through the system. All other entries in the
    /// patch table are actively processing. This will be used to link new updates to events that
    /// occur while they're being processed.
    next_patch: u32,
    /// The reference count of the next patch that will come through the system.
    next_ref_count: u32,
}
impl ConflictDetector {
    /// Creates a new, empty [`ConflictDetector`].
    pub fn new() -> Self {
        Self {
            patch_table: HashMap::new(),
            next_patch: 0,
            next_ref_count: 0,
        }
    }
    /// Registers a new update as starting to be processed right this instant. When that update
    /// later completes, it should pass the number this method returns with any writes to the
    /// filesystem it wants to perform so they can be parsed for conflicts.
    pub fn register_update(&mut self) -> u32 {
        self.next_ref_count += 1;
        self.next_patch
    }
    /// Detects conflicts in the given writes with what has occurred since the update whose
    /// [`Self::register_update`] call returned the given number.
    ///
    /// When the provided writes attempt to write to a file that has been deleted, the write is
    /// dropped. When they try to write to a file that has been renamed, they are adjusted to write
    /// to that file. When they try to write to a file that has been modified (including one that
    /// was renamed and then the renamed path was modified), a conflict is produced.
    ///
    /// This will decrement the "reference count" on the patch with the given index internally,
    /// meaning once this is called for every update that depended on that patch, its information
    /// will be discarded to prevent the conflict detector from growing indefinitely.
    ///
    /// For writes emanating from filesystem processing, this will remove them if they conflict
    /// with a write to the same path from an out-of-band source, as filesystem processing writes
    /// that are not to a path in the original set of events that made up the patch are
    /// definitionally auxiliary, and the out-of-band write takes precedence. To facilitate this,
    /// writes from other sources that do not conflict with the filesystem will be added to the
    /// filtration list for all patches after and including the one provided which are still
    /// processing. **This requires out-of-band writes to call this method *before* filesystem
    /// writes when both are available.**
    ///
    /// Note that this will *not* detect logical conflicts between two updates from any source that
    /// occurred on the same file(s) at the same time. Whatever happens here is effectively up to
    /// chance due to locking --- there will never be an invalid state, but which state takes
    /// precedence is essentially random. It is therefore important that writes are actioned in the
    /// strict order they are sent (but they should be conflict-detected as in the bold section
    /// above).
    ///
    /// # Panics
    ///
    /// This will panic if the provided number is not a valid patch index, or is one that has been
    /// closed already (i.e. you've already tried to detect conflicts for all the updates
    /// registered to it).
    pub fn detect_conflicts(&mut self, patch_idx: u32, writes: Vec<Write>) -> Vec<Write> {
        // Convert the `DebouncedEvents` we used to accumulate into a table that can index by the
        // *old* paths (i.e. those in each write)
        let events_since = self
            .patch_table
            .get_mut(&patch_idx)
            .unwrap()
            .events_since
            .clone();

        let mut conflict_table: HashMap<PathBuf, (PathRename, Option<Event>)> = HashMap::new();
        for (new_path, old_path, event) in events_since.into_iter() {
            if let Some(old_path) = old_path {
                // Renamed from `old_path` to `new_path` and the event recorded has been hoisted to
                // `new_path`, insert the two separately. The relation from new paths to old paths
                // is one-to-many, so we may have encountered this before. If so, preserve any
                // events and add to the list of renames.
                conflict_table
                    .entry(old_path)
                    .and_modify(|(rename, _)| rename.add(new_path.clone()))
                    .or_insert((PathRename::One(new_path.clone()), None));

                if event.is_some() {
                    // This cannot have been inserted before. It is unique among all `new_path`s
                    // (as the keys in a hash table) and it cannot also be an `old_path` because
                    // that would imply we have a path which was renamed from something and then
                    // renamed *to* something and that we've observed the rename on the interim
                    // path, which is impossible, because renames are coalesced. **I think.**
                    let old_entry = conflict_table.insert(new_path, (PathRename::None, event));
                    debug_assert!(
                        old_entry.is_none(),
                        "failed to anticipate all scenarios for conflict table insertion"
                    );
                }
            } else if event.is_some() {
                // No rename, insert as-is (could have seen this path if it were renamed and then
                // recreated at the original location)
                conflict_table
                    .entry(new_path)
                    .and_modify(|(_, old_event)| {
                        // We should never have an old event because we'll only insert an event if
                        // this was a real `new_path`, and it can only be that once, which is right
                        // here
                        debug_assert!(
                            old_event.is_none(),
                            "old event not none in conflict table creation"
                        );
                        *old_event = event.clone();
                    })
                    .or_insert((PathRename::None, event));
            }
        }

        let new_writes = writes
            .into_iter()
            .filter_map(|mut write| {
                // NOTE: Written as a loop for convenience, but this will never be executed more
                // than twice due to rename coalescence
                let write_opt = loop {
                    if let Some((rename, event)) = conflict_table.get(&write.path) {
                        match rename {
                            PathRename::None => {
                                break match event {
                                    // Path has been modified, we have a conflict (but for
                                    // filesystem updates, they're not strictly necessary, we can
                                    // just drop them)
                                    Some(Event::Create(_)) | Some(Event::Modify(_)) => {
                                        match write.source {
                                            WriteSource::Filesystem => None,
                                            WriteSource::Other => Some(Write {
                                                path: write.path,
                                                contents: write.contents,
                                                source: write.source,
                                                conflict: Conflict::Simple,
                                            }),
                                        }
                                    }
                                    // Path has been deleted, drop the write
                                    Some(Event::Delete(_)) => None,
                                    // Renames handled separately from debouncing
                                    Some(Event::Rename(_, _)) => unreachable!(),

                                    // No event, write is fine as-is (this shouldn't happen)
                                    None => Some(write),
                                };
                            }
                            // Try again with the new path (essentially moving this write)
                            PathRename::One(rename_target) => write.path = rename_target.clone(),
                            // Instant conflict
                            PathRename::Many(paths) => {
                                break Some(Write {
                                    path: write.path,
                                    contents: write.contents,
                                    source: write.source,
                                    conflict: Conflict::Multi(paths.clone()),
                                })
                            }
                        }
                    } else {
                        break Some(write);
                    }
                };
                if let Some(write) = write_opt {
                    match write.source {
                        WriteSource::Other => {
                            // We have an out-of-band write that's about to go through; record that
                            // it is on every patch so we can filter out filesystem writes to this
                            // path
                            self.patch_table.values_mut().for_each(|patch| {
                                patch.other_writes.insert(write.path.clone());
                            });

                            Some(write)
                        }
                        WriteSource::Filesystem => {
                            // Final check: we don't want to override an out-of-band write that's
                            // occurred. We have to index every time to avoid holding two mutable
                            // borrows with the above branch.
                            if self
                                .patch_table
                                .get(&patch_idx)
                                .unwrap()
                                .other_writes
                                .contains(&write.path)
                            {
                                None
                            } else {
                                Some(write)
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect();

        // Decrement the reference count, if it falls to zero, remove
        let entry = self.patch_table.get_mut(&patch_idx).unwrap();
        entry.ref_count -= 1;
        if entry.ref_count <= 0 {
            self.patch_table.remove(&patch_idx);
        }

        new_writes
    }
    /// Adds a new patch to the conflict detector, returning the ID of the next patch, which it
    /// will depend on (i.e. it may conflict with any events that occur after its own) and whose
    /// reference count it will increment.
    ///
    /// This will increment the `next_patch` index and reset its reference count to zero,
    /// inheriting the old values. It will also record the events that are part of this patch as
    /// having happened on all patches in the table.
    pub fn add_patch(&mut self, events: DebouncedEvents) -> u32 {
        // Update all patches (which are inherently previous to this one, which is "next") with the
        // events that have occurred, which anything depending on them will want to know
        self.patch_table.values_mut().for_each(|patch| {
            patch.events_since.combine(&events);
        });
        // Only bother to create an entry in the table if we have some references to this new
        // patch, otherwise there's no point (and we'd never remove it from the table anyway)
        if self.next_ref_count > 0 {
            self.patch_table.insert(
                self.next_patch,
                PatchTableEntry {
                    ref_count: self.next_ref_count,
                    events_since: events,
                    other_writes: HashSet::new(),
                },
            );
        }

        // We will have a reference to the next patch already (even if we haven't recorded our
        // patch's details, it's still going to start processing)
        self.next_ref_count = 1;
        self.next_patch += 1;

        self.next_patch
    }
}

/// The possibilities for a single path to be renamed.
enum PathRename {
    /// The path has not been renamed.
    None,
    /// The path has been renamed a single time, and we can shift straight to the new path.
    One(PathBuf),
    /// The path has been renamed, and the old path has been recreated and renamed again to
    /// something *different*. This is an irresolvable conflict.
    Many(Vec<PathBuf>),
}
impl PathRename {
    /// Adds the given rename to this [`PathRename`].
    fn add(&mut self, path: PathBuf) {
        match self {
            Self::None => *self = Self::One(path),
            Self::One(curr) if *curr == path => {}
            Self::One(curr) => *self = Self::Many(vec![curr.clone(), path]),
            Self::Many(paths) => paths.push(path),
        }
    }
}

/// A single entry in a [`ConflictDetector`]'s patch table.
struct PatchTableEntry {
    /// The number of "things" that depend on this patch (they will also depend on all patches
    /// after this, but that is not reflected in their reference counts). This will be decremented
    /// as things complete, and, once it falls to zero, this will be removed from the patch table.
    ref_count: u32,
    /// All events in this patch and those since it, debounced in one block.
    events_since: DebouncedEvents,
    /// Writes made from out-of-band updates that depended on this patch or an earlier one that was
    /// still processing. Writes will only be added here while a patch is still processing so that,
    /// when it finishes and we need to check for conflicts in the writes produced by the
    /// filesystem processing, we can filter out any paths that it would have written to to just
    /// update a link title, but which an out-of-band update wanted to make a change to, as that
    /// update takes precedence.
    ///
    /// The paths written to by the writes resulting from out-of-band updates that have occurred
    /// since this patch. When such writes occur, they will be added to every patch in the patch
    /// table to ensure that, when a patch finishes processing and tries to detect conflicts
    /// between its own writes and the events that have occurred since it, it will see these writes
    /// and drop any conflicts (as writes to update a link title must give way to those which have
    /// performed a requested update).
    other_writes: HashSet<PathBuf>,
}

/// A write to the filesystem.
pub struct Write {
    /// The path we want to write to.
    pub path: PathBuf,
    /// The contents we want to write to it.
    pub contents: String,
    /// Where this write came from (determins precedence).
    pub source: WriteSource,
    /// The type of conflict on this write, if any.
    pub conflict: Conflict,
}

pub enum WriteSource {
    /// This write came after processing a patch from the filesystem. These writes are entirely
    /// secondary, and only contain minor changes to things like link titles. If they conflict with
    /// an out-of-band write, the out-of-band write will take precedence.
    ///
    /// Though these writes will also occur to stabilise UUIDs on new nodes, this will only occur
    /// when an event has created such a node, which is a modification, and these events will
    /// always cause a conflict with an out-of-band write first. Hence, we don't worry about them
    /// here.
    Filesystem,
    /// This write came from some other source outside the filesystem. If this conflicts with an
    /// event *from* the filesystem, that takes precedence, but if it conflicts with a write *to*
    /// the filesystem, as above, this takes precedence. Conflicts between these kinds of writes
    /// are "resolved" by applying them directly in order.
    Other,
}

/// Types of conflicts that can occur on a write.
pub enum Conflict {
    /// No conflict.
    None,
    /// This path has been modified on the filesystem as well, we should compare what's on-disk
    /// with whatever we have.
    Simple,
    /// This path was renamed to multiple other paths, and we don't know where to go. This is an
    /// irresolvable conflict.
    Multi(Vec<PathBuf>),
}
