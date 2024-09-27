use crate::debouncer::{DebouncedEvents, Event};
use futures::{
    future::{join, join_all},
    Future,
};
use std::path::{Path, PathBuf};
use tracing::debug;

/// A patch to a graph which has resolved all I/O-bound operations.
///
/// This is designed to consume a [`DebouncedEvents`] object, and cannot be extended continually
/// from one. This asynchronous work should be done in a single batch when ready.
#[derive(Debug)]
pub struct GraphPatch {
    /// A list of paths in the domain which have been renamed from the first element of the tuple
    /// to the second.
    ///
    /// **Important:** these may not necessarily exist. If a path is created and then deleted, and
    /// renamed in between, this will still record that rename! Similarly, if a path is created,
    /// and then renamed, the creation event returned will be on the new path, but the rename will
    /// still exist! In the former case, a deletion will be recorded on the path for clarity.
    ///
    /// As with a set of [`DebouncedEvents`], these must be processed first.
    pub renames: Vec<(PathBuf, PathBuf)>,
    /// A list of paths in the domain which have been deleted.
    pub deletions: Vec<PathBuf>,
    /// A list of [`PathPatch`]es to paths which have been created in the domain.
    pub creations: Vec<PathPatch>,
    /// A list of [`PathPatch`]es to paths which have been modified in the domain.
    pub modifications: Vec<PathPatch>,
}
impl GraphPatch {
    /// Resolves the given debounced events into a series of patches to a graph which can be
    /// applied as a CPU-bound task. In essence, this does all the I/O that might be needed.
    ///
    /// Any errors in reading from a particular path will be stored as errors in the patch output.
    pub async fn from_events(events: DebouncedEvents, dir: &Path) -> Self {
        let mut creations_futs = Vec::new();
        let mut modifications_futs = Vec::new();
        let mut renames = Vec::new();
        let mut deletions = Vec::new();
        for (new_path, old_path, event) in events.into_iter() {
            // If there's an old path, we have a rename
            if let Some(old_path) = old_path {
                renames.push((old_path, new_path.clone()));
            }

            // If we have an event, push it, using the new path (renames will be actioned first by
            // the caller)
            if let Some(event) = event {
                match event {
                    Event::Delete(_) => deletions.push(new_path),
                    Event::Create(_) => {
                        if let Some(patch_fut) = PathPatch::new(new_path, dir) {
                            creations_futs.push(patch_fut);
                        }
                    }
                    Event::Modify(_) => {
                        if let Some(patch_fut) = PathPatch::new(new_path, dir) {
                            modifications_futs.push(patch_fut);
                        }
                    }
                    Event::Rename(_, _) => unreachable!(),
                }
            }
        }
        let (creations, modifications) =
            join(join_all(creations_futs), join_all(modifications_futs)).await;

        Self {
            renames,
            deletions,
            creations,
            modifications,
        }
    }
}

/// An I/O-resolved patch for a single path. From this, the necessary changes can be made to a
/// graph. Loading many of these into memory is no different than holding a graph in memory (i.e.
/// no additional details are held).
pub struct PathPatch {
    /// The path the patch is for.
    pub path: PathBuf,
    /// The result of trying to read the contents of that path as a string (which should be
    /// possible for Org/Markdown files).
    pub contents_res: Result<String, std::io::Error>,
}
impl PathPatch {
    /// Creates a new [`PathPatch`] from the given path. This is entirely self-contained, and, if
    /// many patches need to be constructed, they should be done in parallel. All this does is read
    /// files. This takes paths in the context of the directory being watched, so it also needs to
    /// know where that is.
    ///
    /// This will return [`None`] if the path doesn't need a patch constructed from it (i.e. if it
    /// isn't one of the types of files we track, or if it isn't a file at all).
    #[tracing::instrument]
    pub fn new(path: PathBuf, dir: &Path) -> Option<impl Future<Output = PathPatch>> {
        // We are the only tikme this path is *actually* used for the filesystem!
        let full_path = dir.join(&path);
        let ext = full_path.extension().unwrap_or_default();
        if (ext == "org" || ext == "md" || ext == "markdown") && full_path.is_file() {
            Some(async move {
                // Read the contents
                let contents_res = tokio::fs::read_to_string(&full_path).await;
                PathPatch { path, contents_res }
            })
        } else {
            debug!("denied path patch creation for {:?}", full_path);
            None
        }
    }
}
// When debugging this, don't print the whole contents
impl std::fmt::Debug for PathPatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathPatch")
            .field("path", &self.path)
            .field(
                "contents_res",
                &self.contents_res.as_ref().map(|_| "[contents]"),
            )
            .finish()
    }
}
