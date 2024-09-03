use crate::debouncer::{DebouncedEvents, Event};
use futures::{
    future::{join, join_all},
    Future,
};
use std::path::PathBuf;

/// A patch to a graph which has resolved all I/O-bound operations.
///
/// This is designed to consume a [`DebouncedEvents`] object, and cannot be extended continually
/// from one. This asynchronous work should be done in a single batch when ready.
pub struct GraphPatch {
    /// A list of paths in the domain which have been renamed from the first element of the tuple
    /// to the second.
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
    pub async fn from_events(events: DebouncedEvents) -> Self {
        let mut creations_futs = Vec::new();
        let mut modifications_futs = Vec::new();
        let mut renames = Vec::new();
        let mut deletions = Vec::new();
        for event in events.into_vec() {
            match event {
                Event::Rename(from, to) => renames.push((from, to)),
                Event::Delete(path) => deletions.push(path),
                Event::Create(path) => {
                    if let Some(patch_fut) = PathPatch::new(path) {
                        creations_futs.push(patch_fut);
                    }
                }
                Event::Modify(path) => {
                    if let Some(patch_fut) = PathPatch::new(path) {
                        modifications_futs.push(patch_fut);
                    }
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
    /// files.
    ///
    /// This will return [`None`] if the path doesn't need a patch constructed from it (i.e. if it
    /// isn't one of the types of files we track).
    pub fn new(path: PathBuf) -> Option<impl Future<Output = PathPatch>> {
        let ext = path.extension().unwrap_or_default();
        if ext == "org" || ext == "md" || ext == "markdown" {
            Some(async move {
                // Read the contents
                let contents_res = tokio::fs::read_to_string(&path).await;
                PathPatch { path, contents_res }
            })
        } else {
            None
        }
    }
}
