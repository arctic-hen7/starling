use crate::{
    debouncer::{DebouncedEvents, Event},
    error::VertexParseError,
    vertex::Vertex,
};
use futures::future::{join, join_all};
use orgish::Format;
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
                Event::Create(path) => creations_futs.push(PathPatch::new(path)),
                Event::Modify(path) => modifications_futs.push(PathPatch::new(path)),
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
pub enum PathPatch {
    /// The path corresponds to a series of vertices, and we were able to read it and parse them.
    VertexOk {
        path: PathBuf,
        vertices: Vec<Vertex>,
    },
    /// The path corresponds to a series of vertices, but parsing failed, and we have a concrete
    /// error to associate with the path.
    VertexErr {
        path: PathBuf,
        err: VertexParseError,
    },
    /// The path corresponds to a resource.
    Resource {
        path: PathBuf,
        // TODO:
    },
}
impl PathPatch {
    /// Creates a new [`PathPatch`] from the given path. This is entirely self-contained, and, if
    /// many patches need to be constructed, they should be done in parallel.
    pub async fn new(path: PathBuf) -> Self {
        let ext = path.extension().unwrap_or_default();
        if ext == "org" || ext == "md" || ext == "markdown" {
            // We have something which should be a vertex, try parsing it
            let parse_res = Vertex::many_from_file(
                &path,
                if ext == "org" {
                    Format::Org
                } else {
                    Format::Markdown
                },
            )
            .await;
            match parse_res {
                Ok(vertices) => PathPatch::VertexOk { path, vertices },
                Err(err) => PathPatch::VertexErr { path, err },
            }
        } else {
            // We have another type of file, which we'll consider a resource
            PathPatch::Resource { path }
        }
    }
}
