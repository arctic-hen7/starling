use crate::{
    connection::{BackConnection, ConnectionTarget},
    debouncer::DebouncedEvents,
    error::{GraphSetupError, PathError, VertexParseError},
    patch::{GraphPatch, PathPatch},
    vertex::Vertex,
};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use uuid::Uuid;

/// The data associated with a single path in the domain of a [`Graph`].
struct PathData {
    /// The unique identifiers of all vertices that come from this path.
    ids: Vec<Uuid>,
    /// An error in parsing this path, if one was encountered.
    error: Option<VertexParseError>,
}

/// A graph of composed of many vertices, each of which represents a single entry in a file with a
/// UUID.
///
/// Any operations changing the overall state of the graph should lock *all* affected entries,
/// update them all, and then mass-release them to ensure the graph is never in a partially-invalid
/// state.
pub struct Graph {
    /// All the vertices in the graph, indexed by their unique identifiers. At any given point, it
    /// is guaranteed that references to other vertices by their IDs will be valid, and that any
    /// vertices which have valid refences to them from other vertices will have valid paths in the
    /// paths map as well.
    vertices: HashMap<Uuid, Vertex>,
    /// A map of paths to the vertices that come from them and any errors associated in parsing
    /// them. Each vertex will appear exactly once in this map. At any point, it is guaranteed that
    /// a reference to this path will contain vertex IDs that exist in `self.vertices`.
    paths: HashMap<PathBuf, PathData>,

    /// A map of invalid vertex links, indexed by the invalid UUID and containing a list of the IDs
    /// of the vertices which made this invalid link. It is guaranteed that all values in here will
    /// be valid vertex IDs, and that all invalid vertex links will be present in here.
    invalid_vertex_links: HashMap<Uuid, HashSet<Uuid>>,
}
impl Graph {
    /// Creates a new graph, reading vertices and resources from the given directory.
    pub async fn new(domain: &Path) -> Result<Self, GraphSetupError> {
        if !domain.is_dir() {
            return Err(GraphSetupError::DomainNotDir {
                path: domain.to_path_buf(),
            });
        }

        let mut this = Self {
            vertices: HashMap::new(),
            paths: HashMap::new(),
            invalid_vertex_links: HashMap::new(),
        };

        // Create a series of "debounced" exclusively creation events from this directory and then
        // process that into a series of creation patches
        let creation_events = DebouncedEvents::start_from_dir(domain);
        let patch = GraphPatch::from_events(creation_events).await;
        this.handle_patch(patch);

        Ok(this)
    }
    /// Gets all the errors associated with the given path and the vertices in it. This can be used
    /// to display diagnostics to a user, rather than reporting errors when they occur and crashing
    /// everything.
    ///
    /// This will return [`None`] if the given path is not in the graph.
    pub async fn errors_for_path(&self, path: &Path) -> Option<Vec<PathError>> {
        let path = self.paths.get(path)?;
        let mut errors = Vec::new();
        if let Some(err) = &path.error {
            errors.push(PathError::ParseError(err.to_string()));
        }

        // The root vertex inherits all connections from its children, so we only need to examine
        // that (prevents duplicates). There is always a root vertex for each path, and the
        // information each path holds on vertex IDs is guaranteed up-to-date, so this will work.
        let root_vertex = self.vertices.get(&path.ids[0]).unwrap();
        for conn in root_vertex.connections_out() {
            if let ConnectionTarget::InvalidVertex(target_id) = &conn.target {
                errors.push(PathError::InvalidConnection(*target_id))
            }
        }

        Some(errors)
    }

    /// Handles the given [`GraphPatch`] and applies it to the graph. Conveniently, all I/O-bound
    /// work is done for us in the construction of a patch, so, once a mutable reference to the
    /// [`Graph`] can be obtained, this can run synchronously with no locking.
    pub fn handle_patch(&mut self, patch: GraphPatch) {
        // Renames are handled atomically, because they don't need any checking or I/O
        for (from, to) in patch.renames {
            self.handle_rename(from, to);
        }
        // The others are handled together because they involve checking links, which is best done
        // once you have all information (i.e. all vertex changes)
        self.handle_creations(patch.creations);
        self.handle_deletions(patch.deletions);
        self.handle_modifications(patch.modifications);
    }

    /// Handles the renaming of a single path in the graph's domain. A rename has no imapct on the
    /// graph's overall validity, as paths are considered superficial details, so multiple renames
    /// can be handled in any order without losing performance.
    ///
    /// If called with a path that isn't in the graph, this will panic.
    fn handle_rename(&mut self, from: PathBuf, to: PathBuf) {
        assert!(
            self.paths.contains_key(&from),
            "tried to rename path not in graph"
        );

        // Remove and re-insert the vertex IDs associated with this path in the paths map
        let path_data = self.paths.remove(&from).unwrap();
        for id in path_data.ids.iter() {
            // We keep the vertices attached to paths updated, so this is guaranteed to exist
            let vertex = self.vertices.get_mut(id).unwrap();
            vertex.set_path(to.clone());
        }
        // Note that any errors associated with a path carry over if there's been a rename
        // (which doesn't involve a modification)
        self.paths.insert(to, path_data);
    }
    /// Handles the creation of a series of new paths in the graph's domain. We parse multiple
    /// creations at once to allow batching checking them all, which ensures we don't pointlessly
    /// check links more than needed.
    fn handle_creations(&mut self, creations: Vec<PathPatch>) {
        // First add everything to the graph, recording all the vertex IDs needing to have their
        // links checked
        let mut new_ids = Vec::new();
        for patch in creations {
            match patch {
                PathPatch::VertexOk { path, vertices } => {
                    assert!(
                        !self.paths.contains_key(&path),
                        "tried to create path already in graph"
                    );

                    let mut ids = Vec::new();
                    for vertex in vertices {
                        ids.push(vertex.id());
                        new_ids.push(vertex.id());
                        self.vertices.insert(vertex.id(), vertex);
                    }
                    self.paths.insert(
                        path,
                        PathData {
                            ids: ids.clone(),
                            error: None,
                        },
                    );
                }
                PathPatch::VertexErr { path, err } => {
                    assert!(
                        !self.paths.contains_key(&path),
                        "tried to create path already in graph"
                    );

                    self.paths.insert(
                        path,
                        PathData {
                            ids: Vec::new(),
                            error: Some(err),
                        },
                    );
                }

                // TODO:
                PathPatch::Resource { .. } => {}
            };
        }

        // Having created some new vertices, there could be invalid vertices in other vertices that
        // are now valid; these are globally indexed by the invalid UUIDs, and we have a vector of
        // newly valid ones! At the same time, we can check the new vertices' validity in their own
        // links.
        for id in new_ids {
            self.check_vertex_and_back_connect(&id);

            // See if this new ID is something that has previously been referred to by other
            // vertices and considered an invalid link
            if let Some(referrers) = self.invalid_vertex_links.remove(&id) {
                for referring_id in referrers {
                    // This will ignore correct vertices, and just analyse incorrect ones, which is
                    // perfect for our purposes (extra work is minimal)
                    self.check_vertex_and_back_connect(&referring_id);
                }
            }
        }
    }
    /// Handles the deletion of an existing path in the graph's domain. All vertices associated
    /// with this path will be removed entirely, which will cause invalid link errors on any
    /// vertices that link to those ones. These will be handled by those invalid connections being
    /// removed and errors being added on those paths.
    fn handle_deletions(&mut self, paths: Vec<PathBuf>) {
        // Remove all the target paths and their vertices, collecting them all for link processing
        let mut condemned_vertices = Vec::new();
        for path in paths {
            // If a path to be deleted doesn't exist, that's not a real problem, so we can ignore
            // it (but panic in debug builds, because it certainly shouldn't happen)
            debug_assert!(
                self.paths.contains_key(&path),
                "tried to delete path not in graph"
            );
            if let Some(path_data) = self.paths.remove(&path) {
                for vertex_id in path_data.ids {
                    // These IDs are all guaranteed to exist
                    condemned_vertices.push(self.vertices.remove(&vertex_id).unwrap());
                }
            }
        }

        // Go through all those vertices and remove them, handling their links
        for vertex in condemned_vertices {
            self.delete_vertex_links(vertex);
        }
    }
    /// Handles the modification of an existing path in the graph's domain. The vertex will be
    /// re-parsed, and any changes to the connections therein will be validated.
    fn handle_modifications(&mut self, modifications: Vec<PathPatch>) {
        let mut ids_to_check = Vec::new();
        for modification in modifications {
            match modification {
                PathPatch::VertexOk { path, vertices } => {
                    assert!(
                        self.paths.contains_key(&path),
                        "tried to modify path not in graph"
                    );

                    // Put the new vertices into a map by their IDs and a set of their IDs for
                    // efficient comparisons
                    let new_ids = vertices.iter().map(|v| v.id()).collect::<HashSet<_>>();
                    let mut vertices = vertices
                        .into_iter()
                        .map(|v| (v.id(), v))
                        .collect::<HashMap<_, _>>();

                    // And put the existing IDs in a set too
                    let path_data = self.paths.get_mut(&path).unwrap();
                    let existing_ids = path_data.ids.iter().cloned().collect::<HashSet<_>>();

                    // Add any that are in the new list but not the old (we'll validate and
                    // back-connect them en-masse later)
                    for new_id in new_ids.difference(&existing_ids) {
                        self.vertices
                            .insert(*new_id, vertices.remove(new_id).unwrap());
                        ids_to_check.push(*new_id);
                    }
                    // Compare connections and update superficial properties for vertices that are
                    // in both lists (and so might have been updated)
                    for possibly_modified_id in new_ids.intersection(&existing_ids) {
                        let vertex = self.vertices.get_mut(possibly_modified_id).unwrap();
                        // Updating gives a "shadow vertex" with only the connections that were
                        // removed --- we can use regular vertex link deletion logic on it to prune
                        // both deleted valid connections (i.e. stale back-connections) and deleted
                        // invalid connection (i.e. outdated references in
                        // `self.invalid_vertex_links`). Note that this is guaranteed to have no
                        // inbound connections, so we don't need to worry about that side of the
                        // deletion processing.
                        let shadow_vertex =
                            vertex.update(vertices.remove(possibly_modified_id).unwrap());
                        self.delete_vertex_links(shadow_vertex);

                        // Any new connections will need validation
                        ids_to_check.push(*possibly_modified_id);
                    }
                    // Delete any that are in the old list but not the new (do this last so we
                    // don't disrupt the new vertices' links)
                    for deleted_id in existing_ids.difference(&new_ids) {
                        let vertex = self.vertices.remove(deleted_id).unwrap();
                        self.delete_vertex_links(vertex);
                    }
                }
                PathPatch::VertexErr { path, err } => {
                    assert!(
                        self.paths.contains_key(&path),
                        "tried to modify path not in graph"
                    );
                    // Replace any existing errors on this path with the new one, leaving any
                    // vertices intact as the last good state
                    let path_data = self.paths.get_mut(&path).unwrap();
                    path_data.error = Some(err);
                }

                // TODO:
                PathPatch::Resource { .. } => {}
            }
        }

        // All new and modified vertices are up-to-date in the graph, now check their links and
        // back-connect
        for id in ids_to_check {
            self.check_vertex_and_back_connect(&id);
        }
    }

    /// Deletes the links of this vertex, restoring the graph to a valid state after it's removal.
    /// This will *not* attempt to remove this vertex from its path's list of vertices.
    fn delete_vertex_links(&mut self, vertex: Vertex) {
        // Process all the connections this vertex has to other entries in the graph and handle
        // them
        for connection in vertex.connections_out() {
            match connection.target {
                ConnectionTarget::Vertex(target_id) => {
                    // All vertex IDs still exist, except those which were just deleted, so we
                    // can safely ignore them (we only care about extant connections to the
                    // remaining vertices in the graph)
                    if let Some(target) = self.vertices.get_mut(&target_id) {
                        target.remove_back_connection(vertex.id());
                    }
                }
                // Invalid connections (apart from self-references) will be in the global map
                // of invalid links, so remove our ID from there
                ConnectionTarget::InvalidVertex(target_id) => {
                    // We have to remove the invalid link reference from the global map
                    if target_id == vertex.id() {
                        continue;
                    }

                    // Invalid links are guaranteed to be in the global map while there's still
                    // some connection to them
                    let refers = self.invalid_vertex_links.get_mut(&target_id).unwrap();
                    refers.remove(&vertex.id());
                    // If there are no more references to this invalid link, remove the entire
                    // entry
                    if refers.is_empty() {
                        self.invalid_vertex_links.remove(&target_id);
                    }
                }

                // TODO:
                ConnectionTarget::Resource(_) => todo!(),
                ConnectionTarget::Unknown(_) => todo!(),
            }
        }

        // Now do the reverse: from the back-connections *on* this vertex, identify all the
        // ones that connect *to* it and invalidate those connections on them. This works fine
        // with nested child vertices because we'll just invalidate the connection on all of
        // them, keeping the structure correct in effect "accidentally".
        for connector_id in vertex.connections_in().map(|c| c.uuid) {
            // Again, easily possible that the vertex connecting to this one is also in this
            // deletion batch, in which case ignore it
            if let Some(connector) = self.vertices.get_mut(&connector_id) {
                connector.invalidate_connection(vertex.id());
            }
        }
    }
    /// Checks the vertex with the given ID (which must exist) and validates all connections from
    /// it to other vertices, constructing back-connections in those target vertices, which refer
    /// back to this vertex. This will ignore any connections which have already been validated
    /// (even if they might have become invalid!).
    ///
    /// This will also record any invalid connections globally.
    fn check_vertex_and_back_connect(&mut self, id: &Uuid) {
        // Only in debug because the callers should check this; this is just to catch bugs
        debug_assert!(
            self.vertices.contains_key(id),
            "attempted to check and back-connect a vertex that doesn't exist"
        );

        // The caller guarantees this to exist; we remove it entirely so we don't have to hold
        // multiple mutable references (`HashMap`s in Rust use tombstones, so it's cheap to delete
        // and re-insert provided there are no insertions in between)
        let mut vertex = self.vertices.remove(id).unwrap();
        for conn in vertex.connections_out_mut() {
            match conn.target {
                ConnectionTarget::InvalidVertex(target_id) => {
                    if target_id == *id {
                        // Self-references are simply ignored
                        continue;
                    } else if let Some(target_vertex) = self.vertices.get_mut(&target_id) {
                        // Update the invalid link so it's now valid
                        conn.target = ConnectionTarget::Vertex(target_id);
                        target_vertex.add_back_connection(BackConnection { uuid: *id });
                    } else {
                        // Invalid connections that aren't self-references should be noted
                        // globally. This is conveniently stored in another map, which prevents
                        // deadlocks from holding the references we do.
                        self.invalid_vertex_links
                            .entry(target_id)
                            .or_insert(HashSet::new())
                            .insert(*id);
                    }
                }
                // We don't need to do anything for vertices that are already known to be valid
                ConnectionTarget::Vertex(_) => {}

                // TODO:
                ConnectionTarget::Resource(_) => todo!(),
                ConnectionTarget::Unknown(_) => todo!(),
            }
        }
        // Reinsert the vertex we removed (no insertions in between, so should be at same position,
        // minimal overhead)
        self.vertices.insert(vertex.id(), vertex);
    }
}
