use crate::{
    connection::{BackConnection, ConnectionTarget},
    debouncer::{DebouncedEvents, Event},
    error::{GraphSetupError, PathError, VertexParseError},
    vertex::Vertex,
};
use dashmap::DashMap;
use futures::future::{join, join3, join_all};
use orgish::Format;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use tokio::sync::RwLock;
use uuid::Uuid;
use walkdir::WalkDir;

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
    ///
    /// Values in this map are held under an asynchronous [`RwLock`].
    vertices: DashMap<Uuid, RwLock<Vertex>>,
    /// A map of paths to the vertices that come from them and any errors associated in parsing
    /// them. Each vertex will appear exactly once in this map. At any point, it is guaranteed that
    /// a reference to this path will contain vertex IDs that exist in `self.vertices`.
    ///
    /// Values in this map are held under an asynchronous [`RwLock`].
    paths: DashMap<PathBuf, RwLock<PathData>>,

    /// A map of invalid vertex links, indexed by the invalid UUID and containing a list of the IDs
    /// of the vertices which made this invalid link. It is guaranteed that all values in here will
    /// be valid vertex IDs, but it is not guaranteed that all invalid vertex IDs will be keys in
    /// this map (there will be a delay).
    invalid_vertex_links: DashMap<Uuid, HashSet<Uuid>>,
}
impl Graph {
    /// Creates a new graph, reading vertices and resources from the given directory.
    pub async fn new(domain: &Path) -> Result<Self, GraphSetupError> {
        if !domain.is_dir() {
            return Err(GraphSetupError::DomainNotDir {
                path: domain.to_path_buf(),
            });
        }

        let this = Self {
            vertices: DashMap::new(),
            paths: DashMap::new(),
            invalid_vertex_links: DashMap::new(),
        };

        // Read all files in the directory recursively, skipping any paths we can't read
        let mut futs = Vec::new();
        for entry in WalkDir::new(domain)
            .into_iter()
            .filter_map(|entry| entry.ok())
        {
            if entry.file_type().is_file() {
                let this_ref = &this;
                futs.push(async move {
                    this_ref.parse_new_path(entry.path()).await;
                });
            }
        }
        join_all(futs).await;

        // Check all the links in every vertex (there will be much overlap here, but we need to be
        // able to generate accurate errors)
        let mut futs = Vec::new();
        for entry in this.vertices.iter() {
            let this_ref = &this;
            futs.push(async move {
                this_ref.check_vertex_and_back_connect(entry.key()).await;
            });
        }
        join_all(futs).await;

        Ok(this)
    }
    /// Gets all the errors associated with the given path and the vertices in it. This can be used
    /// to display diagnostics to a user, rather than reporting errors when they occur and crashing
    /// everything.
    pub async fn errors_for_path(&self, path: &Path) -> Option<Vec<PathError>> {
        let path = self.paths.get(path)?;
        let path = path.read().await;
        let mut errors = Vec::new();
        if let Some(err) = &path.error {
            errors.push(PathError::ParseError(err.to_string()));
        }

        // The root vertex inherits all connections from its children, so we only need to examine
        // that (prevents duplicates). There is always a root vertex for each path, and the
        // information each path holds on vertex IDs is guaranteed up-to-date, so this will work.
        let root_vertex = self.vertices.get(&path.ids[0]).unwrap();
        let root_vertex = root_vertex.read().await;
        for conn in root_vertex.connections_out() {
            if let ConnectionTarget::InvalidVertex(target_id) = &conn.target {
                errors.push(PathError::InvalidConnection(*target_id))
            }
        }

        Some(errors)
    }

    /// Handles a series of batched filesystem updates that impact the graph.
    pub async fn handle_updates(&self, updates: Vec<Event>) {
        // Collapse all the updates into a series of independent ones
        let updates = DebouncedEvents::from_sequential(updates);
        // Renames have no real impact on the validity of the graph because paths are considered
        // superficial details, we just need to re-insert some things into the map, so we can
        // handle each one individually. Because we collapse sequential renames of the same file
        // into one operation, the order in which these occur doesn't matter.
        let rename_futs = updates
            .renames
            .into_iter()
            .map(|(from, to)| self.handle_rename(from, to));
        join_all(rename_futs).await;

        let creation_futs = updates
            .creations
            .into_iter()
            .map(|path| self.handle_creation(path));
        let deletion_futs = updates
            .deletions
            .into_iter()
            .map(|path| self.handle_deletion(path));
        let modification_futs = updates
            .modifications
            .into_iter()
            .map(|path| self.handle_modification(path));

        // We can run creation, deletion, and modification operations all simultaneously (their
        // locks might interfere with each other, but they can work this out on their own)
        join3(
            join_all(creation_futs),
            join_all(deletion_futs),
            join_all(modification_futs),
        )
        .await;
    }
    /// Handles the renaming of a single path in the graph's domain. A rename has no imapct on the
    /// graph's overall validity, as paths are considered superficial details, so multiple renames
    /// can be handled in any order without losing performance.
    async fn handle_rename(&self, from: PathBuf, to: PathBuf) {
        // Remove and re-insert the vertex IDs associated with this path in the paths map
        if let Some((_, path_data)) = self.paths.remove(&from) {
            // We've extracted this from the map, which means no-one else had a reference to it,
            // therefore we're the only holder, so there can't be any contention
            let path_data_ref = path_data.try_read().unwrap();
            for id in path_data_ref.ids.iter() {
                // We keep the vertices attached to paths updated, so this is guaranteed to exist
                let vertex = self.vertices.get(id).unwrap();
                // But anyone could be trying to access this vertex, and we need to change its path
                let mut vertex = vertex.write().await;
                vertex.set_path(to.clone());
            }
            drop(path_data_ref);
            // Note that any errors associated with a path carry over if there's been a rename
            // (which doesn't involve a modification)
            self.paths.insert(to, path_data);
        }
    }
    /// Handles the creation of a new path in the graph's domain. This path will be parsed as a
    /// vertex and validated.
    ///
    /// Note that a new path cannot invalidate any existing vertices, but it can itself be invalid.
    /// If this is the case, a new entry will be created for this path, and it will be associated
    /// only with errors.
    async fn handle_creation(&self, path: PathBuf) {
        // This does a sanity check that the path is not already in the graph
        let new_ids = self.parse_new_path(&path).await;

        // Having created some new vertices, there could be invalid vertices in other vertices that
        // are now valid; these are globally indexed by the invalid UUIDs, and we have a vector of
        // newly valid ones! At the same time, we can check the new vertices' validity in their own
        // links.
        //
        // Checking one vertex cannot interfere with checking another, and the locks prevent race
        // conditions, so this is perfectly safe to do all in parallel.
        let mut new_checking_futs = Vec::new();
        let mut old_checking_futs = Vec::new();
        for id in new_ids.iter() {
            new_checking_futs.push(self.check_vertex_and_back_connect(id));

            // The `invalid_vertex_links` map is self-contained, so removing something from here
            // doesn't require changing anything else
            if let Some((_, refers)) = self.invalid_vertex_links.remove(&id) {
                for referring_id in refers {
                    old_checking_futs.push(async move {
                        self.check_vertex_and_back_connect(&referring_id).await
                    });
                }
            }
        }
        join(join_all(new_checking_futs), join_all(old_checking_futs)).await;
    }
    /// Handles the deletion of an existing path in the graph's domain. All vertices associated
    /// with this path will be removed entirely, which will cause invalid link errors on any
    /// vertices that link to those ones. These will be handled by those invalid connections being
    /// removed and errors being added on those paths.
    async fn handle_deletion(&self, path: PathBuf) {
        assert!(
            self.paths.contains_key(&path),
            "tried to delete path not in graph"
        );

        // We have to be careful to delete the vertices and references to them while keeping
        // everything in a valid state; hence we have to get the path data and then go through the
        // vertices before we can remove the path at the end (in the interim, we'll drain the IDs
        // it points to as we delete them, release that, and then delete the entry entirely)
        let path_data = self.paths.get(&path).unwrap();
        let mut path_data = path_data.write().await;

        for vertex_id in path_data.ids.drain(..) {
            let vertex = self.vertices.get(&vertex_id).unwrap();
            let mut vertex = vertex.write().await;

            // Start by going through its connections and removing both backlinks and invalid link
            // references from `self.invalid_vertex_links`
            for conn in vertex.connections_out_mut() {
                match &conn.target {
                    ConnectionTarget::Vertex(target_id) => {
                        // By the guarantees of this very function, a vertex won't be deleted until
                        // there are no extant references, so this has to work
                        let target_vertex = self.vertices.get(&target_id).unwrap();
                        let mut target_vertex = target_vertex.write().await;
                        target_vertex.remove_back_connection(vertex_id);
                    }
                    ConnectionTarget::InvalidVertex(target_id) => {
                        // We have to remove the invalid link reference from the global map
                        let mut refers = self.invalid_vertex_links.get_mut(target_id).unwrap();
                        refers.remove(&vertex_id);
                        if refers.is_empty() {
                            self.invalid_vertex_links.remove(target_id);
                        }
                    }

                    ConnectionTarget::Resource(_) => todo!(),
                    ConnectionTarget::Unknown(_) => todo!(),
                }
            }
        }

        // Remove the path and use that to remove all its vertices too
        let (_, path_data) = self.paths.remove(&path).unwrap();
        let path_data = path_data.into_inner();
        // We'll be able to invalidate connections from other vertices to the ones we're removing
        // all in parallel (they're the most likely to be being accessed at the same time)
        let mut invalidate_futs = Vec::new();
        let mut remove_back_connections_futs = Vec::new();
        for vertex_id in &path_data.ids {
            let (_, vertex) = self.vertices.remove(&vertex_id).unwrap();
            let vertex = vertex.into_inner();

            // Process all the connections this vertex has to other entries in the graph and handle
            // them (particualrly removing back-connections from the connectees)
            for connection in vertex.connections_out() {
                match connection.target {
                    ConnectionTarget::Vertex(target_id) => {
                        if path_data.ids.contains(&target_id) {
                            continue;
                        }

                        // It's possible that another deletion is simultaneously happening, we
                        // shouldn't assume information from vertex connections is completely
                        // accurate
                        if let Some(target) = self.vertices.get(&target_id) {
                            remove_back_connections_futs.push(async move {
                                let mut target = target.write().await;
                                target.remove_back_connection(*vertex_id);
                            });
                        }
                    }
                    // We don't need to do anything for invalid connections, they don't matter once
                    // we've deleted this
                    ConnectionTarget::InvalidVertex(_) => {}

                    ConnectionTarget::Resource(_) => todo!(),
                    ConnectionTarget::Unknown(_) => todo!(),
                }
            }

            // Now do the reverse: from the back-connections *on* this vertex, identify all the
            // ones that connect *to* it and invalidate those connections on them. This works fine
            // with nested child vertices because we'll just invalidate the connection on all of
            // them, keeping the structure correct in effect "accidentally".
            for connector_id in vertex.connections_in().map(|c| c.uuid) {
                // It's possible that another deletion is simultaneously happening, we shouldn't
                // assume information from vertex connections is completely accurate
                if let Some(connector) = self.vertices.get(&connector_id) {
                    invalidate_futs.push(async move {
                        let mut connector = connector.write().await;
                        connector.invalidate_connection(*vertex_id);
                    });
                }
            }
        }
        // Invalidate all the connections to the vertices we're deleting from others at once
        join_all(invalidate_futs).await;
        join_all(remove_back_connections_futs).await;
    }
    /// Handles the modification of an existing path in the graph's domain. The vertex will be
    /// re-parsed, and any changes to the connections therein will be validated.
    async fn handle_modification(&self, path: PathBuf) {
        // TODO:
    }

    /// Internal helper function for parsing a new path as a vertex or resource and adding it to
    /// the internal maps, handling errors. This will return a list of vertices requiring
    /// validation.
    async fn parse_new_path(&self, path: &Path) -> Vec<Uuid> {
        assert!(
            !self.paths.contains_key(path),
            "attempted to parse as a new path a path already in the graph"
        );

        // If we have a known extension, handle it as a set of vertices
        let ext = path.extension().unwrap_or_default();
        if ext == "org" || ext == "md" || ext == "markdown" {
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
                Ok(vertices_in_file) => {
                    let mut ids = Vec::new();
                    // There are no references to these vertices yet, so inserting them doesn't
                    // require changing anything else (also, all their links are marked as invalid,
                    // so they don't even validly reference anything else). The only thing we have
                    // to bend here is that not all invalid links are in
                    // `self.invalid_vertex_links` at all times.
                    for vertex in vertices_in_file {
                        ids.push(vertex.id());
                        self.vertices.insert(vertex.id(), RwLock::new(vertex));
                    }
                    // By inserting the path, there are now references to these vertices, and the
                    // vertices themselves now become valid (because their path is defined)
                    self.paths.insert(
                        path.to_path_buf(),
                        RwLock::new(PathData {
                            ids: ids.clone(),
                            error: None,
                        }),
                    );

                    ids
                }
                Err(err) => {
                    // Parsing failed, note the error as associated with this path. None of
                    // the vertices have been added, so some connections from other
                    // vertices might fail, but those will be resolved once this one starts
                    // working again.
                    self.paths.insert(
                        path.to_path_buf(),
                        RwLock::new(PathData {
                            ids: Vec::new(),
                            error: Some(err),
                        }),
                    );

                    // We have no new vertices, hence none whose connections need to be checked
                    Vec::new()
                }
            }
        } else {
            // We have a resource
            // TODO:
            Vec::new()
        }
    }
    /// Checks the vertex with the given ID (which must exist) and validates all connections from
    /// it to other vertices, constructing back-connections in those target vertices, which refer
    /// back to this vertex.
    ///
    /// This will also add errors to the path associated with this vertex if any of its connections
    /// are invalid.
    async fn check_vertex_and_back_connect(&self, id: &Uuid) {
        assert!(
            self.vertices.contains_key(id),
            "attempted to check a vertex and back-connect that doesn't exist"
        );

        // We just checked that this exists
        let vertex = self.vertices.get(id).unwrap();
        let mut vertex = vertex.write().await;
        for conn in vertex.connections_out_mut() {
            match &conn.target {
                ConnectionTarget::InvalidVertex(target_id) => {
                    if target_id == id {
                        // Self-references are simply ignored
                        continue;
                    } else if let Some(target_vertex) = self.vertices.get(target_id) {
                        // Update the invalid link so it's now valid (this will only happen once we
                        // release the lock)
                        conn.target = ConnectionTarget::Vertex(*target_id);
                        let mut target_vertex = target_vertex.write().await;
                        target_vertex.add_back_connection(BackConnection {
                            uuid: *id,
                            ty: conn.ty.clone(),
                        });
                    } else {
                        // Invalid connections that aren't self-references should be noted
                        // globally. This is conveniently stored in another map, which prevents
                        // deadlocks from holding the references we do.
                        self.invalid_vertex_links
                            .entry(*target_id)
                            .or_insert(HashSet::new())
                            .insert(*id);
                    }
                }
                // TODO:
                ConnectionTarget::Resource(_) => todo!(),
                ConnectionTarget::Unknown(_) => todo!(),

                // We don't need to do anything for vertices that are already known to be valid
                ConnectionTarget::Vertex(_) => {}
            }
        }
    }
}
