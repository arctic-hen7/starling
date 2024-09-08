use crate::error::PathParseError;
use crate::{debouncer::DebouncedEvents, patch::GraphPatch, path_node::PathNode};
use futures::future::join;
use futures::future::join_all;
use futures::future::OptionFuture;
use orgish::Format;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use tokio::sync::RwLock;
use uuid::Uuid;

/// An update to be made to the graph.
pub enum GraphUpdate {
    /// The provided [`PathNode`] should be created and added to the graph. This does *not* include
    /// any instructions regarding its constituent nodes.
    CreatePathNode(PathNode),
    /// The [`PathNode`] with the provided path should be modified to be the new node.
    ModifyPathNode { path: PathBuf, new_node: PathNode },
    /// The [`PathNode`] with the provided path should be deleted from the graph.
    DeletePathNode(PathBuf),
    /// The provided node should be added to the graph. This does *not* include a connection
    /// validation instruction.
    ///
    /// As new nodes may have IDs that have been force-generated during parsing, any paths subject
    /// to one of these will be written to disk.
    AddNode { id: Uuid, path: PathBuf },
    /// The node with the given ID should be removed. This will correspond to a blatant deletion of
    /// the node from the map of all nodes (i.e. connections will *not* be handled, and separate
    /// [`GraphUpdate::RemoveBacklink`] instructions will probably be needed).
    RemoveNode(Uuid),
    /// We should remove the backlink on the node with the given ID from the node with the given
    /// ID. This will be because either there is no longer a connection to it, or because the
    /// source vertex has been removed.
    RemoveBacklink { on: Uuid, from: Uuid },
    /// We should remove a record of an invalid connection from the (valid) node with the given ID
    /// to the given (invalid) ID. If there are no references left to that invalid ID, we should
    /// drop it entirely (we only keep track of them to speed the process of validating previously
    /// existing connections to newly valid created nodes).
    RemoveInvalidConnection { from: Uuid, to: Uuid },
    /// We should make the connection to the given node, which was previously recorded in the
    /// global tracker as invalid, as valid. This will involve creating a backlink on every node
    /// that referenced it, and removing the entry from the global tracker.
    ///
    /// Any paths found to be referencing this previously invalid connection will be written to
    /// disk with updated connection titles (handled through the [`GraphUpdate::CheckConnection`]
    /// instructions this implicitly generates).
    ValidateInvalidConnection { to: Uuid },
    /// We should check if the connection from the node with the given ID to the node with the
    /// given ID is valid. If so, we should set it as valid and create an appropriate backlink, and
    /// if not, we should leave it as invalid and register it in a global map that accounts for
    /// invalid connections (so they can be easily rendered valid if a node with the ID they point
    /// to is created).
    ///
    /// Note that this is also used for rendering valid connections that were previously marked as
    /// invalid in the global tracker when a node with the (invalid) ID they pointed to is created
    /// (in which case we know this will succeed).
    ///
    /// If the connection is found to be valid, the path which made the connection will be written
    /// to disk with any updated connection titles.
    CheckConnection { from: Uuid, to: Uuid },
    /// We should set the connection on the node with the given ID which goes to the other node
    /// with the given ID to be invalid. This is used when the node to which the connection goes is
    /// being removed. The target connection should be added to the global map of invalid
    /// connections in case it's re-created.
    InvalidateConnection { on: Uuid, to: Uuid },
}

type NodeMap = HashMap<Uuid, PathBuf>;
type PathMap = HashMap<PathBuf, RwLock<PathNode>>;
type InvalidConnectionsMap = HashMap<Uuid, HashSet<Uuid>>;

/// A graph of many nodes derived from Org/Markdown files ([`PathNode`]s), which are connected
/// together.
pub struct Graph {
    /// A map of all the nodes in the graph to the paths containing them (which are guaranteed to
    /// exist and contain them).
    ///
    /// If maps are to be locked, this must always be locked first.
    pub(crate) nodes: RwLock<NodeMap>,
    /// All the paths in the graph, indexed by their (relative) paths. On a rename, an entry will
    /// be removed and recreated here. All the node IDs on a path are guaranteed to exist in the
    /// nodes map and point back to this path.
    ///
    /// If maps are to be locked, this must always be locked second. If individual paths are to be
    /// locked, they should be locked sorted in path order to prevent deadlocks.
    pub(crate) paths: RwLock<PathMap>,
    /// A list of invalid connections, indexed by the invalid ID they connected to, and listing in
    /// each entry the set of nodes which made such a connection, by their IDs.
    ///
    /// If maps are to be locked, this must always be locked third.
    pub(crate) invalid_connections: RwLock<InvalidConnectionsMap>,
}
impl Graph {
    /// Creates a new, completely empty graph. Typically, [`Self::from_dir`] would be used to
    /// initially populate the graph from a directory.
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
            invalid_connections: RwLock::new(HashMap::new()),
        }
    }
    /// Returns any errors associated with the given path. The return type here is a little
    /// strange: if the path couldn't be parsed, you'll get an `Err(PathParseError)` (stringified),
    /// but if it could be, you'll get an `Ok(_)` with a list of the IDs of all invalid connections
    /// made in the path. If the path doesn't exist at all, you'll get `None`.
    pub async fn errors(&self, path: &Path) -> Option<Result<Vec<Uuid>, String>> {
        let paths = self.paths.read().await;
        let path_node = paths.get(path)?.read().await;
        Some(
            path_node
                .document()
                .map(|doc| {
                    doc.root
                        .connections()
                        .filter(|conn| !conn.is_valid())
                        .map(|conn| conn.id())
                        .collect()
                })
                // If there's no document, an error is guaranteed
                .ok_or_else(|| path_node.error.as_ref().unwrap().to_string()),
        )
    }
    /// Creates a new graph, tracking all files in the given directory recursively. This will read
    /// every file that can be parsed and parse them all.
    ///
    /// # Panics
    ///
    /// This will panic if the provided path is not a valid directory.
    pub async fn from_dir(dir: PathBuf) -> Self {
        assert!(dir.is_dir());

        // Fake creation events recursively for everything in the directory
        let creations = DebouncedEvents::start_from_dir(&dir);
        let patch = GraphPatch::from_events(creations).await;

        let this = Self::new();
        this.process_fs_patch(patch).await;

        this
    }
    /// Process a batch of updates from the filesystem. This operates as the start of a pipeline,
    /// generating modifications which in turn generate instructions for locking and graph updates.
    /// This will acquire read locks on the paths map and some individual paths as necessary to
    /// generate updates, but it will not write anything directly (though it will call both
    /// [`Self::process_renames`] and [`Self::process_updates`]).
    ///
    /// Like [`Self::process_updates`], this will return a list of paths and the contents that
    /// should be written to them.
    pub async fn process_fs_patch(&self, patch: GraphPatch) -> Vec<(PathBuf, String)> {
        // Start with renames (they have to be fully executed before anything else so the right
        // paths are in the map for everything else)
        // TODO: If we can make renames happen at the *end* rather than the start, these could be
        // processed like everything else...
        self.process_renames(patch.renames).await;

        // Creations, deletions, and modifications need read guards, and so can all be done
        // simultaneously without impacting anything else. Creations can be done synchronously, the
        // others are async.
        let mut updates = Vec::new();
        for path_patch in patch.creations {
            let (path_node, mut updates_l) =
                PathNode::new(path_patch.path, path_patch.contents_res);
            updates_l.push(GraphUpdate::CreatePathNode(path_node));
            updates.push(updates_l);
        }

        let paths = self.paths.read().await;
        let mut deletion_futs = Vec::new();
        for path in patch.deletions {
            if let Some(path_node) = paths.get(&path) {
                deletion_futs.push(async {
                    let path_node = path_node.read().await;
                    path_node.delete()
                });
            }
        }
        let mut modification_futs = Vec::new();
        for path_patch in patch.modifications {
            if let Some(path_node) = paths.get(&path_patch.path) {
                modification_futs.push(async {
                    let path_node = path_node.read().await;
                    let (new_path_node, mut updates_l) =
                        path_node.update(path_patch.path.clone(), path_patch.contents_res);
                    updates_l.push(GraphUpdate::ModifyPathNode {
                        // We use the old path in case the new one has changed
                        path: path_patch.path,
                        new_node: new_path_node,
                    });

                    updates_l
                });
            }
        }

        // These are both `Vec<Vec<GraphUpdate>>`
        let (deletion_updates, modification_updates) =
            join(join_all(deletion_futs), join_all(modification_futs)).await;
        updates.extend(deletion_updates);
        updates.extend(modification_updates);

        // This doesn't get automatically dropped, so we have to do it manually to avoid a deadlock
        drop(paths);

        self.process_updates(updates.into_iter().map(|v| v.into_iter()).flatten())
            .await
    }
    /// Fully processes the given array of renames (where each tuple is a `from` and then `to`
    /// path). This will update the paths map and all the nodes in the renamed paths.
    async fn process_renames(&self, renames: Vec<(PathBuf, PathBuf)>) {
        // Short-circuit if there are no renames to avoid unnecessary locking
        if renames.is_empty() {
            return;
        }

        let mut nodes = self.nodes.write().await;
        let mut paths = self.paths.write().await;
        for (from, to) in renames {
            // If we can't find the original path, we'll leave this
            if let Some(path_node) = paths.remove(&from) {
                // We hold the only reference, reading is guaranteed
                let path_node_ref = path_node.try_read().unwrap();
                for node_id in path_node_ref.ids() {
                    let node_path = nodes.get_mut(node_id).unwrap();
                    *node_path = to.clone();
                }
                drop(path_node_ref);

                paths.insert(to, path_node);
            } else {
                debug_assert!(
                    false,
                    "found rename instruction for path that wasn't in the graph"
                );
            }
        }
    }
    /// Processes a series of [`GraphUpdate`]s and modifies the graph accordingly. This will return
    /// a list of paths which need to be updated on the disk and the string contents that should be
    /// written to them.
    ///
    /// *Hint: if there's a deadlock, it's probably happening in here!*
    async fn process_updates(
        &self,
        updates: impl Iterator<Item = GraphUpdate>,
    ) -> Vec<(PathBuf, String)> {
        let mut should_lock_nodes = false;
        let mut should_lock_paths = false;
        let mut should_lock_invalid_connections = false;
        // These are the IDs of nodes whose paths we'll need to lock (but not all of them will be
        // entered into the nodes map until after stage 1). If any of them don't exist, they'll be
        // from `GraphUpdate::CheckConnection`, so it's fine if they aren't available.
        //
        // We use a `HashSet` to avoid unnecessary doubling-up (which would lead to deadlocks).
        let mut nodes_to_lock = HashSet::new();

        // We split updates into those that affect maps only, and those that affect nodes (and
        // possibly the invalid connections map)
        let mut map_updates = Vec::new();
        let mut node_updates = Vec::new();

        // This will keep track of paths we need to write to (i.e. any new nodes or validated
        // connections), which will be because they have new IDs (possibly forced and we need to
        // make them permanent) or new/updated connections, or one of their connection targets
        // updated its title.
        let mut paths_to_write = HashSet::new();

        for update in updates {
            match update {
                // Map updates (stage 1)
                GraphUpdate::CreatePathNode(_)
                // We use coarse locks for modification to avoid breaking the hierarchy of stages
                // (otherwise we'd have to pre-lock a path before we've worked out what other paths
                // we're going to lock, etc.)
                | GraphUpdate::ModifyPathNode { .. }
                | GraphUpdate::DeletePathNode(_) => {
                    map_updates.push(update);
                    should_lock_paths = true;
                }
                GraphUpdate::AddNode { .. } | GraphUpdate::RemoveNode(_) => {
                    should_lock_nodes = true;

                    // Adding a new node might make some connections that were previously invalid
                    // valid, so we could need the invalid connections map as well
                    if let GraphUpdate::AddNode { id, ref path } = update {
                        should_lock_invalid_connections = true;

                        // A new node might have had an ID force-created for it during parsing, so
                        // we should write this path back to the disk to ensure ID stability
                        paths_to_write.insert(path.clone());
                        // We need to lock that path in order to write to it, and this ID comes
                        // from it, so locking that is sufficient
                        nodes_to_lock.insert(id);
                    }

                    map_updates.push(update);
                }
                GraphUpdate::ValidateInvalidConnection { .. } => {
                    should_lock_invalid_connections = true;
                    map_updates.push(update);
                    // We will need to lock some nodes, but we don't know which ones until we see
                    // the map
                }
                GraphUpdate::RemoveInvalidConnection { .. } => {
                    map_updates.push(update);
                    should_lock_invalid_connections = true;
                }

                // Node updates (stage 2)
                GraphUpdate::InvalidateConnection { on, .. } => {
                    node_updates.push(update);
                    nodes_to_lock.insert(on);
                }
                GraphUpdate::RemoveBacklink { on, .. } => {
                    node_updates.push(update);
                    nodes_to_lock.insert(on);
                }
                GraphUpdate::CheckConnection { from, to } => {
                    node_updates.push(update);
                    // We'll need to read the `from` path node and possibly modify the connection
                    // in it to be valid; also might need to write this whole path to its source if
                    // it's valid (to rewrite titles)
                    nodes_to_lock.insert(from);
                    // And we might need to add a backlink to `to`, if it exists
                    nodes_to_lock.insert(to);

                    // We also might need to add an invalid connection
                    should_lock_invalid_connections = true;
                }
            }
        }

        // Lock all the maps we need, in the global locking order
        let mut nodes = OptionFuture::from(should_lock_nodes.then(|| self.nodes.write())).await;
        let mut paths = OptionFuture::from(should_lock_paths.then(|| self.paths.write())).await;
        let mut invalid_connections = OptionFuture::from(
            should_lock_invalid_connections.then(|| self.invalid_connections.write()),
        )
        .await;

        // Now we have what we need to run the stage 1 updates (which operate on maps). We'll
        // insert things with new locks here, which doesn't matter because nothing can get at them
        // while we hold write locks over the maps. We'll acquire fine-grained locks *before*
        // releasing the coarse locks for this reason.
        //
        // We handle pretty much every update as if it might have already happened or might now be
        // invalid, because one modification might overlap with another (they'll happen
        // sequentially if they involve some of the same paths due to the global locking order, but
        // logical overlap/invalidation can still occur).
        for update in map_updates {
            match update {
                GraphUpdate::CreatePathNode(path_node) => {
                    // BUG: Multiple adds of the same path that contain different nodes could be a
                    // problem...
                    debug_assert!(
                        !paths.as_ref().unwrap().contains_key(&path_node.path()),
                        "tried to create new path node that was already present in graph"
                    );

                    paths
                        .as_mut()
                        .unwrap()
                        .insert(path_node.path(), RwLock::new(path_node));
                }
                GraphUpdate::ModifyPathNode { path, new_node } => {
                    // TODO: What do we do if this has already been removed?
                    debug_assert!(
                        paths.as_ref().unwrap().contains_key(&path),
                        "tried to modify path node that wasn't in the graph"
                    );

                    let path_node = paths.as_mut().unwrap().get_mut(&path).unwrap();
                    // Fine to blitz the other lock, there can't be any references to it
                    *path_node = RwLock::new(new_node);
                }
                GraphUpdate::DeletePathNode(path) => {
                    // This certainly should still exist, but it's no big deal if it doesn't
                    paths.as_mut().unwrap().remove(&path);
                }
                GraphUpdate::AddNode { id, path } => {
                    // BUG: Big problem if this has just been added going to a *different* path...
                    debug_assert!(
                        !nodes.as_ref().unwrap().contains_key(&id),
                        "tried to add node that was already present in graph"
                    );

                    nodes.as_mut().unwrap().insert(id, path);
                }
                GraphUpdate::RemoveNode(node_id) => {
                    // This certainly should still exist, but it's no big deal if it doesn't
                    nodes.as_mut().unwrap().remove(&node_id);
                }
                GraphUpdate::ValidateInvalidConnection { to } => {
                    // We'll need to add backlinks to all the nodes that referenced this invalid
                    // connection
                    if let Some(referrers) = invalid_connections.as_mut().unwrap().remove(&to) {
                        nodes_to_lock.insert(to);

                        for referrer in referrers {
                            // NOTE: This is the only instance where we retroactively add an
                            // update. We replicate perfectly the logic we would have used to
                            // handle it though, including ordering the locking of the appropriate
                            // nodes, so in this case, this violation of the overall paradigm is
                            // acceptable.
                            node_updates.push(GraphUpdate::CheckConnection { from: referrer, to });
                            nodes_to_lock.insert(referrer);
                        }
                    }
                }
                GraphUpdate::RemoveInvalidConnection { from, to } => {
                    if let Some(invalid_referrers) =
                        invalid_connections.as_mut().unwrap().get_mut(&from)
                    {
                        invalid_referrers.remove(&to);
                    }
                }

                _ => unreachable!(),
            }
        }

        // We'll need to map from node IDs to paths to figure out which paths to lock, but we won't
        // need to change anything about this relation, so it's fine to have `nodes` as a read
        // guard
        let nodes_ref = if let Some(nodes) = nodes {
            nodes.downgrade()
        } else {
            self.nodes.read().await
        };

        // If we don't have a write guard on `self.paths`, then the map is in a valid state right
        // now, so we can get a read guard to the paths and use that to get our fine-grained write
        // guards (we'll lock in the global order, so we won't deadlock). However, if we already
        // have a write guard,, the map is currently in an invalid state (e.g. bad backlinks), so
        // we can't let anyone else touch it until we have locks over all the affected paths. That
        // means we need to have a getter which uses the write guard if it exists, or falls back to
        // the read guard (which will definitely exist if the write guard doesn't).
        //
        // Unfortunately, Rust won't let us drop the write guard afterward, but this is better than
        // state pollution.
        let paths_ref = OptionFuture::from((!should_lock_paths).then(|| self.paths.read())).await;
        let path_node_getter = |path: &PathBuf| {
            if let Some(paths) = paths.as_ref() {
                paths.get(path)
            } else {
                // Guaranteed to exist if we didn't have a write guard
                paths_ref.as_ref().unwrap().get(path)
            }
        };

        // Acquire fine-grained locks on the paths *in-order* to ensure we don't get circular waits
        // and therefore deadlocks; not every node will resolve because some will be invalid
        // `CheckConnection`s
        let mut paths_to_lock = nodes_to_lock
            .into_iter()
            .filter_map(|id| nodes_ref.get(&id))
            // Ensure there are no duplicates
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        paths_to_lock.sort_unstable();
        let mut path_nodes = HashMap::new();
        for path in paths_to_lock {
            path_nodes.insert(
                path.to_path_buf(),
                path_node_getter(path).unwrap().write().await,
            );
        }

        // TODO: Would be great if we could downgrade a possible write guard here...

        // We now have everything we need to handle node-level updates
        for update in node_updates {
            match update {
                GraphUpdate::InvalidateConnection { on, to } => {
                    // If the target was deleted in another instruction, this doesn't matter
                    // anymore
                    if let Some(path) = nodes_ref.get(&on) {
                        let path_node = path_nodes.get_mut(path).unwrap();
                        path_node.invalidate_connection(on, to);
                    }
                }
                GraphUpdate::RemoveBacklink { on, from } => {
                    // If the target was deleted in another instruction, this doesn't matter
                    // anymore
                    if let Some(path) = nodes_ref.get(&on) {
                        let path_node = path_nodes.get_mut(path).unwrap();
                        path_node.remove_backlink(on, from);
                    }
                }
                GraphUpdate::CheckConnection { from, to } => {
                    // Another instruction *could* have ripped this node out from under us
                    if let Some(path_from) = nodes_ref.get(&from) {
                        // Here, if the target doesn't exist, then we should log an invalid connection
                        // (the existence of this update means we will have a write guard on that map)
                        if let Some(path_to) = nodes_ref.get(&to) {
                            // Add the backlink first and get the title
                            let path_node_to = path_nodes.get_mut(path_to).unwrap();
                            path_node_to.add_backlink(to, from);
                            let title = path_node_to
                                .display_title(
                                    to,
                                    // We're getting the title of this node to display in our
                                    //`from` node, let's use the format of the from node so we
                                    // implant a title that makes sense (even though we're talking
                                    // about insane nested connections here...)
                                    if path_from.extension().unwrap_or_default() == "org" {
                                        Format::Org
                                    } else {
                                        Format::Markdown
                                    },
                                )
                                .unwrap();

                            // And then validate the connection and update the title of the target
                            let path_node_from = path_nodes.get_mut(path_from).unwrap();
                            path_node_from.validate_connection(from, to, title);

                            // We've updated a title, which means we need to write the from path
                            // back to the disk (this path is guaranteed already locked)
                            paths_to_write.insert(path_from.clone());
                        } else {
                            invalid_connections
                                .as_mut()
                                .unwrap()
                                .entry(to)
                                .or_insert_with(|| HashSet::new())
                                .insert(from);
                        }
                    }
                }

                _ => unreachable!(),
            }
        }

        // All the paths we need to write to are guaranteed to be locked, so go through them and
        // convert their documents to strings
        paths_to_write
            .into_iter()
            .filter_map(|path| {
                let path_node = path_nodes.get(&path).unwrap();
                // There most certainly should be a document currently, but for future-proofness
                // we'll allow there not to be
                let format = if path.extension().unwrap_or_default() == "org" {
                    Format::Org
                } else {
                    Format::Markdown
                };
                Some((
                    path,
                    path_node
                        .document()?
                        .to_document(format)
                        .into_string(format),
                ))
            })
            .collect()
    }
}
