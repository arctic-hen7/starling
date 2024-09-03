use crate::{debouncer::DebouncedEvents, patch::GraphPatch, path_node::PathNode};
use futures::future::join;
use futures::future::join3;
use futures::future::join_all;
use futures::future::OptionFuture;
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
    /// Currently, this is generated when a [`GraphUpdate::AddNode`] instruction is encountered.
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
    nodes: RwLock<NodeMap>,
    /// All the paths in the graph, indexed by their (relative) paths. On a rename, an entry will
    /// be removed and recreated here. All the node IDs on a path are guaranteed to exist in the
    /// nodes map and point back to this path.
    paths: RwLock<PathMap>,
    /// A list of invalid connections, indexed by the invalid ID they connected to, and listing in
    /// each entry the set of nodes which made such a connection, by their IDs.
    invalid_connections: RwLock<InvalidConnectionsMap>,
}
impl Graph {
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

        let this = Self {
            nodes: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
            invalid_connections: RwLock::new(HashMap::new()),
        };
        this.process_fs_patch(patch).await;

        this
    }
    /// Process a batch of updates from the filesystem. This operates as the start of a pipeline,
    /// generating modifications which in turn generate instructions for locking and graph updates.
    /// This will acquire read locks on the paths map and some individual paths as necessary to
    /// generate updates, but it will not write anything directly (though it will call both
    /// [`Self::process_renames`] and [`Self::process_updates`]).
    async fn process_fs_patch(&self, patch: GraphPatch) {
        // Start with renames (they have to be fully executed before anything else so the right
        // paths are in the map for everything else)
        // TODO: If we can make renames happen at the *end* rather than the start, these could be
        // processed like everything else...
        self.process_renames(patch.renames);

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

        self.process_updates(updates.into_iter().map(|v| v.into_iter()).flatten())
            .await;
    }
    /// Fully processes the given array of renames (where each tuple is a `from` and then `to`
    /// path). This will update the paths map and all the nodes in the renamed paths.
    async fn process_renames(&self, renames: Vec<(PathBuf, PathBuf)>) {
        // Short-circuit if there are no renames to avoid unnecessary locking
        if renames.is_empty() {
            return;
        }

        let mut paths = self.paths.write().await;
        let mut nodes = self.nodes.write().await;
        for (from, to) in renames {
            // If we can't find the original path, we'll leave this
            if let Some(path_node) = paths.remove(&from) {
                // We hold the only reference, reading is guaranteed
                let path_node_ref = path_node.try_read().unwrap();
                for node_id in path_node_ref.ids() {
                    let node_path = nodes.get_mut(node_id).unwrap();
                    *node_path = path_node_ref.path();
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
    /// Processes a series of [`GraphUpdate`]s and modifies the graph accordingly. This method is
    /// where all locking occurs.
    ///
    /// *Hint: if there's a deadlock, it's happening in here!*
    async fn process_updates(&self, updates: impl Iterator<Item = GraphUpdate>) {
        // Write locks over the maps will go here
        let mut nodes_fut = None;
        let mut paths_fut = None;
        let mut invalid_connections_fut = None;
        // These are the IDs of nodes whose paths we'll need to lock (but not all of them will be
        // entered into the nodes map until after stage 1). If any of them don't exist, they'll be
        // from `GraphUpdate::CheckConnection`, so it's fine if they aren't available.
        //
        // We use a `HashSet` to avoid unnecessary doubling-up (which would lead to deadlocks).
        let mut nodes_to_lock = HashSet::new();

        // This is used in multiple places, so we need one `async` block
        let mut ic_predefined_fut = Some(async { self.invalid_connections.write().await });

        // We split updates into those that affect maps only, and those that affect nodes (and
        // possibly the invalid connections map)
        let mut map_updates = Vec::new();
        let mut node_updates = Vec::new();

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
                    if paths_fut.is_none() {
                        paths_fut = Some(async { self.paths.write().await });
                    }
                }
                GraphUpdate::AddNode { .. } | GraphUpdate::RemoveNode(_) => {
                    if nodes_fut.is_none() {
                        nodes_fut = Some(async { self.nodes.write().await });
                    }

                    // Adding a new node might make some connections that were previously invalid
                    // valid, so we could need the invalid connections map as well
                    if let GraphUpdate::AddNode { id, .. } = update {
                        if invalid_connections_fut.is_none() {
                            invalid_connections_fut = Some(ic_predefined_fut.take().unwrap());
                        }
                        map_updates.push(GraphUpdate::ValidateInvalidConnection { to: id });
                    }

                    map_updates.push(update);
                }
                GraphUpdate::ValidateInvalidConnection { .. } => {
                    // This is part of `AddNode`, we shouldn't ever reach it independently
                    // (currently)
                    debug_assert!(false, "reached instruction to validate invalid connection independently of adding a node");
                }
                GraphUpdate::RemoveInvalidConnection { .. } => {
                    map_updates.push(update);
                    if invalid_connections_fut.is_none() {
                        invalid_connections_fut = Some(ic_predefined_fut.take().unwrap());
                    }
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
                    // in it to be valid
                    nodes_to_lock.insert(from);
                    // And we might need to add a backlink to `to`, if it exists
                    nodes_to_lock.insert(to);

                    // We also might need to add an invalid connection
                    if invalid_connections_fut.is_none() {
                        invalid_connections_fut = Some(ic_predefined_fut.take().unwrap());
                    }
                }
            }
        }

        // Lock all the maps we need
        let (mut nodes, mut paths, mut invalid_connections) = join3(
            OptionFuture::from(nodes_fut),
            OptionFuture::from(paths_fut),
            OptionFuture::from(invalid_connections_fut),
        )
        .await;
        // Now we have what we need to run the stage 1 updates (which operate on maps). We'll
        // insert things with new locks here, which doesn't matter because nothing can get at them
        // while we hold write locks over the maps. We'll acquire fine-grained locks *before*
        // releasing the coarse locks for this reason.
        for update in map_updates {
            match update {
                GraphUpdate::CreatePathNode(path_node) => {
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
                    debug_assert!(
                        paths.as_ref().unwrap().contains_key(&path),
                        "tried to modify path node that wasn't in the graph"
                    );

                    let path_node = paths.as_mut().unwrap().get_mut(&path).unwrap();
                    // Fine to blitz the other lock, there can't be any references to it
                    *path_node = RwLock::new(new_node);
                }
                GraphUpdate::DeletePathNode(path) => {
                    debug_assert!(
                        paths.as_ref().unwrap().contains_key(&path),
                        "tried to remove path node that wasn't in the graph"
                    );
                    // This certainly should still exist, but it's no big deal if it doesn't
                    paths.as_mut().unwrap().remove(&path);
                }
                GraphUpdate::AddNode { id, path } => {
                    debug_assert!(
                        !nodes.as_ref().unwrap().contains_key(&id),
                        "tried to add node that was already present in graph"
                    );

                    nodes.as_mut().unwrap().insert(id, path);
                }
                GraphUpdate::RemoveNode(node_id) => {
                    debug_assert!(
                        nodes.as_ref().unwrap().contains_key(&node_id),
                        "tried to remove node that wasn't in the graph"
                    );

                    // This certainly should still exist, but it's no big deal if it doesn't
                    nodes.as_mut().unwrap().remove(&node_id);
                }
                GraphUpdate::ValidateInvalidConnection { to } => {
                    debug_assert!(
                        invalid_connections.as_ref().unwrap().contains_key(&to),
                        "tried to validate invalid connection that wasn't in the map"
                    );

                    // We'll need to add backlinks to all the nodes that referenced this invalid
                    // connection
                    if let Some(referrers) = invalid_connections.as_mut().unwrap().remove(&to) {
                        // This will almost certainly already be accounted for by other connection
                        // checking instructions, but it's worth doubling up
                        nodes_to_lock.insert(to);

                        for referrer in referrers {
                            node_updates.push(GraphUpdate::CheckConnection { from: referrer, to });
                            nodes_to_lock.insert(referrer);
                        }
                    }
                }
                GraphUpdate::RemoveInvalidConnection { from, to } => {
                    if let Some(invalid_referrers) =
                        invalid_connections.as_mut().unwrap().get_mut(&from)
                    {
                        debug_assert!(
                            invalid_referrers.contains(&to),
                            "tried to remove invalid connection that wasn't in the map (referrer side)"
                        );

                        invalid_referrers.remove(&to);
                    } else {
                        debug_assert!(
                            false,
                            "tried to remove invalid connection that wasn't in the map (referent side)"
                        );
                    }
                }

                _ => unreachable!(),
            }
        }

        // We'll need to map from node IDs to paths to figure out which paths to lock, so downgrade
        // any write guards to read guards or separately acquire such read guards
        let (nodes_ref, paths_ref) = join(
            async {
                if let Some(nodes) = nodes {
                    // This is fine because it's atomic, a writer can't get to this in the meantime and
                    // access an invalid map state (i.e. without proper connections)
                    nodes.downgrade()
                } else {
                    self.nodes.read().await
                }
            },
            async {
                if let Some(paths) = paths {
                    paths.downgrade()
                } else {
                    self.paths.read().await
                }
            },
        )
        .await;

        // Now we can release the coarse locks and acquire fine-grained locks
        let path_nodes_futs = nodes_to_lock.into_iter().map(|id| {
            // We should always be able to find these
            let path = nodes_ref.get(&id).expect("node ID not found in nodes map");
            let path_node = paths_ref.get(path).unwrap();
            async { (path.to_path_buf(), path_node.write().await) }
        });
        // This will be used instead of `paths_ref`
        let mut path_nodes: HashMap<PathBuf, _> =
            join_all(path_nodes_futs).await.into_iter().collect();

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
                    // We should never get here, because that would require creating and deleting a
                    // node in the same instruction set, which *should* be impossible (that would
                    // just be a modification...)
                    debug_assert!(
                        nodes_ref.contains_key(&from),
                        "tried to check connection from non-existent node"
                    );
                    let path_from = nodes_ref.get(&from).unwrap();

                    // Here, if the target doesn't exist, then we should log an invalid connection
                    // (the existence of this update means we will have a write guard on that map)
                    if let Some(path_to) = nodes_ref.get(&to) {
                        // Add the backlink first and get the title
                        let path_node_to = path_nodes.get_mut(path_to).unwrap();
                        path_node_to.add_backlink(to, from);
                        let title = path_node_to.display_title(to).unwrap();

                        // And then validate the connection and update the title of the target
                        let path_node_from = path_nodes.get_mut(path_from).unwrap();
                        path_node_from.validate_connection(from, to, title);
                    } else {
                        invalid_connections
                            .as_mut()
                            .unwrap()
                            .entry(from)
                            .or_insert_with(|| HashSet::new())
                            .insert(from);
                    }
                }

                _ => unreachable!(),
            }
        }

        // All fine-grained and coarse-grained locks are dropped here, and the map is in a valid
        // state
    }
}
