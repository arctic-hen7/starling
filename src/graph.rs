use crate::conflict_detector::{Conflict, Write, WriteSource};
use crate::node::{Node, NodeOptions};
use crate::path_node::StarlingNode;
use crate::{debouncer::DebouncedEvents, patch::GraphPatch, path_node::PathNode};
use futures::future::join;
use futures::future::join_all;
use futures::future::OptionFuture;
use orgish::Format;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use tokio::sync::{RwLock, RwLockWriteGuard};
use tracing::{debug, error, info, warn};
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
    /// The provided node should be added to the graph. This will check the invalid connections map
    /// to validate any previously invalid connections other nodes had to this one (i.e. backlinks
    /// will be created, titles will be updated, etc. --- this creates
    /// [`GraphUpdate::CheckConnection`] instructions).
    ///
    /// As new nodes may have IDs that have been force-generated during parsing, any paths subject
    /// to one of these will be written to disk.
    AddNode { id: Uuid, path: PathBuf },
    /// The node with the given ID should be removed. This will correspond to a blatant deletion of
    /// the node from the map of all nodes (i.e. connections will *not* be handled, and separate
    /// [`GraphUpdate::RemoveBacklink`] instructions will probably be needed).
    RemoveNode(Uuid),
    /// The provided node should be added to the index with the given name. This will not create
    /// any connection checking instructions or the like.
    AddNodeToIndex {
        id: Uuid,
        path: PathBuf,
        index: String,
    },
    /// The node with the given ID should be removed from the index with the given name.
    RemoveNodeFromIndex { id: Uuid, index: String },
    /// We should remove the backlink on the node with the given ID from the node with the given
    /// ID. This will be because either there is no longer a connection to it, or because the
    /// source vertex has been removed.
    RemoveBacklink { on: Uuid, from: Uuid },
    /// We should remove a record of an invalid connection from the (valid) node with the given ID
    /// to the given (invalid) ID. If there are no references left to that invalid ID, we should
    /// drop it entirely (we only keep track of them to speed the process of validating previously
    /// existing connections to newly valid created nodes).
    RemoveInvalidConnection { from: Uuid, to: Uuid },
    /// We should check if the connection from the node with the given ID to the node with the
    /// given ID is valid. If so, we should set it as valid and create an appropriate backlink, and
    /// if not, we should leave it as invalid and register it in a global map that accounts for
    /// invalid connections (so they can be easily rendered valid if a node with the ID they point
    /// to is created).
    ///
    /// Note that this is also used for rendering valid connections that were previously marked as
    /// invalid in the global tracker when a node with the (invalid) ID they pointed to is created
    /// (in which case we know this will succeed). It is also used to invalidate known-bad
    /// connections.
    ///
    /// If the connection is found to be valid, the path which made the connection will be written
    /// to disk with any updated connection titles.
    CheckConnection { from: Uuid, to: Uuid },
}
impl std::fmt::Debug for GraphUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphUpdate::CreatePathNode(_) => write!(f, "CreatePathNode"),
            GraphUpdate::ModifyPathNode { path, new_node: _ } => {
                write!(f, "ModifyPathNode({:?}, [node])", path)
            }
            GraphUpdate::DeletePathNode(path) => write!(f, "DeletePathNode({:?})", path),
            GraphUpdate::AddNode { id, path } => write!(f, "AddNode({:?}, {:?})", id, path),
            GraphUpdate::RemoveNode(id) => write!(f, "RemoveNode({:?})", id),
            GraphUpdate::AddNodeToIndex { id, path, index } => {
                write!(f, "AddNodeToIndex({:?}, {:?}, {:?})", id, path, index)
            }
            GraphUpdate::RemoveNodeFromIndex { id, index } => {
                write!(f, "RemoveNodeFromIndex({:?}, {:?})", id, index)
            }
            GraphUpdate::RemoveBacklink { on, from } => {
                write!(f, "RemoveBacklink({:?}, {:?})", on, from)
            }
            GraphUpdate::RemoveInvalidConnection { from, to } => {
                write!(f, "RemoveInvalidConnection({:?}, {:?})", from, to)
            }
            GraphUpdate::CheckConnection { from, to } => {
                write!(f, "CheckConnection({:?}, {:?})", from, to)
            }
        }
    }
}

type NodeMap = HashMap<Uuid, PathBuf>;
type PathMap = HashMap<PathBuf, RwLock<PathNode>>;
type InvalidConnectionsMap = HashMap<Uuid, HashSet<Uuid>>;

pub(crate) struct IndexMap {
    /// An alphabetically-ordered map of the actual index data.
    inner: Vec<Index>,
    /// A map to the indices in the above vector for lookup.
    map: HashMap<String, usize>,
}
impl IndexMap {
    fn new(indices: HashMap<String, IndexCriteria>) -> Self {
        let mut inner = Vec::new();
        let mut map = HashMap::new();

        let mut names = Vec::new();
        for name in indices.keys() {
            names.push(name.clone());
        }
        // Fine to do an unstable sort, duplicates are impossible
        names.sort_unstable();
        for name in names {
            let index = Index {
                nodes: RwLock::new(HashMap::new()),
                criteria: indices[&name].clone(),
            };
            map.insert(name, inner.len());
            inner.push(index);
        }

        Self { inner, map }
    }
    fn get(&self, name: &str) -> Option<&Index> {
        self.map.get(name).map(|i| &self.inner[*i])
    }
    fn remove(&mut self, name: &str) -> Option<Index> {
        self.map.remove(name).map(|i| self.inner.remove(i))
    }
    async fn write_all(&self) -> HashMap<&String, RwLockWriteGuard<NodeMap>> {
        // Use the ordering of `self.inner` to lock in order
        let mut locks = Vec::new();
        for index in &self.inner {
            locks.push(Some(index.nodes.write().await));
        }

        let mut locks_map = HashMap::new();
        for (name, idx) in self.map.iter() {
            locks_map.insert(name, locks[*idx].take().unwrap());
        }

        locks_map
    }
    async fn write_some(
        &self,
        names: HashSet<String>,
    ) -> HashMap<&String, RwLockWriteGuard<NodeMap>> {
        let mut indices_to_lock = HashSet::new();
        for (name, idx) in self.map.iter() {
            if names.contains(name) {
                indices_to_lock.insert(*idx);
            }
        }

        // Use the ordering of `self.inner` to lock in order
        let mut locks = Vec::new();
        for (idx, index) /* SCREAM */ in self.inner.iter().enumerate() {
            if indices_to_lock.contains(&idx) {
                locks.push(Some(index.nodes.write().await));
            }
        }

        let mut locks_map = HashMap::new();
        for (name, idx) in self.map.iter() {
            if indices_to_lock.contains(idx) {
                locks_map.insert(name, locks[*idx].take().unwrap());
            }
        }

        locks_map
    }
    fn criteria(&self) -> HashMap<String, IndexCriteria> {
        self.map
            .iter()
            .map(|(name, idx)| (name.clone(), self.inner[*idx].criteria.clone()))
            .collect()
    }
    fn checkers(&self) -> Vec<(IndexCriteria, String)> {
        self.map
            .iter()
            .map(|(name, idx)| (self.inner[*idx].criteria.clone(), name.clone()))
            .collect()
    }
    pub(crate) fn names(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }
}

/// A single *index*, which holds a subset of the total nodes map, indexed by some criteria. The
/// map of nodes that an index holds includes values for the paths, allowing the same indexing
/// speed as if one were using the full map.
pub(crate) struct Index {
    nodes: RwLock<NodeMap>,
    criteria: IndexCriteria,
}
pub type IndexCriteria = Arc<dyn Fn(&StarlingNode) -> bool + Send + Sync>;

/// A graph of many nodes derived from Org/Markdown files ([`PathNode`]s), which are connected
/// together.
pub struct Graph {
    /// A map of all the nodes in the graph to the paths containing them (which are guaranteed to
    /// exist and contain them).
    ///
    /// If maps are to be locked, this must always be locked first.
    pub(crate) nodes: RwLock<NodeMap>,
    /// A map of indices. The user can create arbitrary indices (with arbitrary names) to index
    /// subsets of the nodes map by certain criteria, allowing the implementation of all sorts
    /// of faster search mechanisms over subsets of the graph.
    ///
    /// Indices cannot be modified once the graph has been created. However, the inner node maps of
    /// each index must be locked in alphabetical order on the index names, and such locking must
    /// be done second.
    pub(crate) indices: IndexMap,
    /// All the paths in the graph, indexed by their (relative) paths. On a rename, an entry will
    /// be removed and recreated here. All the node IDs on a path are guaranteed to exist in the
    /// nodes map and point back to this path.
    ///
    /// If maps are to be locked, this must always be locked third. If individual paths are to be
    /// locked, they should be locked sorted in path order to prevent deadlocks.
    pub(crate) paths: RwLock<PathMap>,
    /// A list of invalid connections, indexed by the invalid ID they connected to, and listing in
    /// each entry the set of nodes which made such a connection, by their IDs.
    ///
    /// If maps are to be locked, this must always be locked fourth.
    pub(crate) invalid_connections: RwLock<InvalidConnectionsMap>,
}
impl Graph {
    /// Creates a new, completely empty graph. Typically, [`Self::from_dir`] would be used to
    /// initially populate the graph from a directory. This also takes a series of indices and
    /// their properties.
    pub fn new(indices: HashMap<String, IndexCriteria>) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            indices: IndexMap::new(indices),
            paths: RwLock::new(HashMap::new()),
            invalid_connections: RwLock::new(HashMap::new()),
        }
    }
    /// Returns any errors associated with the given path. The return type here is a little
    /// strange: if the path couldn't be parsed, you'll get an `Err(PathParseError)` (stringified),
    /// but if it could be, you'll get an `Ok(_)` with a list of the IDs of all invalid connections
    /// made in the path. If the path doesn't exist at all, you'll get `None`.
    #[tracing::instrument(skip(self))]
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
    /// Gets the ID of the root node in the given path, if it exists and has a document defined.
    /// This can be used to, given a path, start interfacing with its nodes.
    pub async fn root_id(&self, path: &Path) -> Option<Uuid> {
        let paths = self.paths.read().await;
        let path_node = paths.get(path)?;
        let path_node = path_node.read().await;
        path_node
            .document()
            .map(|doc| *doc.root.scrubbed_node().properties.id)
    }
    /// Creates a new graph, tracking all files in the given directory recursively. This will read
    /// every file that can be parsed and parse them all, returning both the graph itself and a
    /// series of writes that should be made to correct any initial errors.
    ///
    /// # Panics
    ///
    /// This will panic if the provided path is not a valid directory.
    pub async fn from_dir(
        dir: &Path,
        indices: HashMap<String, IndexCriteria>,
    ) -> (Self, Vec<Write>) {
        assert!(dir.is_dir());

        // Fake creation events recursively for everything in the directory
        let creations = DebouncedEvents::start_from_dir(dir);
        let patch = GraphPatch::from_events(creations, dir).await;

        let this = Self::new(indices);
        let writes = this.process_fs_patch(patch).await;

        (this, writes)
    }
    /// Rescans the given directory, completely reconstructing the graph from it, from scratch.
    /// This will take considerably longer than processing atomic file events, and should only be
    /// done if absolutely necessary. This returns any correcting writes needed.
    #[tracing::instrument(skip(self))]
    pub async fn rescan(&mut self, dir: &Path) -> Vec<Write> {
        let mut nodes = self.nodes.write().await;
        let index_locks = self.indices.write_all().await;
        let mut paths = self.paths.write().await;
        let mut invalid_connections = self.invalid_connections.write().await;

        let (mut new_graph, writes) = Self::from_dir(dir, self.indices.criteria()).await;
        *nodes = new_graph.nodes.into_inner();
        *paths = new_graph.paths.into_inner();
        *invalid_connections = new_graph.invalid_connections.into_inner();

        // Update each index in order (the new graph is guaranteed to have the same indices)
        for (index_name, mut index_map) in index_locks {
            *index_map = new_graph
                .indices
                .remove(index_name)
                .unwrap()
                .nodes
                .into_inner();
        }

        writes
    }
    /// Gets a list of all the nodes in the given index (or across the whole system if the index is
    /// `None`), with their titles and the paths from which they came. This takes a format for
    /// links in titles.
    #[tracing::instrument(skip(self))]
    pub async fn nodes(&self, index: Option<&str>, options: NodeOptions) -> Vec<Node> {
        let nodes = if let Some(index_name) = index {
            self.indices
                .get(index_name)
                .as_ref()
                .unwrap()
                .nodes
                .read()
                .await
        } else {
            self.nodes.read().await
        };

        let mut full_nodes = Vec::new();
        for id in nodes.keys() {
            // A node listed in an index is guaranteed to exist
            full_nodes.push(self.get_node(*id, options).await.unwrap());
        }

        // In testing, we need a reliable order
        #[cfg(test)]
        full_nodes.sort_by_key(|n| n.id);

        full_nodes
    }
    /// Process a batch of updates from the filesystem. This operates as the start of a pipeline,
    /// generating modifications which in turn generate instructions for locking and graph updates.
    /// This will acquire read locks on the paths map and some individual paths as necessary to
    /// generate updates, but it will not write anything directly (though it will call both
    /// [`Self::process_renames`] and [`Self::process_updates`]).
    ///
    /// Like [`Self::process_updates`], this will return a list of paths and the contents that
    /// should be written to them.
    #[tracing::instrument(skip_all)]
    pub async fn process_fs_patch(&self, patch: GraphPatch) -> Vec<Write> {
        info!("about to process patch {:?}", patch);
        // Create a list of the index criteria to send to the processing path for each node
        let index_checkers = self.indices.checkers();

        // Start with renames (they have to be fully executed before anything else so the right
        // paths are in the map for everything else)
        self.process_renames(patch.renames).await;

        // Creations, deletions, and modifications need read guards, and so can all be done
        // simultaneously without impacting anything else. Creations can be done synchronously, the
        // others are async. We do deletions first to avoid possible ID conflicts and the like.
        let mut creation_updates = Vec::new();
        let paths = self.paths.read().await;
        for path_patch in patch.creations {
            let (path_node, mut updates_l) =
                PathNode::new(path_patch.path, path_patch.contents_res, &index_checkers);
            updates_l.push(GraphUpdate::CreatePathNode(path_node));
            creation_updates.push(updates_l);
        }
        let mut deletion_futs = Vec::new();
        for path in patch.deletions {
            // We by definition can't do anything with a bad deletion, so ignore it if we can't
            // find the path it's talking about
            if let Some(path_node) = paths.get(&path) {
                info!("deleting path {:?}", path);
                deletion_futs.push(async {
                    let path_node = path_node.read().await;
                    path_node.delete()
                });
            }
        }
        let mut modification_futs = Vec::new();
        for path_patch in patch.modifications {
            // If we can't find the path a modification is talking about, treat it as a creation
            if let Some(path_node) = paths.get(&path_patch.path) {
                modification_futs.push(async {
                    let path_node = path_node.read().await;
                    let (new_path_node, mut updates_l) = path_node.update(
                        path_patch.path.clone(),
                        path_patch.contents_res,
                        &index_checkers,
                    );
                    updates_l.push(GraphUpdate::ModifyPathNode {
                        // We use the old path in case the new one has changed
                        path: path_patch.path,
                        new_node: new_path_node,
                    });

                    updates_l
                });
            } else {
                debug!(
                    "tried to modify path that didn't exist: {:?}",
                    &path_patch.path
                );

                let (path_node, mut updates_l) =
                    PathNode::new(path_patch.path, path_patch.contents_res, &index_checkers);
                updates_l.push(GraphUpdate::CreatePathNode(path_node));
                creation_updates.push(updates_l);
            }
        }

        // These are both `Vec<Vec<GraphUpdate>>`
        // TODO: If we get deadlocks, we may need to sort these by path so they read in a fixed
        // order
        let (deletion_updates, modification_updates) =
            join(join_all(deletion_futs), join_all(modification_futs)).await;
        // Existing updates are from creations, put everything else first to avoid creating a new
        // ID (this can happen with Vim-style saves)
        let mut updates = deletion_updates;
        updates.extend(modification_updates);
        updates.extend(creation_updates);

        // This doesn't get automatically dropped, so we have to do it manually to avoid a deadlock
        drop(paths);

        self.process_updates(updates.into_iter().flat_map(|v| v.into_iter()))
            .await
    }
    /// Fully processes the given array of renames (where each tuple is a `from` and then `to`
    /// path). This will update the paths map and all the nodes in the renamed paths.
    #[tracing::instrument(skip(self))]
    async fn process_renames(&self, renames: Vec<(PathBuf, PathBuf)>) {
        // Short-circuit if there are no renames to avoid unnecessary locking
        if renames.is_empty() {
            return;
        }

        let mut nodes = self.nodes.write().await;
        let mut indices = self.indices.write_all().await;
        let mut paths = self.paths.write().await;
        debug!("maps locked for renaming");
        for (from, to) in renames {
            // If we can't find the original path, we'll leave this (this is a valid case, see
            // `patch.rs`)
            if let Some(path_node) = paths.remove(&from) {
                // We hold the only reference, writing is guaranteed
                let mut path_node_ref = path_node.try_write().unwrap();
                path_node_ref.rename(to.clone());
                // Make sure all its nodes point to the new path
                for node_id in path_node_ref.ids() {
                    let node_path = nodes.get_mut(node_id).unwrap();
                    *node_path = to.clone();
                    // Including in all the indices
                    for index_map in indices.values_mut() {
                        if let Some(node_path) = index_map.get_mut(node_id) {
                            *node_path = to.clone();
                        }
                    }
                }
                drop(path_node_ref);

                paths.insert(to, path_node);
            }
        }
    }
    /// Processes a series of [`GraphUpdate`]s and modifies the graph accordingly. This will return
    /// a list of paths which need to be updated on the disk and the string contents that should be
    /// written to them.
    ///
    /// *Hint: if there's a deadlock, it's probably happening in here!*
    #[tracing::instrument(skip_all)]
    async fn process_updates(&self, updates: impl Iterator<Item = GraphUpdate>) -> Vec<Write> {
        let mut should_lock_nodes = false;
        let mut should_lock_paths = false;
        let mut should_lock_invalid_connections = false;
        let mut indices_to_lock = HashSet::new();
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
                    debug!("will lock `paths` for path node update");
                }
                GraphUpdate::AddNode { id, ref path } => {
                    // Adding a new node might make some connections that were previously invalid
                    // valid, so we could need the invalid connections map as well (we'll only find
                    // out which nodes we need to lock when we hit this instruction!)
                    should_lock_invalid_connections = true;
                    debug!("will lock `invalid_connections` for new node {id} in {path:?}");

                    // A new node might have had an ID force-created for it during parsing, so
                    // we should write this path back to the disk to ensure ID stability
                    paths_to_write.insert(path.clone());
                    debug!("will write to path {path:?} for new node {id}");
                    // We need to lock that path in order to write to it, and this ID comes
                    // from it, so locking that is sufficient
                    nodes_to_lock.insert(id);
                    debug!("will lock new node {id} in {path:?}");

                    should_lock_nodes = true;
                    debug!("will lock `nodes` for new node {id} in {path:?}");
                    map_updates.push(update);
                }
                GraphUpdate::RemoveNode(_) => {
                    should_lock_nodes = true;
                    map_updates.push(update);
                    debug!("will lock `nodes` for node removal");
                }
                GraphUpdate::AddNodeToIndex { id, ref path, ref index } => {
                    // We'll need to lock the index map to add the node to it
                    indices_to_lock.insert(index.clone());
                    debug!("will lock index {index} to add node {id} in {path:?}");
                    map_updates.push(update);
                }
                GraphUpdate::RemoveNodeFromIndex { id, ref index } => {
                    // We'll need to lock the index map to remove the node from it
                    indices_to_lock.insert(index.clone());
                    debug!("will lock index {index} to remove node {id}");
                    map_updates.push(update);
                }
                GraphUpdate::RemoveInvalidConnection { from, to } => {
                    map_updates.push(update);
                    should_lock_invalid_connections = true;
                    debug!("will lock `invalid_connections` to remove invalid connection from {from} to {to}");
                }

                // Node updates (stage 2)
                GraphUpdate::RemoveBacklink { on, from } => {
                    node_updates.push(update);
                    nodes_to_lock.insert(on);
                    debug!("will lock {on} to remove backlink from {from}")
                }
                GraphUpdate::CheckConnection { from, to } => {
                    node_updates.push(update);
                    // We'll need to read the `from` path node and possibly modify the connection
                    // in it to be valid; also might need to write this whole path to its source if
                    // it's valid (to rewrite titles)
                    nodes_to_lock.insert(from);
                    debug!("will lock {from} to check its connection to {to}");
                    // And we might need to add a backlink to `to`, if it exists
                    nodes_to_lock.insert(to);
                    debug!("will lock {to} to maybe add backlink from {from}");

                    // We also might need to add an invalid connection
                    should_lock_invalid_connections = true;
                    debug!("will lock `invalid_connections` to maybe add invalid connection from {from} to {to}");
                }
            }
        }

        // Lock all the maps we need, in the global locking order
        let mut nodes = OptionFuture::from(should_lock_nodes.then(|| self.nodes.write())).await;
        let mut index_maps = self.indices.write_some(indices_to_lock).await;
        let mut paths = OptionFuture::from(should_lock_paths.then(|| self.paths.write())).await;
        let mut invalid_connections = OptionFuture::from(
            should_lock_invalid_connections.then(|| self.invalid_connections.write()),
        )
        .await;
        if nodes.is_some() {
            debug!("nodes map locked");
        }
        if paths.is_some() {
            debug!("paths map locked");
        }
        if invalid_connections.is_some() {
            debug!("invalid connections map locked");
        }

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
                    if paths.as_ref().unwrap().contains_key(&path_node.path()) {
                        error!("tried to create new path node for {:?} that was already present in graph", path_node.path());
                    }

                    let path = path_node.path();
                    paths
                        .as_mut()
                        .unwrap()
                        .insert(path.clone(), RwLock::new(path_node));
                    debug!("inserted new path node for {:?}", path);
                }
                GraphUpdate::ModifyPathNode { path, new_node } => {
                    if !paths.as_ref().unwrap().contains_key(&path) {
                        error!("tried to modify path node for {path:?} that wasn't in the graph");
                    }

                    let path_node = paths.as_mut().unwrap().get_mut(&path).unwrap();
                    // Fine to blitz the other lock, there can't be any references to it
                    *path_node = RwLock::new(new_node);
                    debug!("updated path node for {path:?}");
                }
                GraphUpdate::DeletePathNode(path) => {
                    // This certainly should still exist, but it's no big deal if it doesn't
                    let removed = paths.as_mut().unwrap().remove(&path);
                    if removed.is_some() {
                        debug!("removed path node for {path:?}");
                    } else {
                        warn!("tried to remove path node for {path:?} that wasn't in the graph");
                    }
                }
                GraphUpdate::AddNode { id, path } => {
                    // BUG: Big problem if this has just been added going to a *different* path...
                    if nodes.as_ref().unwrap().contains_key(&id) {
                        error!("tried to add node {id} in {path:?} that was already present in the graph");
                    }

                    nodes.as_mut().unwrap().insert(id, path.clone());
                    debug!("added new node {id} in {path:?}");

                    // We'll need to add backlinks to all the nodes that referenced this when it
                    // was an invalid connection (if it ever was). This is the only time we add
                    // more nodes to lock or create new instructions (fine because of the node/map
                    // update separation).
                    if let Some(referrers) = invalid_connections.as_mut().unwrap().remove(&id) {
                        nodes_to_lock.insert(id);
                        debug!("will lock {id} to maybe add backlinks for previously invalid connections");

                        for referrer in referrers {
                            // NOTE: This is the only instance where we retroactively add an
                            // update. We replicate perfectly the logic we would have used to
                            // handle it though, including ordering the locking of the appropriate
                            // nodes, so in this case, this violation of the overall paradigm is
                            // acceptable.
                            node_updates.push(GraphUpdate::CheckConnection {
                                from: referrer,
                                to: id,
                            });
                            nodes_to_lock.insert(referrer);
                            debug!("will lock {referrer} to check its previously invalid connection to {id}");
                        }
                    } else {
                        debug!("tried to validate unrecorded invalid connections to {id}");
                    }
                }
                GraphUpdate::RemoveNode(node_id) => {
                    // This certainly should still exist, but it's no big deal if it doesn't
                    let removed = nodes.as_mut().unwrap().remove(&node_id);
                    if removed.is_some() {
                        debug!("removed node {node_id}");
                    } else {
                        debug!("tried to remove node {node_id} that wasn't in the graph");
                    }
                }
                GraphUpdate::AddNodeToIndex { id, path, index } => {
                    let index_map = index_maps.get_mut(&index).unwrap();
                    if index_map.contains_key(&id) {
                        // Unlike adding a general node to the graph, it's no indicator of
                        // something having gone wrong if we try to add something to an index twice
                        debug!("tried to add node {id} in {path:?} to index {index} that was already present in the graph");
                    }

                    index_map.insert(id, path.clone());
                    debug!("added node {id} in {path:?} to index {index}");
                }
                GraphUpdate::RemoveNodeFromIndex { id, index } => {
                    let index_map = index_maps.get_mut(&index).unwrap();
                    let removed = index_map.remove(&id);
                    if removed.is_some() {
                        debug!("removed node {id} from index {index}");
                    } else {
                        debug!(
                            "tried to remove node {id} from index {index} that wasn't in the graph"
                        );
                    }
                }
                GraphUpdate::RemoveInvalidConnection { from, to } => {
                    if let Some(invalid_referrers) =
                        invalid_connections.as_mut().unwrap().get_mut(&from)
                    {
                        let removed = invalid_referrers.remove(&to);
                        if removed {
                            debug!("removed invalid connection from {from} to {to}");
                        } else {
                            debug!("tried to remove invalid connection from {from} to {to} that wasn't in the graph");
                        }
                    } else {
                        debug!("tried to remove unrecorded invalid connection to {to}");
                    }
                }

                _ => unreachable!(),
            }
        }

        // We're guaranteed not to need the indices anymore
        drop(index_maps);
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
        // have a write guard, the map is currently in an invalid state (e.g. bad backlinks), so
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
        if !path_nodes.is_empty() {
            debug!("locked all required paths");
        } else {
            debug!("didn't need to lock any paths");
        }

        // TODO: Would be great if we could downgrade a possible write guard here...

        // We now have everything we need to handle node-level updates
        for update in node_updates {
            match update {
                GraphUpdate::RemoveBacklink { on, from } => {
                    // If the target was deleted in another instruction, this doesn't matter
                    // anymore
                    if let Some(path) = nodes_ref.get(&on) {
                        let path_node = path_nodes.get_mut(path).unwrap();
                        path_node.remove_backlink(on, from);
                        debug!("removed backlink on {on} from {from}");
                    } else {
                        debug!("tried to remove backlink on unknown node {on}");
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
                            debug!("added backlink on {to} from {from}");

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
                                .unwrap()
                                .join("/");

                            // And then validate the connection and update the title of the target
                            let path_node_from = path_nodes.get_mut(path_from).unwrap();
                            path_node_from.validate_connection(from, to, title.clone());
                            debug!("validated connection from {from} to {to} (\"{title}\")");

                            // We've updated a title, which means we need to write the from path
                            // back to the disk (this path is guaranteed already locked)
                            paths_to_write.insert(path_from.clone());
                            debug!("will write to {path_from:?} after possible link title update");
                        } else {
                            // This instruction is used to both add knowingly to the global map,
                            // and to invalidate locally, so do both for good measure
                            let path_node_from = path_nodes.get_mut(path_from).unwrap();
                            path_node_from.invalidate_connection(from, to);
                            invalid_connections
                                .as_mut()
                                .unwrap()
                                .entry(to)
                                .or_insert_with(HashSet::new)
                                .insert(from);
                            debug!("recorded invalid connection from {from} to {to}");
                        }
                    } else {
                        debug!("tried to check connection from unknown node {from}");
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
                let document = path_node.document();
                if let Some(document) = document {
                    let write = Write {
                        path: path.clone(),
                        contents: document.to_document(format).into_string(format),
                        source: WriteSource::Filesystem,
                        // This will be worked out by the conflict detector later
                        conflict: Conflict::None,
                    };
                    debug!("produced filesystem write to {path:?}");
                    Some(write)
                } else {
                    error!("tried to write path {path:?} with no document");
                    None
                }
            })
            .collect()
    }
}
