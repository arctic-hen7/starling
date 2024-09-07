use orgish::{Format, Timestamp};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use tokio::sync::RwLockReadGuard;
use uuid::Uuid;

use crate::{
    connection::ConnectedNode,
    graph::Graph,
    path_node::{PathNode, StarlingNode},
};

/// A representation of all the information about a single node in the graph.
///
/// The information returned can be regulated with [`NodeOptions`].
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct Node {
    // --- Basics ---
    /// The node's unique identifier.
    pub id: Uuid,
    /// The title of this node.
    pub title: String,
    /// The path this node came from.
    pub path: PathBuf,
    /// The tags on this node itself. There will be no duplicates here.
    pub tags: HashSet<String>,
    /// The tags on this node's parents. There will be no duplicates here.
    pub parent_tags: HashSet<String>,

    // --- Metadata ---
    /// The metadata about the node, if requested.
    pub metadata: Option<NodeMetadata>,

    /// The body of the node, if requested. This may be arbitrarily large.
    ///
    /// If the body is not requested, this will be `None`, but it could also be `None` if the node
    /// has no body. For most uses, `None` can be treated as an empty string (though technically
    /// that is just a blank line, as opposed to the immediate start of the next node).
    pub body: Option<String>,

    /// The unique identifiers of all the *direct* children of this node. Unlike child connections,
    /// this will *not* traverse the entire tree.
    ///
    /// This will only be populated if the children are requested.
    pub children: Vec<Uuid>,

    // --- Connection information ---
    /// Any valid connections this node has directly to other nodes.
    ///
    /// This will only be populated if connection information is requested.
    pub connections: HashMap<Uuid, NodeConnection>,
    /// Valid connections this node's children have to other nodes. These will be combined with
    /// each other. No information about which children different connections come from is
    /// preserved.
    ///
    /// If the requested node and one or more of its children connect to the same node, the
    /// connection will be recorded on the root only (with all the types from the children).
    ///
    /// This will only be populated if both connection and child connection information is
    /// requested.
    pub child_connections: HashMap<Uuid, NodeConnection>,
    /// Connections from other nodes *to* this specific node.
    ///
    /// This will only be populated if connection information is requested.
    pub backlinks: HashMap<Uuid, NodeConnection>,
    /// Connections from other nodes *to* any of the children of this node.
    ///
    /// This will only be populated if both connection and child connection information is
    /// requested.
    pub child_backlinks: HashMap<Uuid, NodeConnection>,
}

/// Metadata about a node. This is a simplification of the representation in a [`StarlingNode`] for
/// transmission.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct NodeMetadata {
    /// The level of this node (0 for a root node) in the hierarchhy of the document it came from.
    /// This is essentially the number of `#`s at the start of the node in Markdown (or `*`s in
    /// Org).
    pub level: u8,
    /// The priority note on this heading, if one was present. These can contain any kind of
    /// string.
    pub priority: Option<String>,
    /// A deadline on this node, if present.
    pub deadline: Option<Timestamp>,
    /// A scheduled timestamp on this node, if present. This is typically used to indicate when an
    /// action item should be started.
    pub scheduled: Option<Timestamp>,
    /// A closed timestamp on this node, if present.
    // TODO: What are these used for??
    pub closed: Option<Timestamp>,
    /// The properties of the node. These are totally freeform.
    pub properties: HashMap<String, String>,
    /// A keyword at the start of the node, which will be one of the ones in the global config if
    /// it's present. These are used to indicate action states, like `TODO` or `NEXT`.
    pub keyword: Option<String>,
    /// Timestamps at the end of the node.
    pub timestamps: Vec<Timestamp>,
}

/// A self-contained representation of a connection with (either to or from) another node. This
/// doesn't include the ID of the other node, just because it's used in maps where that information
/// is known from the key.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct NodeConnection {
    /// The other node's raw title.
    pub title: String,
    /// The types of the connection (one node can connect with another multiple times, this
    /// aggregates all the different types).
    pub types: HashSet<String>,
}

pub struct NodeOptions {
    /// Whether or not to return the body of this node (this may be arbitrarily large).
    pub body: bool,
    /// Whether or not to return metadata about the requested node itself, like schedule
    /// information, and properties. Particularly properties may be arbitrarily large. Note that
    /// tags will always be returned.
    pub metadata: bool,
    /// Whether or not to return the IDs of the direct children of this node.
    pub children: bool,
    /// Whether or not to return connections and backlinks for this node. This doesn't incur
    /// additional computation so much as additional locking, so it should be avoided if it isn't
    /// needed.
    pub connections: bool,
    /// Whether or not to return connections and backlinks in the children. These "logically"
    /// inherit upwards (e.g. if another node connects to a node a child, then it implicitly
    /// connects to the parents too). This incurs quite a bit of extra computation, so should only
    /// be used when necessary.
    ///
    /// If this is `true` and `connections` is false, this will be treated as `false`.
    pub child_connections: bool,
    /// The format links should be serialized to (Markdown or Org).
    pub conn_format: Format,
}
impl NodeOptions {
    pub fn new(format: Format) -> Self {
        Self {
            body: false,
            metadata: false,
            children: false,
            connections: false,
            child_connections: false,
            conn_format: format,
        }
    }
    pub fn body(mut self, v: bool) -> Self {
        self.body = v;
        self
    }
    pub fn metadata(mut self, v: bool) -> Self {
        self.metadata = v;
        self
    }
    pub fn children(mut self, v: bool) -> Self {
        self.children = v;
        self
    }
    pub fn connections(mut self, v: bool) -> Self {
        self.connections = v;
        self
    }
    pub fn child_connections(mut self, v: bool) -> Self {
        self.child_connections = v;
        self
    }
}

impl Graph {
    /// Gets the details of the node with the given ID, if it exists.
    ///
    /// If `backinherit` is `true`, child connections and backlinks will be explored and returned
    /// with the node, otherwise they will be left empty. If it can be set to `false`, this will
    /// improve performance.
    // NOTE: We do this on the graph so we can get all the nodes it's connected to. This involves a
    // considerable degree of read-locking, so deadlocks could occur in here.
    pub async fn get_node(&self, uuid: Uuid, options: NodeOptions) -> Option<Node> {
        // We acquire the nodes before the paths (global lock ordering)
        let nodes = self.nodes.read().await;
        let node_path = nodes.get(&uuid)?;
        let paths = self.paths.read().await;
        let path_node = paths.get(node_path).unwrap();
        let path_node = path_node.read().await;

        // Get the `ConnectedNode` we want, and then use the position that gives to get the
        // `StarlingNode`
        let document = path_node.document()?;
        let connected_node = document.root.node(&uuid)?;
        // Traverse down to get the raw `StarlingNode`, accumulating tags along the way
        let mut parent_tags = HashSet::new();
        let mut curr_node = document.root.scrubbed_node();
        for idx in connected_node.position() {
            parent_tags.extend(curr_node.tags.iter().cloned());
            curr_node = &curr_node.children()[*idx];
        }
        // This is the `StarlingNode` with children and other properties
        let raw_node = curr_node;

        // Collect the direct children if requested
        let mut children = Vec::new();
        if options.children {
            children.extend(raw_node.children().iter().map(|child| *child.properties.id));
        }

        // Collect metadata if requested
        let mut metadata = None;
        if options.metadata {
            metadata = Some(NodeMetadata {
                level: raw_node.level(),
                priority: raw_node.priority.0.clone(),
                deadline: raw_node.planning.deadline.clone(),
                scheduled: raw_node.planning.scheduled.clone(),
                closed: raw_node.planning.closed.clone(),
                properties: (*raw_node.properties).clone(),
                keyword: raw_node.keyword.clone().map(|k| k.keyword),
                timestamps: raw_node.timestamps.clone(),
            });
        }

        // Collection connection information is requested
        let mut connections = HashMap::new();
        let mut backlinks = HashMap::new();
        let mut child_connections = HashMap::new();
        let mut child_backlinks = HashMap::new();
        if options.connections {
            // We'll need to read-lock multiple other paths to get the details of connections and
            // backlinks; those will all go into a map of read guards. We need to lock them in order,
            // so first keep track of them all.
            let mut nodes_to_lock = HashSet::new();

            // We'll need to lock connections in the root
            for conn in connected_node.connections() {
                if conn.is_valid() {
                    nodes_to_lock.insert(conn.id());
                }
            }
            // And backlinks in the root
            for backlink_id in connected_node.backlinks() {
                nodes_to_lock.insert(*backlink_id);
            }
            // And, if we've been requested to go through children, their connections and backlinks too
            if options.child_connections {
                fn traverse(
                    node: &StarlingNode,
                    connected_root: &ConnectedNode,
                    nodes_to_lock: &mut HashSet<Uuid>,
                ) {
                    // For each of the children, get its `SingleConnectedNode` by ID, and then handle
                    // all the connections in there, before traversing each child. We don't traverse
                    // the provided root because that will start as the root for which we've already
                    // accumulated connections.
                    for child in node.children() {
                        let connected_node = connected_root.node(&child.properties.id).unwrap();
                        for conn in connected_node.connections() {
                            if conn.is_valid() {
                                nodes_to_lock.insert(conn.id());
                            }
                        }
                        for backlink_id in connected_node.backlinks() {
                            nodes_to_lock.insert(*backlink_id);
                        }

                        traverse(child, connected_root, nodes_to_lock);
                    }
                }

                traverse(raw_node, &document.root, &mut nodes_to_lock);
            }

            // Resolve the nodes to paths and lock them in the global order (identical to the
            // fine-grained locking we do for updates in `graph.rs`, but with read guards instead)
            let mut paths_to_lock = nodes_to_lock
                .into_iter()
                .filter_map(|id| nodes.get(&id))
                // Ensure there are no duplicates
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            paths_to_lock.sort_unstable();
            let mut path_refs = HashMap::new();
            for path in paths_to_lock {
                // Trying to lock the path we've already locked is a bad idea...
                if path == node_path {
                    continue;
                }

                path_refs.insert(path.clone(), paths.get(path).unwrap().read().await);
            }

            // Now we can go through the connections and backlinks again and we'll have everything we
            // need!
            for conn in connected_node.connections() {
                if conn.is_valid() {
                    let path = nodes.get(&conn.id()).unwrap();
                    let path_node = if path == node_path {
                        &path_node
                    } else {
                        path_refs.get(nodes.get(&conn.id()).unwrap()).unwrap()
                    };
                    // We're guaranteed to have a document, because we have a connection to a node in
                    // there
                    let node = path_node.document().unwrap().root.node(&conn.id()).unwrap();

                    connections.insert(
                        conn.id(),
                        NodeConnection {
                            title: node.title(options.conn_format),
                            types: conn.types().map(|s| s.to_string()).collect(),
                        },
                    );
                }
            }
            for backlink_id in connected_node.backlinks() {
                let path = nodes.get(backlink_id).unwrap();
                let path_node = if path == node_path {
                    &path_node
                } else {
                    path_refs.get(nodes.get(backlink_id).unwrap()).unwrap()
                };
                // We're guaranteed to have a document, because we have a backlink to a node in
                // there
                let node = path_node
                    .document()
                    .unwrap()
                    .root
                    .node(&backlink_id)
                    .unwrap();

                backlinks.insert(
                    *backlink_id,
                    NodeConnection {
                        title: node.title(options.conn_format),
                        // The types of connections the node made to us can be extracted by looking at
                        // the types of the connection to our node
                        types: node
                            .connections_map()
                            .get(&uuid)
                            .unwrap()
                            .types()
                            .map(|s| s.to_string())
                            .collect(),
                    },
                );
            }
            // Now do the same for the children
            if options.child_connections {
                fn traverse(
                    node: &StarlingNode,
                    connected_root: &ConnectedNode,
                    nodes: &HashMap<Uuid, PathBuf>,
                    path_refs: &HashMap<PathBuf, RwLockReadGuard<PathNode>>,
                    child_connections: &mut HashMap<Uuid, NodeConnection>,
                    child_backlinks: &mut HashMap<Uuid, NodeConnection>,
                    node_path: &PathBuf,
                    path_node: &RwLockReadGuard<PathNode>,
                    conn_format: Format,
                ) {
                    // For each of the children, get its `SingleConnectedNode` by ID, and then handle
                    // all the connections in there, before traversing each child. We don't traverse
                    // the provided root because that will start as the root for which we've already
                    // accumulated connections.
                    for child in node.children() {
                        let connected_node = connected_root.node(&child.properties.id).unwrap();
                        for conn in connected_node.connections() {
                            if conn.is_valid() {
                                let path = nodes.get(&conn.id()).unwrap();
                                let path_node = if path == node_path {
                                    path_node
                                } else {
                                    path_refs.get(nodes.get(&conn.id()).unwrap()).unwrap()
                                };
                                // We're guaranteed to have a document, because we have a connection to a node in
                                // there
                                let node =
                                    path_node.document().unwrap().root.node(&conn.id()).unwrap();
                                let types =
                                    conn.types().map(|s| s.to_string()).collect::<HashSet<_>>();

                                // At the root, connections are naturally accumulated as a one-to-many
                                // relation of ID to types, which is the same for each child node, but
                                // we have many of those nodes. We've just accumulated the types for
                                // this one, let's add them to an existing entry (`HashSet`) if there
                                // is one. Note that, because we're dealing with valid connections, the
                                // title will be the same everywhere.
                                child_connections
                                    .entry(conn.id())
                                    .or_insert_with(|| NodeConnection {
                                        title: node.title(conn_format),
                                        types: HashSet::new(),
                                    })
                                    .types
                                    .extend(types);
                            }
                        }
                        for backlink_id in connected_node.backlinks() {
                            let path = nodes.get(backlink_id).unwrap();
                            let path_node = if path == node_path {
                                path_node
                            } else {
                                path_refs.get(nodes.get(backlink_id).unwrap()).unwrap()
                            };
                            // We're guaranteed to have a document, because we have a backlink to a node in
                            // there
                            let node = path_node
                                .document()
                                .unwrap()
                                .root
                                .node(&backlink_id)
                                .unwrap();
                            let types = node
                                .connections_map()
                                .get(&child.properties.id)
                                .unwrap()
                                .types()
                                .map(|s| s.to_string())
                                .collect::<HashSet<_>>();

                            // As with the connections, we might have many backlinks from the same node
                            // to different child nodes, so we'll accumulate all the different types of
                            // references to "the children" as one set (undifferentiated deliberately).
                            child_backlinks
                                .entry(*backlink_id)
                                .or_insert_with(|| NodeConnection {
                                    title: node.title(conn_format),
                                    types: HashSet::new(),
                                })
                                .types
                                .extend(types);
                        }

                        traverse(
                            child,
                            connected_root,
                            nodes,
                            path_refs,
                            child_connections,
                            child_backlinks,
                            node_path,
                            path_node,
                            conn_format,
                        );
                    }
                }

                traverse(
                    raw_node,
                    &document.root,
                    &nodes,
                    &path_refs,
                    &mut child_connections,
                    &mut child_backlinks,
                    node_path,
                    &path_node,
                    options.conn_format,
                );
            }
        }

        // After this, all fine-grained and coarse-grained locks get safely dropped
        Some(Node {
            id: uuid,
            title: connected_node.title(options.conn_format),
            path: node_path.clone(),
            tags: raw_node.tags.iter().cloned().collect(),
            parent_tags,

            metadata,
            body: options
                .body
                .then(|| connected_node.body(options.conn_format))
                .flatten(),
            children,

            connections,
            child_connections,
            backlinks,
            child_backlinks,
        })
    }
}
