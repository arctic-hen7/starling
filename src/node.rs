use futures::future::{join, join4, join_all};
use orgish::Format;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use tokio::sync::RwLockReadGuard;
use uuid::Uuid;

use crate::{
    connection::{ConnectedNode, ConnectionRef},
    graph::Graph,
    path_node::{PathNode, StarlingNode},
};

/// A representation of all the information about a single node in the graph.
#[derive(Serialize)]
pub struct Node {
    /// The node's unique identifier.
    pub id: Uuid,
    /// The title of this node.
    pub title: String,
    /// The tags on this node itself. There will be no duplicates here.
    pub tags: HashSet<String>,
    /// The tags on this node's parents. There will be no duplicates here.
    pub parent_tags: HashSet<String>,

    /// Any valid connections this node has directly to other nodes.
    pub connections: HashMap<Uuid, NodeConnection>,
    /// Valid connections this node's children have to other nodes. These will be combined with
    /// each other. No information about which children different connections come from is
    /// preserved.
    ///
    /// If the requested node and one or more of its children connect to the same node, the
    /// connection will be recorded on the root only (with all the types from the children).
    pub child_connections: HashMap<Uuid, NodeConnection>,
    /// Connections from other nodes *to* this specific node.
    pub backlinks: HashMap<Uuid, NodeConnection>,
    /// Connections from other nodes *to* any of the children of this node.
    pub child_backlinks: HashMap<Uuid, NodeConnection>,
}

/// A self-contained representation of a connection with (either to or from) another node.
#[derive(Serialize)]
pub struct NodeConnection {
    /// The other node's unique identifier.
    pub id: Uuid,
    /// The other node's raw title.
    pub title: String,
    /// The types of the connection (one node can connect with another multiple times, this
    /// aggregates all the different types).
    pub types: HashSet<String>,
}

impl Graph {
    /// Gets the details of the node with the given ID, if it exists.
    ///
    /// If `backinherit` is `true`, child connections and backlinks will be explored and returned
    /// with the node, otherwise they will be left empty. If it can be set to `false`, this will
    /// improve performance.
    // NOTE: We do this on the graph so we can get all the nodes it's connected to. This involves a
    // considerable degree of read-locking, so deadlocks could occur in here.
    pub async fn get_node(
        &self,
        uuid: Uuid,
        backinherit: bool,
        conn_format: Format,
    ) -> Option<Node> {
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
        // Traverse down to get the raw `StarlingNode`, accumulating tags along the way.
        let mut parent_tags = HashSet::new();
        let mut curr_node = document.root.scrubbed_node();
        parent_tags.extend(curr_node.tags.iter().cloned());
        for idx in connected_node.position() {
            curr_node = &curr_node.children()[*idx];
            parent_tags.extend(curr_node.tags.iter().cloned());
        }
        // This is the `StarlingNode` with children and other properties
        let raw_node = curr_node;

        // We'll need to read-lock multiple other paths to get the details of connections and
        // backlinks; those will all go into a map of read guards (including the one we've already
        // got!!). We need to lock them in order, so first keep track of them all.
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
        if backinherit {
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
            .map(|id| nodes.get(&id).unwrap())
            .collect::<Vec<_>>();
        paths_to_lock.sort_unstable();
        let mut path_refs = HashMap::new();
        for path in paths_to_lock {
            path_refs.insert(path.clone(), paths.get(path).unwrap().read().await);
        }

        // Now we can go through the connections and backlinks again and we'll have everything we
        // need!
        let mut connections = HashMap::new();
        let mut backlinks = HashMap::new();
        for conn in connected_node.connections() {
            if conn.is_valid() {
                let path_node = path_refs.get(nodes.get(&conn.id()).unwrap()).unwrap();
                // We're guaranteed to have a document, because we have a connection to a node in
                // there
                let node = path_node.document().unwrap().root.node(&conn.id()).unwrap();

                connections.insert(
                    conn.id(),
                    NodeConnection {
                        id: conn.id(),
                        title: node.title(conn_format),
                        types: conn.types().map(|s| s.to_string()).collect(),
                    },
                );
            }
        }
        for backlink_id in connected_node.backlinks() {
            let path_node = path_refs.get(nodes.get(backlink_id).unwrap()).unwrap();
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
                    // ID of the node that made the connection and its title
                    id: *backlink_id,
                    title: node.title(conn_format),
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
        let mut child_connections = HashMap::new();
        let mut child_backlinks = HashMap::new();
        if backinherit {
            fn traverse(
                node: &StarlingNode,
                connected_root: &ConnectedNode,
                nodes: &HashMap<Uuid, PathBuf>,
                path_refs: &HashMap<PathBuf, RwLockReadGuard<PathNode>>,
                child_connections: &mut HashMap<Uuid, NodeConnection>,
                child_backlinks: &mut HashMap<Uuid, NodeConnection>,
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
                            let path_node = path_refs.get(nodes.get(&conn.id()).unwrap()).unwrap();
                            // We're guaranteed to have a document, because we have a connection to a node in
                            // there
                            let node = path_node.document().unwrap().root.node(&conn.id()).unwrap();
                            let types = conn.types().map(|s| s.to_string()).collect::<HashSet<_>>();

                            // At the root, connections are naturally accumulated as a one-to-many
                            // relation of ID to types, which is the same for each child node, but
                            // we have many of those nodes. We've just accumulated the types for
                            // this one, let's add them to an existing entry (`HashSet`) if there
                            // is one. Note that, because we're dealing with valid connections, the
                            // title will be the same everywhere.
                            child_connections
                                .entry(conn.id())
                                .or_insert_with(|| NodeConnection {
                                    id: conn.id(),
                                    title: node.title(conn_format),
                                    types: HashSet::new(),
                                })
                                .types
                                .extend(types);
                        }
                    }
                    for backlink_id in connected_node.backlinks() {
                        let path_node = path_refs.get(nodes.get(backlink_id).unwrap()).unwrap();
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
                                // ID of the node that made the connection and its title
                                id: *backlink_id,
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
                conn_format,
            );
        }

        // After this, all fine-grained and coarse-grained locks get safely dropped
        Some(Node {
            id: uuid,
            title: connected_node.title(conn_format),
            tags: raw_node.tags.iter().cloned().collect(),
            parent_tags,
            connections,
            child_connections,
            backlinks,
            child_backlinks,
        })
    }
}
