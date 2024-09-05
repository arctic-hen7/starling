use futures::future::{join, join4, join_all};
use orgish::Format;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    connection::{ConnectedNode, ConnectionRef},
    graph::Graph,
    path_node::StarlingNode,
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
struct NodeConnection {
    /// The other node's unique identifier.
    pub id: Uuid,
    /// The other node's raw title.
    pub title: String,
    /// The types of the connection (one node can connect with another multiple times, this
    /// aggregates all the different types).
    pub types: Vec<String>,
}

impl Graph {
    /// Gets the details of the node with the given ID, if it exists.
    ///
    /// If `backinherit` is `true`, child connections and backlinks will be explored and returned
    /// with the node, otherwise they will be left empty. If it can be set to `false`, this will
    /// improve performance.
    // NOTE: We do this on the graph so we can get all the nodes it's connected to
    pub async fn get_node(
        &self,
        uuid: Uuid,
        backinherit: bool,
        conn_format: Format,
    ) -> Option<Node> {
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
        let raw_node = curr_node;

        // Connections in the root and children are accumulated separately, but we only look at
        // valid ones, which means their titles are the same (so we can put those into a dedicated
        // map)
        let mut connection_title_futs = HashMap::new();
        // These will store the types (that way we can accumulate across many different children)
        let mut connections: HashMap<Uuid, HashSet<String>> = HashMap::new();
        let mut child_connections = HashMap::new();

        // This will add connections to the appropriate map
        let handle_connection =
            |conn: ConnectionRef, conn_map: &mut HashMap<Uuid, HashSet<String>>| {
                if conn.is_valid() {
                    conn_map
                        .entry(conn.id())
                        .or_default()
                        .extend(conn.types().map(|s| s.to_string()));
                    if connection_title_futs.get(&conn.id()).is_none() {
                        let node_path = nodes.get(&conn.id()).unwrap();
                        let path_node = paths.get(node_path).unwrap();

                        connection_title_futs.insert(conn.id(), async {
                            let path_node = path_node.read().await;

                            // We know this will have a document if there's a connection to it
                            let node = path_node.document().unwrap().root.node(&conn.id()).unwrap();
                            node.title(conn_format)
                        });
                    }
                }
            };
        // For backlinks, we have to resolve the node in order to know anything, so we'll map to a
        // bunch of resolvers directly (getting the details of the different connections they have
        // to some of our nodes)
        // TODO:
        let handle_backlink = |backlink_id: &Uuid| {};

        for conn in connected_node.connections() {
            handle_connection(conn, &mut connections);
        }
        // If we're exploring child connections too, accumulate them
        if backinherit {
            fn traverse(
                node: &StarlingNode,
                root: &ConnectedNode,
                child_connections: &mut HashMap<Uuid, HashSet<String>>,
                handle_connection: &mut impl FnMut(ConnectionRef, &mut HashMap<Uuid, HashSet<String>>),
            ) {
                // For each of the children, get its `SingleConnectedNode` by ID, and then handle
                // all the connections in there, before traversing each child. We don't traverse
                // the provided root because that will start as the root for which we've already
                // accumulated connections.
                for child in node.children() {
                    let connected_node = root.node(&child.properties.id).unwrap();
                    for conn in connected_node.connections() {
                        handle_connection(conn, child_connections);
                    }

                    traverse(child, root, child_connections, handle_connection);
                }
            }

            traverse(
                raw_node,
                &document.root,
                &mut child_connections,
                &mut handle_connection,
            );
        }

        // Await getting all the titles
        let connection_titles: HashMap<Uuid, String> = join_all(
            connection_title_futs
                .into_iter()
                .map(|(id, fut)| async { (id, fut.await) }),
        )
        .await
        .into_iter()
        .collect();
        // Then assemble the root and child maps separately
        let assemble_node_conn = |(id, types): (Uuid, HashSet<String>)| {
            (
                id,
                NodeConnection {
                    id,
                    title: connection_titles.get(&id).unwrap().clone(),
                    types: types.into_iter().collect(),
                },
            )
        };
        let connections = connections.into_iter().map(assemble_node_conn).collect();
        let child_connections = child_connections
            .into_iter()
            .map(assemble_node_conn)
            .collect();

        // Collect all the connection references and backlink IDs across this node and its children
        // if requested
        let backlink_to_future = |backlink_id: &Uuid| {
            let node_path = nodes.get(backlink_id).unwrap();
            let path_node = paths.get(node_path).unwrap();

            async move {
                let path_node = path_node.read().await;

                // This is the node that made the connection (guaranteed to exist)
                let node = path_node.document().unwrap().root.node(&uuid).unwrap();
                // We can index to get information about the connection easily!
                let conn_data = node.connections_map().get(backlink_id).unwrap();

                let mut backlink_data = NodeConnection {
                    id: uuid,
                    title: node.title(conn_format),
                    types: conn_data.types().map(|s| s.to_string()).collect(),
                };
                (backlink_data.id, backlink_data)
            }
        };

        let connection_futs = connected_node
            .connections()
            .filter_map(connection_to_future)
            .collect::<Vec<_>>();
        let backlink_futs = connected_node
            .backlinks()
            .map(backlink_to_future)
            .collect::<Vec<_>>();
        // If we're exploring child connections and backlinks too, accumulate them
        let (child_connection_futs, child_backlink_futs) = if backinherit {
            let mut child_connection_refs = Vec::new();
            let mut child_backlink_ids = Vec::new();

            let mut child_connection_futs = Vec::new();
            let mut child_backlink_futs = Vec::new();
            for child in connected_node.children() {
                let child_node = document.root.node(&child).unwrap();
                let child_connection_futs = child_node
                    .connections()
                    .filter_map(connection_to_future)
                    .collect::<Vec<_>>();
                let child_backlink_futs = child_node
                    .backlinks()
                    .map(backlink_to_future)
                    .collect::<Vec<_>>();
                child_connection_futs.extend(child_connection_futs);
                child_backlink_futs.extend(child_backlink_futs);
            }

            // Convert the accumulated connection references and IDs into futures that will unlock
            // those connections
            (
                child_connection_refs
                    .into_iter()
                    .filter_map(connection_to_future)
                    .collect(),
                child_backlink_ids
                    .into_iter()
                    .map(backlink_to_future)
                    .collect(),
            )
        } else {
            (Vec::new(), Vec::new())
        };

        // Resolve all those at the same time
        let (connections, child_connections, backlinks, child_backlinks) = join4(
            join_all(connection_futs),
            join_all(backlink_futs),
            join_all(child_connection_futs),
            join_all(child_backlink_futs),
        )
        .await;

        Some(Node {
            id: uuid,
            title: connected_node.title(conn_format),
            tags: raw_node.tags.iter().cloned().collect(),
            parent_tags,
            connections: HashMap::from_iter(connections),
            child_connections: HashMap::from_iter(child_connections),
            backlinks: HashMap::from_iter(backlinks),
            child_backlinks: HashMap::from_iter(child_backlinks),
        })
    }
}
