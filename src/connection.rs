use crate::{config::STARLING_CONFIG, vertex::VertexNode};
use orgish::Format;
use std::collections::HashMap;
use uuid::Uuid;

/// A connection from one vertex to another vertex or resource. Connections are unidirectional, and
/// characterised by their type, which can be any of the types the user allows in their Starling
/// config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    /// The target of the connection. This will start as [`ConnectionTarget::Unknown`].
    pub target: ConnectionTarget,
    /// The "type" of the connection, which is guaranteed to come from a list the user defined in
    /// their config file (anything else will be an error). This can encode arbitrary metadata.
    pub ty: String,
    /// The title the user used for the link. This may be out of date relative to the title of the
    /// vertex it points to (if it does point to a vertex), and could need updating.
    pub title: String,
}

/// A connection *to* a particular vertex, storing details about where it came from.
#[derive(Debug, Clone)]
pub struct BackConnection {
    /// The identifier of the other vertex that connected to this one.
    pub uuid: Uuid,
}

/// The target of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionTarget {
    /// A vertex, which has its own connections going out from it.
    Vertex(Uuid),
    /// An invalid vertex; the connection has been made to a UUID, but it doesn't correspond to a
    /// real vertex.
    ///
    /// All connections to UUIDs will start here before becoming [`Self::Vertex`] if they're
    /// demonstrably valid. Note that self-referencing connections will stay permanently invalid.
    InvalidVertex(Uuid),
    /// A resource, which is a black-box end-state for connections (e.g. a PDF).
    Resource(String),
    /// The type of the connection has not yet been determined. This will only be used until the
    /// parsing of the connections in a single vertex has been compared against all other vertices.
    Unknown(String),
}

impl Connection {
    /// Parses a single connection from a string of the form `[title](type:key)` in Markdown, or
    /// `[[type:key][title]]` in Org mode. In these formats, `type` will be one of the types the
    /// user has specified in their configuration, and `key` will be some kind of ID, which will be
    /// resolved as to whether it points to a resource or another vertex once compared with the
    /// overall vertex map.
    ///
    /// This function will return `None` if it is provided either a string which is not a link, or
    /// a link which does not conform to the expected format.
    pub fn from_str(link: &str, format: Format) -> Option<Self> {
        let link = link.trim();

        // Regardless of the format, this will get the title and get the parts of the link
        // around the `:` (if one, a generic link; if two, a typed link)
        let (title, link_parts) = match format {
            // Format: [Title](ty:link)
            Format::Markdown => {
                if !link.starts_with('[') || !link.ends_with(')') || !link.contains("](") {
                    // This indicates something that doesn't even qualify as a link
                    return None;
                }

                // Parse the link into its parts
                let mut link_parts = link.splitn(2, "](");
                let title = link_parts.next().unwrap();
                let title = title.strip_prefix('[').unwrap();
                let link = link_parts.next().unwrap();
                let link = link.strip_suffix(')').unwrap();

                (title, link.splitn(2, ':').collect::<Vec<_>>())
            }
            // Format: [[ty:link][Title]]
            Format::Org => {
                if !link.starts_with("[[") || !link.ends_with("]]") || !link.contains("][") {
                    return None;
                }

                // Parse the link into its parts (ignoring the title)
                let mut link_parts = link.splitn(2, "][");
                let link = link_parts.next().unwrap();
                let link = link.strip_prefix("[[").unwrap();
                let title = link_parts.next()?;
                let title = title.strip_suffix("]]").unwrap();

                (title, link.splitn(2, ':').collect::<Vec<_>>())
            }
        };

        let config = STARLING_CONFIG.get();
        let (target_str, ty) = if link_parts.len() == 2 {
            // We have two parts, parse the first one as a link type
            if config.link_types.iter().any(|t| t == link_parts[0]) {
                (link_parts[1], link_parts[0])
            } else {
                // This is not a valid link type
                // TODO: URL links trigger this path, what should we do with resources?
                return None;
            }
        } else {
            // We only have one part, which means we have a generic link
            (link_parts[0], config.default_link_type.as_str())
        };

        // Try to parse the target as a UUID, if we can, then it's an attempt to link to another
        // vertex
        let target = if let Ok(uuid) = Uuid::try_parse(target_str) {
            // We'll start as invalid, and progress to valid if we can
            ConnectionTarget::InvalidVertex(uuid)
        } else {
            // TODO: Resource IDs
            ConnectionTarget::Unknown(target_str.to_string())
        };

        Some(Self {
            target,
            ty: ty.to_string(),
            title: title.to_string(),
        })
    }
}
// impl Connection {
//     /// Converts this connection back into a string in the given format, adding the saved key
//     /// prefix. This will return `None` if the target of this connection no longer exists, or if
//     /// the connection never had an in-memory link established.
//     pub fn to_string(&self, format: Format) -> Option<String> {
//         let title = self.target.upgrade()?.read().ok()?.title();
//
//         Some(match format {
//             Format::Markdown => format!(
//                 "[{}]({}:{}{})",
//                 title,
//                 self.ty.to_string(),
//                 self.key_prefix,
//                 self.target_id
//             ),
//             Format::Org => format!(
//                 "[[{}][{}:{}{}]]",
//                 title,
//                 self.ty.to_string(),
//                 self.key_prefix,
//                 self.target_id
//             ),
//         })
//     }
// }

/// A token in a string that's parsed with connections: each part can be either a string that does
/// not contain a (valid) link, or a connection.
enum ConnectionToken {
    String(String),
    Connection(Connection),
}

/// A string which contains parsed connections.
pub struct ConnectedString {
    inner: Vec<ConnectionToken>,
}
impl ConnectedString {
    /// Parses the provided string into one with connections.
    pub fn from_str(target: &str, format: Format) -> Self {
        // Go through the string contents manually to find links (format-specific)
        let mut tokens = Vec::new();
        let mut chars = target.chars().peekable();
        let mut conn_loc = ConnectionLoc::None;
        // This will store a full link (including delimiters) so we can use string
        // replacement on it later if needed
        let mut curr_match = String::new();
        // This will store the current string in between links
        let mut curr_str = String::new();
        while let Some(c) = chars.next() {
            match conn_loc {
                ConnectionLoc::None => {
                    match format {
                        Format::Markdown => {
                            if c == '[' {
                                // We have the start of some kind of link
                                conn_loc = ConnectionLoc::Title;
                                curr_match.push(c);

                                tokens.push(ConnectionToken::String(curr_str));
                                curr_str = String::new();
                            } else {
                                curr_str.push(c);
                            }
                        }
                        // In Org, we have `[[][]]` syntax, so we'll parse the first *two*
                        // brackets
                        Format::Org => {
                            if c == '[' && chars.peek().is_some_and(|next_c| *next_c == '[') {
                                // We have the start of some kind of link
                                conn_loc = ConnectionLoc::Title;
                                curr_match.push(c);
                                curr_match.push(chars.next().unwrap());

                                tokens.push(ConnectionToken::String(curr_str));
                                curr_str = String::new();
                            } else {
                                curr_str.push(c);
                            }
                        }
                    }
                }
                // Inside a title, we'll look only for the delimiter before the link target
                // (but we'll store the title itself so we have it for later string
                // replacement if needed)
                ConnectionLoc::Title => {
                    if (format == Format::Markdown
                        && c == ']'
                        && chars.peek().is_some_and(|next_c| *next_c == '('))
                        || (format == Format::Org
                            && c == ']'
                            && chars.peek().is_some_and(|next_c| *next_c == '['))
                    {
                        // We have the end of a title inside a link
                        conn_loc = ConnectionLoc::Target;
                        // Push both delimiters to get straight onto the target
                        curr_match.push(c);
                        curr_match.push(chars.next().unwrap());
                    } else {
                        curr_match.push(c);
                    }
                }
                // Inside a link target, we'll just wait for the end. Again, for Org we
                // parse both brackets.
                ConnectionLoc::Target => {
                    if (format == Format::Markdown && c == ')')
                        || (format == Format::Org
                            && c == ']'
                            && chars.peek().is_some_and(|next_c| *next_c == ']'))
                    {
                        // We have the end of a link entirely
                        conn_loc = ConnectionLoc::None;
                        curr_match.push(c);
                        if format == Format::Org {
                            curr_match.push(chars.next().unwrap());
                        }

                        // We have a full connection, parse it
                        if let Some(conn) = Connection::from_str(&curr_match, format) {
                            tokens.push(ConnectionToken::Connection(conn));
                        } else {
                            // This isn't actually a connection, add it as a string
                            tokens.push(ConnectionToken::String(curr_match));
                        }
                        curr_match = String::new();
                    } else {
                        curr_match.push(c);
                    }
                }
            }
        }

        // If we've got an extant string, add it to the tokens
        if !curr_str.is_empty() {
            tokens.push(ConnectionToken::String(curr_str));
        }
        // If we've got an extant match, that means it was never finished; add it as a string to
        // the tokens (we don't have to worry about order with `curr_str`, as only one will be
        // populated at a time)
        if !curr_match.is_empty() {
            tokens.push(ConnectionToken::String(curr_match));
        }

        Self { inner: tokens }
    }
    // /// Converts [`Self`] back into a regular string by stringifying all the connections in it.
    // /// This will fail with a dangling connection error if a connection with a target that no
    // /// longer exists is found.
    // pub fn to_string(&self, format: Format) -> Result<String, DanglingConnectionError> {
    //     let mut strings = Vec::new();
    //     for token in &self.inner {
    //         match token {
    //             ConnectionToken::String(s) => strings.push(s.clone()),
    //             ConnectionToken::Connection(conn) => {
    //                 let string = conn
    //                     .to_string(format)
    //                     .ok_or(DanglingConnectionError(conn.target_id().to_string()))?;
    //                 strings.push(string);
    //             }
    //         }
    //     }
    //
    //     Ok(strings.join(""))
    // }

    /// Turns this [`ConnectedString`] into an iterator of the connections in the string.
    pub fn into_connections(self) -> impl Iterator<Item = Connection> {
        self.inner.into_iter().filter_map(|token| match token {
            ConnectionToken::Connection(conn) => Some(conn),
            ConnectionToken::String(_) => None,
        })
    }
    /// Gets an iterator of all the connections in this string.
    pub fn connections(&self) -> impl Iterator<Item = &Connection> {
        self.inner.iter().filter_map(|token| match token {
            ConnectionToken::Connection(conn) => Some(conn),
            ConnectionToken::String(_) => None,
        })
    }
    /// Gets an iterator of mutable references to all the connections in this string.
    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut Connection> {
        self.inner.iter_mut().filter_map(|token| match token {
            ConnectionToken::Connection(conn) => Some(conn),
            ConnectionToken::String(_) => None,
        })
    }
}
/// The parser's position while parsing a connection.
#[derive(PartialEq, Eq)]
enum ConnectionLoc {
    Title,
    Target,
    None,
}

/// The properties of a single connected node within a [`ConnectedNode`] tree.
struct SingleConnectedNode {
    /// The tokenised title of the node.
    title: ConnectedString,
    /// The tokenised body of the node, if it exists.
    body: Option<ConnectedString>,
}

/// A [`VertexNode`] which contains parsed connections in its title and/or body.
pub struct ConnectedNode {
    /// The original node from which this connected node was created.
    pub node: VertexNode,
    /// A map of the UUIDs of nodes in the tree to extracted and tokenised properties.
    map: HashMap<Uuid, SingleConnectedNode>,
}
impl ConnectedNode {
    /// Parses the provided node into a connected node by tokenising its title and body (if
    /// present).
    pub fn from_node(node: VertexNode, format: Format) -> Self {
        // Parse through all the nodes recursively
        fn tokenise_tree(
            node: &VertexNode,
            format: Format,
            nodes: &mut HashMap<Uuid, SingleConnectedNode>,
        ) {
            let connected_node = SingleConnectedNode {
                title: ConnectedString::from_str(&node.title, format),
                body: node
                    .body
                    .as_ref()
                    .map(|body| ConnectedString::from_str(body, format)),
            };
            let id = *node.properties.id;
            nodes.insert(id, connected_node);

            for child in node.children() {
                tokenise_tree(child, format, nodes);
            }
        }
        let mut map = HashMap::new();
        tokenise_tree(&node, format, &mut map);

        Self { node, map }
    }
    // /// Converts [`Self`] back into a regular node by stringifying all the connections in it.
    // ///
    // /// This will update the internal node with any new connection titles.
    // pub fn to_node(&self, format: Format) -> Result<VertexNode, DanglingConnectionError> {
    //     // Recursively go through the tree, replacing the title and body of each node with the
    //     fn detokenise_tree(
    //         node: &mut VertexNode,
    //         format: Format,
    //         nodes: &HashMap<Uuid, SingleConnectedNode>,
    //     ) -> Result<(), DanglingConnectionError> {
    //         let id = *node.properties.id;
    //         let connected_node = nodes.get(&id).unwrap();
    //
    //         node.title = connected_node.title.to_string(format)?;
    //         if let Some(body) = node.body.as_mut() {
    //             *body = connected_node.body.as_ref().unwrap().to_string(format)?;
    //         }
    //
    //         // Fine to get the children mutably here, we're not changing their levels
    //         for child in node.unchecked_mut_children() {
    //             detokenise_tree(child, format, nodes);
    //         }
    //
    //         Ok(())
    //     }
    //     // We clone so we don't have to take `&mut self`, which would make it impossible to save a
    //     // box depending on this function, because loaders only get `&self`!
    //     // PERF: Could we have a method to clone without strings?
    //     let mut node = self.node.clone();
    //     detokenise_tree(&mut node, format, &self.map)?;
    //
    //     Ok(node)
    // }

    /// Returns an iterator of connections in the body and title of the node in this
    /// [`ConnectedNode`]'s tree with the given UUID. This returns [`None`] if there is nonode with
    /// the given ID in this tree.
    pub fn connections_for_uuid(&self, uuid: Uuid) -> Option<impl Iterator<Item = &Connection>> {
        let node = self.map.get(&uuid)?;
        Some(
            node.title.connections().chain(
                node.body
                    .as_ref()
                    .into_iter()
                    .flat_map(|body| body.connections()),
            ),
        )
    }
    /// Turns this [`ConnectedNode`] into an iterator of the connections in the node's entire tree.
    pub fn into_connections(self) -> impl Iterator<Item = Connection> {
        self.map.into_values().flat_map(|node| {
            let title_connections = node.title.into_connections();
            let body_connections = node
                .body
                .into_iter()
                .flat_map(|body| body.into_connections());
            title_connections.chain(body_connections)
        })
    }
    /// Gets an iterator of all the connections in this node's entire tree.
    pub fn connections(&self) -> impl Iterator<Item = &Connection> {
        self.map.values().flat_map(|node| {
            let title_connections = node.title.connections();
            let body_connections = node
                .body
                .as_ref()
                .into_iter()
                .flat_map(|body| body.connections());
            title_connections.chain(body_connections)
        })
    }
    /// Gets an iterator of mutable references to all the connections in this node's entire tree.
    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut Connection> {
        self.map.values_mut().flat_map(|node| {
            let title_connections = node.title.connections_mut();
            let body_connections = node
                .body
                .as_mut()
                .into_iter()
                .flat_map(|body| body.connections_mut());
            title_connections.chain(body_connections)
        })
    }
}

/// A map that maps idea and goal references to their titles. This allows writing connections to
/// strings.
///
/// This *can* also handle citations, but, as these come from a different box, it may not always.
pub struct ConnectionMap<'a> {
    pub ideas: HashMap<&'a String, &'a String>,
    pub goals: HashMap<&'a String, &'a String>,
    pub citations: Option<HashMap<&'a String, &'a String>>,
}
