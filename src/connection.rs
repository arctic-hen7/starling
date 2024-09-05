use crate::{
    config::STARLING_CONFIG,
    path_node::{StarlingDocument, StarlingNode},
};
use orgish::Format;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// A connection from one node to another, by the unique ID of the node being connected to.
/// Connections can have *types* to encode metadata, and all have a title, which will be updated to
/// ensure it's valid.
///
/// This type doesn't include the actual identifier, because it's designed to be used in a
/// [`ConnectedString`], which contains an internal map of IDs to these (we avoid double-storing to
/// minimise space use).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Connection {
    /// The "type" of the connection, which is guaranteed to come from a list the user defined in
    /// their config file (anything else will be an error). This can encode arbitrary metadata.
    ///
    /// When written back to a string, this will be fully-qualified.
    ty: String,
    /// The title the user used for the link. This may be out of date relative to the title of the
    /// vertex it points to (if it does point to a vertex), and could need updating.
    ///
    /// This will be used for reconstructing the link, whatever it may be.
    title: String,
}
impl Connection {
    /// Parses a single connection from a string of the form `[title](type:key)` in Markdown, or
    /// `[[type:key][title]]` in Org mode. In these formats, `type` will be one of the types the
    /// user has specified in their configuration, and `key` will be the unique identifier of
    /// another node in the graph. This will return both the ID, as well as the metadata properties
    /// of the title and type.
    ///
    /// This function will return `None` if it is provided either a string which is not a link, or
    /// a link which does not conform to the expected format.
    fn from_str(link: &str, format: Format) -> Option<(Uuid, Self)> {
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
        // vertex; otherwise, it's not a link as far as we're concerned
        let id = Uuid::try_parse(target_str).ok()?;

        Some((
            id,
            Self {
                ty: ty.to_string(),
                title: title.to_string(),
            },
        ))
    }
    /// Converts this connection into a string in the given [`Format`]. This will use whatever the
    /// registered title is for the connection, and will fully-qualify the link type (e.g. the
    /// default will not be elided).
    ///
    /// As [`Connection`] does not include the ID of the node it points to, the ID must be provided
    /// separately.
    fn to_string(&self, id: Uuid, format: Format) -> String {
        match format {
            Format::Markdown => {
                format!("[{}]({}:{})", self.title, self.ty, id)
            }
            Format::Org => format!("[[{}:{}][{}]]", self.ty, id, self.title),
        }
    }
}

/// A token in a string that's parsed with connections: each part can be either a string that does
/// not contain a (valid) link, or a connection.
#[derive(Clone)]
enum ConnectionToken {
    /// A regular string.
    String(String),
    /// A connection, represented by an index into a map of connections and an index (there can be
    /// many connections to the same other node, all distinguished by their types.)
    Connection { id: Uuid, idx: usize },
}

/// A series of connections to a single node.
#[derive(Clone)]
pub struct ParallelConnections {
    /// Whether or not these connections are valid (they all point to the same place, and validity
    /// is unimpacted by the metadata of titles and types).
    valid: bool,
    /// All the different variants, with their own types and titles.
    ///
    /// We store the titles separately in case this connection isn't valid, in which case the
    /// titles shouldn't be blatantly overriden. If the connection is valid, however, these will
    /// all be updated to match the target node's title.
    variants: Vec<Connection>,
}
impl ParallelConnections {
    /// Returns whether or not this set of connections is valid.
    pub fn valid(&self) -> bool {
        self.valid
    }
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.variants.iter().map(|conn_data| conn_data.ty.as_str())
    }
}

pub struct ConnectionRef<'a> {
    id: Uuid,
    valid: bool,
    variants: &'a Vec<Connection>,
}
impl<'a> ConnectionRef<'a> {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn is_valid(&self) -> bool {
        self.valid
    }
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.variants.iter().map(|conn_data| conn_data.ty.as_str())
    }
}
pub struct ConnectionMut<'a> {
    id: Uuid,
    valid: &'a mut bool,
    variants: &'a mut Vec<Connection>,
}
impl<'a> ConnectionMut<'a> {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn is_valid(&self) -> bool {
        *self.valid
    }
    pub fn set_valid(&mut self, valid: bool) {
        *self.valid = valid;
    }
}
pub struct OwnedConnection {
    id: Uuid,
    valid: bool,
    variants: Vec<Connection>,
}
impl OwnedConnection {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn is_valid(&self) -> bool {
        self.valid
    }
}

/// A map of the IDs of nodes being connected to to the details of the connections to those nodes.
/// This characterises all the connections in a string unambiguously.
type ConnectionMap = HashMap<Uuid, ParallelConnections>;

/// A string which contains parsed connections. Connections are indexed by the IDs of the nodes
/// they connect to for efficiency of reference, though the map is held separately to allow the
/// combination of maps for different strings (e.g. the title and body of a node).
#[derive(Clone)]
struct ConnectedString {
    /// A list of raw connection tokens, which can be used to reconstruct the original string.
    inner: Vec<ConnectionToken>,
}
impl ConnectedString {
    /// Parses the provided string into one with connections.
    fn from_str(target: &str, format: Format) -> (Self, ConnectionMap) {
        let mut connections = HashMap::new();
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
                        if let Some((id, conn)) = Connection::from_str(&curr_match, format) {
                            let variants = &mut connections
                                .entry(id)
                                .or_insert(ParallelConnections {
                                    valid: false,
                                    variants: Vec::new(),
                                })
                                .variants;
                            variants.push(conn);
                            tokens.push(ConnectionToken::Connection {
                                id,
                                idx: variants.len() - 1,
                            });
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

        (Self { inner: tokens }, connections)
    }
    /// Converts [`Self`] back into a regular string by stringifying all the connections in it.
    /// This takes in a map for reference.
    fn to_string(&self, connections: &ConnectionMap, format: Format) -> String {
        let mut string = String::new();
        for token in &self.inner {
            match token {
                // This takes a reference anyway, so no real cost to making this take `&self`
                ConnectionToken::String(s) => string.push_str(s),
                ConnectionToken::Connection { id, idx } => {
                    string.push_str(&connections[id].variants[*idx].to_string(*id, format));
                }
            }
        }

        string
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
#[derive(Clone)]
pub struct SingleConnectedNode {
    /// The tokenised title of the node.
    title: ConnectedString,
    /// The tokenised body of the node, if it exists.
    body: Option<ConnectedString>,
    /// The map of connections for both the title and body.
    connections: ConnectionMap,
    /// The position of the [`StarlingNode`] this corresponds to in the tree from which this
    /// [`SingleConnectedNode`] was derived. This is expressed as an array of positions in the chld
    /// vectors of each parent, until this node is reached.
    ///
    /// This allows efficiently accessing the non-connection-based properties of nodes by their
    /// IDs.
    position: Vec<usize>,
    /// A set of the IDs of other nodes which connect to this one. The ID of a node can be used to
    /// get information about its connections to this one in a series of $O(1)$ lookups.
    ///
    /// This will contain an exhaustive list of all the nodes which link to this one, and no
    /// others.
    backlinks: HashSet<Uuid>,
}
impl SingleConnectedNode {
    /// Creates a new [`SingleConnectedNode`] from the given strings for a title and body. This
    /// will start with no backlinks.
    fn new(
        title_str: String,
        body_str: Option<String>,
        position: Vec<usize>,
        format: Format,
    ) -> Self {
        let (title, mut title_map) = ConnectedString::from_str(&title_str, format);
        if let Some(body_str) = body_str {
            let (mut body, body_map) = ConnectedString::from_str(&body_str, format);
            // We're going to put all entries in the body map into the title map, and where there
            // are overlaps, the variants in the body will be *appended* to those from the title,
            // meaning the variant indices among the body tokens should be incremented by however
            // many variants are currently on that entry in the title map. *Then* we can add the
            // actual entries.
            for token in body.inner.iter_mut() {
                if let ConnectionToken::Connection { id, idx } = token {
                    let increment = title_map
                        .get(id)
                        .map(|conn| conn.variants.len())
                        .unwrap_or(0);
                    *idx += increment;
                }
            }
            // Append all the variants of the body map to the title map (there may be overlaps, but
            // equality comparisons on arbitrary-length strings aren't worth the memory savings
            // (probably...))
            for (id, conns) in body_map {
                title_map
                    .entry(id)
                    .or_insert_with(|| ParallelConnections {
                        valid: conns.valid,
                        variants: Vec::new(),
                    })
                    .variants
                    .extend(conns.variants);
            }

            Self {
                title,
                body: Some(body),
                connections: title_map,
                position,
                backlinks: HashSet::new(),
            }
        } else {
            // Simple case, no map combination needed
            Self {
                title,
                body: None,
                connections: title_map,
                position,
                backlinks: HashSet::new(),
            }
        }
    }

    /// Gets an iterator of all the connections in the title and body of this node.
    pub fn connections(&self) -> impl Iterator<Item = ConnectionRef<'_>> {
        self.connections.iter().map(|(id, conn)| ConnectionRef {
            id: *id,
            valid: conn.valid,
            variants: &conn.variants,
        })
    }
    /// Gets an iterator of mutable references to all the connections in the title and body of this
    /// node.
    pub fn connections_mut(&mut self) -> impl Iterator<Item = ConnectionMut<'_>> {
        self.connections.iter_mut().map(|(id, conn)| ConnectionMut {
            id: *id,
            valid: &mut conn.valid,
            variants: &mut conn.variants,
        })
    }
    /// Gets an iterator of the IDs of the nodes which link *to* this node.
    pub fn backlinks(&self) -> impl Iterator<Item = &Uuid> {
        self.backlinks.iter()
    }
    /// Gets the raw map of connections in the title and body of this node.
    pub fn connections_map(&self) -> &ConnectionMap {
        &self.connections
    }
    pub fn position(&self) -> &[usize] {
        &self.position
    }
    pub fn title(&self, format: Format) -> String {
        self.title.to_string(&self.connections, format)
    }
}

/// A [`StarlingNode`] which contains parsed connections in its title and/or body.
#[derive(Clone)]
pub struct ConnectedNode {
    /// The original node from which this connected node was created. To save on memory, the title
    /// and body of this node will be empty strings.
    ///
    /// This will have its children inside it as usual.
    node: StarlingNode,
    /// A map of the UUIDs of nodes in the tree to extracted and tokenised properties.
    map: HashMap<Uuid, SingleConnectedNode>,
}
impl ConnectedNode {
    /// Parses the provided node into a connected node by tokenising its title and body (if
    /// present).
    fn from_node(mut node: StarlingNode, format: Format) -> Self {
        // Parse through all the nodes recursively (recording the positions for later indexing)
        fn tokenise_tree(
            node: &mut StarlingNode,
            format: Format,
            nodes: &mut HashMap<Uuid, SingleConnectedNode>,
            position: Vec<usize>,
        ) {
            // Parse the title and body as connected strings, scrubbing them out of the original
            // `node`
            let connected_node = SingleConnectedNode::new(
                std::mem::take(&mut node.title),
                node.body.take(),
                position.clone(),
                format,
            );
            let id = *node.properties.id;
            nodes.insert(id, connected_node);

            // Perfectly safe, we aren't modifying the levels of any children
            for (idx, child) in node.unchecked_mut_children().iter_mut().enumerate() {
                let mut child_pos = position.clone();
                child_pos.push(idx);
                tokenise_tree(child, format, nodes, child_pos);
            }
        }
        let mut map = HashMap::new();
        tokenise_tree(&mut node, format, &mut map, Vec::new());

        Self { node, map }
    }
    /// Converts [`Self`] back into a regular node by stringifying all the connections in it.
    fn to_node(&self, format: Format) -> StarlingNode {
        // Recursively go through the tree, replacing the title and body of each node with the
        // serialized versions of their respective connected strings
        fn detokenise_tree(
            node: &mut StarlingNode,
            format: Format,
            nodes: &HashMap<Uuid, SingleConnectedNode>,
        ) {
            let id = *node.properties.id;
            let connected_node = nodes.get(&id).unwrap();

            node.title = connected_node
                .title
                .to_string(&connected_node.connections, format);
            node.body = connected_node
                .body
                .as_ref()
                .map(|body| body.to_string(&connected_node.connections, format));

            // Fine to get the children mutably here, we're not changing their levels
            for child in node.unchecked_mut_children() {
                detokenise_tree(child, format, nodes);
            }
        }
        // This clone is acceptable because all string-based properties are empty! We're only
        // cloning metadata.
        let mut node = self.node.clone();
        detokenise_tree(&mut node, format, &self.map);

        node
    }

    /// Returns the node at the root of this [`ConnectedNode`]'s tree. This is gated behind a
    /// method to emphasise that the returned node *will not* have a title or body defined as more
    /// than an empty string and [`None`] respectively.
    pub fn scrubbed_node(&self) -> &StarlingNode {
        &self.node
    }
    /// Returns the details of the node with the given ID in this tree, if it exists.
    pub fn node(&self, uuid: &Uuid) -> Option<&SingleConnectedNode> {
        self.map.get(uuid)
    }
    /// Returns a mutable reference to the details of the node with the given ID in this tree, if
    /// it exists.
    pub fn node_mut(&mut self, uuid: &Uuid) -> Option<&mut SingleConnectedNode> {
        self.map.get_mut(uuid)
    }
    // /// Returns the raw [`StarlingNode`] with the given ID, which will *not* have a title or body.
    // /// This should be used to access node properties only.
    // pub fn raw_node(&self, uuid: &Uuid) -> Option<&StarlingNode> {
    //
    // }
    // /// Returns the stringified title of the node with the given UUID in this [`ConnectedNode`]'s
    // /// tree. This returns [`None`] if there is no node with the given ID in this tree.
    // ///
    // /// This takes a format to determine how connections should be stringified.
    // pub fn title_for_uuid(&self, uuid: Uuid, format: Format) -> Option<String> {
    //     let node = self.map.get(&uuid)?;
    //     Some(node.title.to_string(format))
    // }
    // /// Returns the stringified body of the node with the given UUID in this [`ConnectedNode`]'s
    // /// tree. This returns [`None`] if there is no node with the given ID in this tree. The inner
    // /// [`Option`] will be [`None`] if the node exists, but it doesn't have a body.
    // ///
    // /// This takes a format to determine how connections should be stringified.
    // pub fn body_for_uuid(&self, uuid: Uuid, format: Format) -> Option<Option<String>> {
    //     let node = self.map.get(&uuid)?;
    //     Some(node.body.as_ref().map(|body| body.to_string(format)))
    // }
    // /// Turns this [`ConnectedNode`] into an iterator of the connections in the node's entire tree,
    // /// with the ID of the node in the tree from which each one came.
    // pub fn into_connections(self) -> impl Iterator<Item = (Uuid, Connection)> {
    //     self.map.into_iter().flat_map(|(id, node)| {
    //         let title_connections = node.title.into_connections().map(move |conn| (id, conn));
    //         let body_connections = node
    //             .body
    //             .into_iter()
    //             .flat_map(move |body| body.into_connections().map(move |conn| (id, conn)));
    //         title_connections.chain(body_connections)
    //     })
    // }
    // /// Gets an iterator of all the connections in this node's entire tree, with the ID of the node
    // /// in the tree from which each one came.
    // pub fn connections(&self) -> impl Iterator<Item = (&Uuid, &Connection)> {
    //     self.map.iter().flat_map(|(id, node)| {
    //         let title_connections = node.title.connections().map(move |conn| (id, conn));
    //         let body_connections = node
    //             .body
    //             .as_ref()
    //             .into_iter()
    //             .flat_map(move |body| body.connections().map(move |conn| (id, conn)));
    //         title_connections.chain(body_connections)
    //     })
    // }
    // /// Gets an iterator of mutable references to all the connections in this node's entire tree,
    // /// with the ID of the node in the tree from which each one came.
    // pub fn connections_mut(&mut self) -> impl Iterator<Item = (&Uuid, &mut Connection)> {
    //     self.map.iter_mut().flat_map(|(id, node)| {
    //         let title_connections = node.title.connections_mut().map(move |conn| (id, conn));
    //         let body_connections = node
    //             .body
    //             .as_mut()
    //             .into_iter()
    //             .flat_map(move |body| body.connections_mut().map(move |conn| (id, conn)));
    //         title_connections.chain(body_connections)
    //     })
    // }
}

/// A document which has been parsed for connections from the root down. This stores the
/// attributes, but they are *not* parsed for connections.
#[derive(Clone)]
pub struct ConnectedDocument {
    /// The root node of a connected document.
    ///
    /// In our parsing process, the tags and title of this will be correctly parsed and populated,
    /// but these will be ignored in favour of the raw attributes when serializing back to a string
    /// document.
    pub root: ConnectedNode,
    /// The raw attributes from the original document
    pub attributes: String,
}
impl ConnectedDocument {
    /// Parses the provided document into a connected document by tokenising its title and body (if
    /// present).
    pub fn from_document(document: StarlingDocument, format: Format) -> Self {
        Self {
            root: ConnectedNode::from_node(document.root, format),
            attributes: document.attributes,
        }
    }
    /// Converts [`Self`] back into a regular document by stringifying all the connections in it.
    /// This will clone the attributes directly.
    pub fn to_document(&self, format: Format) -> StarlingDocument {
        StarlingDocument {
            root: self.root.to_node(format),
            attributes: self.attributes.clone(),
        }
    }

    // /// Turns this [`ConnectedDocument`] into an iterator of the connections in the document's
    // /// entire tree, with the ID of the node each one came from.
    // pub fn into_connections(self) -> impl Iterator<Item = (Uuid, Connection)> {
    //     self.root.into_connections()
    // }
    // /// Gets an iterator of all the connections in this document's entire tree, with the ID of the
    // /// node each one came from.
    // pub fn connections(&self) -> impl Iterator<Item = (&Uuid, &Connection)> {
    //     self.root.connections()
    // }
    // /// Gets an iterator of mutable references to all the connections in this document's entire
    // /// tree, with the ID of the node each one came from.
    // pub fn connections_mut(&mut self) -> impl Iterator<Item = (&Uuid, &mut Connection)> {
    //     self.root.connections_mut()
    // }
}
