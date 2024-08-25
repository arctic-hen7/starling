use crate::{
    config::STARLING_CONFIG,
    connection::{BackConnection, ConnectedNode, Connection, ConnectionTarget},
    error::VertexParseError,
};
use orgish::{Document, ForceUuidId, Format, Keyword, Node};
use serde::Deserialize;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};
use tokio::fs;
use uuid::Uuid;

#[derive(Debug)]
pub struct Vertex {
    /// The vertex's unique identifier. All documents in Starling are forced to have IDs on every
    /// node, including the root, meaning this is a stable identifier even if the heading is moved.
    /// Users can have their own naming schemes for the paths themselves, but this is the internal
    /// source of truth.
    id: Uuid,
    /// The path to the file from which this vertex comes. This will be canonicalized. As many
    /// vertices can come from a single path, this should be treated as non-unique.
    path: PathBuf,
    /// The human-readable title of the vertex. This is stored so it can be propagated through to
    /// all other vertices that link to this one (keeping link titles up to date).
    ///
    /// This is a vector because a vertex represented by a heading within a document will have the
    /// headings in the tree from the root to it as its title, allowing it to be seen in context
    /// (this makes real-world usage much easier, esp. for fuzzy searching). This is guaranteed to
    /// have at least one element.
    ///
    /// For the root of a document, this will be extracted from the attributes; otherwise it will
    /// be the literal title of a heading.
    title: VecDeque<String>,
    /// The tags of this vertex, which must all be present in the global configuration.
    tags: Vec<String>,
    /// The tags this vertex inherits from its parent(s). These would come from the tags in each
    /// parent node, all the way to the tags on the whole file in the root node.
    parent_tags: Vec<String>,
    /// All the connections going out from *just this* vertex, not including any from its children.
    connections_out: Vec<Connection>,
    /// All the connections going out from the children of this vertex.
    child_connections_out: Vec<Connection>,
    /// Connections from other vertices to this one. This doesn't handle anything about child or
    /// parent vertices, as a given vertex is connected to directly.
    connections_in: Vec<BackConnection>,
}
impl Vertex {
    /// Gets the unique identifier of this vertex.
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Gets the canonicalized path of the file in whcih this vertex was found.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
    /// Sets the path of this vertex to the given [`PathBuf`]. This is a fairly superficial
    /// operation, and can be done without consequences for linked vertices.
    pub fn set_path(&mut self, path: PathBuf) {
        self.path = path;
    }
    /// Gets the full contextualized title of this vertex, including those of all parent vertices
    /// up to the root of the file it was found in.
    pub fn title(&self) -> String {
        self.title.iter().cloned().collect::<Vec<_>>().join("/")
    }
    /// Gets the title of this vertex as an iterator of its parts (starting with the title of the
    /// file root, and ending with the title of the vertex itself). This iterator will always yield
    /// at leat one element.
    pub fn title_parts(&self) -> impl Iterator<Item = &String> {
        self.title.iter()
    }
    /// Gets just the title of this vertex, without the context of any parent vertices it might
    /// have.
    pub fn self_title(&self) -> &str {
        self.title.back().unwrap()
    }
    /// Gets *all* the tags this vertex has, including those inherited from its parent(s).
    pub fn all_tags(&self) -> impl Iterator<Item = &String> {
        self.tags.iter().chain(self.parent_tags.iter())
    }
    /// Gets just the tags this vertex has, without those inherited from its parent(s).
    pub fn self_tags(&self) -> impl Iterator<Item = &String> {
        self.tags.iter()
    }
    /// Gets all the connections to other vertices/resources within this vertex, including those of
    /// any child vertices.
    pub fn connections_out(&self) -> impl Iterator<Item = &Connection> {
        self.connections_out
            .iter()
            .chain(self.child_connections_out.iter())
    }
    /// Gets all the connections to other vertices/resources within this vertex, including those of
    /// any child vertices, as mutable references.
    pub fn connections_out_mut(&mut self) -> impl Iterator<Item = &mut Connection> {
        self.connections_out
            .iter_mut()
            .chain(self.child_connections_out.iter_mut())
    }
    /// Gets all connections into this particualr vertex, not accounting for any to parent or child
    /// vertices.
    pub fn connections_in(&self) -> impl Iterator<Item = &BackConnection> {
        self.connections_in.iter()
    }
    /// Adds the given [`BackConnection`] to this vertex.
    pub fn add_back_connection(&mut self, connection: BackConnection) {
        self.connections_in.push(connection);
    }
    /// Removes the [`BackConnection`] with the given ID (i.e. from the vertex with the given ID)
    /// from this vertex. This will change the state of the graph this vertex fits into, and should
    /// only be called if the vertex with the given ID is being deleted.
    ///
    /// If a back-connection to the vertex with the given ID does not exist, this will do nothing.
    pub fn remove_back_connection(&mut self, connection_id: Uuid) {
        self.connections_in.retain(|c| c.uuid != connection_id);
    }
    /// Invalidates a connection to another vertex. This will change the connection from
    /// [`ConnectionTarget::Vertex`] to [`ConnectionTarget::InvalidVertex`] for both the
    /// connections on this vertex itself, and those on its children. Provided this is called
    /// equally on all children, the nested structure will remain correct.
    ///
    /// If a connection to the vertex with the given ID does not exist, this will do nothing.
    pub fn invalidate_connection(&mut self, connection_id: Uuid) {
        for connection in self.connections_out.iter_mut() {
            if matches!(
                connection.target,
                ConnectionTarget::Vertex(target_id) if target_id == connection_id
            ) {
                connection.target = ConnectionTarget::InvalidVertex(connection_id);
            }
        }
        for connection in self.child_connections_out.iter_mut() {
            if matches!(
                connection.target,
                ConnectionTarget::Vertex(target_id) if target_id == connection_id
            ) {
                connection.target = ConnectionTarget::InvalidVertex(connection_id);
            }
        }
    }
    /// Returns a list of tuples of vertex IDs and their data, all from the vertex file at the
    /// given path, which is expected to be in the given [`Format`]. This will parse the vertex as
    /// a document and extract information like the title and any tags, from every single node in
    /// the document tree (including the root), all of which will become separate vertices.
    ///
    /// The nesting of vertices like this is handled quite literally: a parent vertex contains its
    /// children and all their connections will be listed for it too, while a child vertex contains
    /// no connections from its parent or siblings. Titles are concatenated into a vector, while
    /// tags are treated as inheritable from the parent (meaning a child's tags are a superset of
    /// its parent's).
    pub async fn many_from_file(
        path: &Path,
        format: Format,
    ) -> Result<Vec<Self>, VertexParseError> {
        let contents =
            fs::read_to_string(path)
                .await
                .map_err(|err| VertexParseError::ReadFailed {
                    path: path.to_path_buf(),
                    err,
                })?;
        // Even though we just read, this mmay not work (race conditions etc.)
        let full_path =
            path.canonicalize()
                .map_err(|err| VertexParseError::CanonicalizeFailed {
                    path: path.to_path_buf(),
                    err,
                })?;
        // Parse the contents as a document
        let mut document = VertexDocument::from_str(&contents, format).map_err(|err| {
            VertexParseError::DocumentParseFailed {
                path: path.to_path_buf(),
                format,
                err,
            }
        })?;

        // Parse the format-specific attributes to extract a title and tags for the root
        let (title, tags) =
            match format {
                // TODO: Support more than just YAML?
                Format::Markdown => {
                    let attributes = if document.attributes.starts_with("---")
                        && document.attributes.ends_with("---")
                    {
                        // Remove the frontmatter delimiters
                        document.attributes[3..document.attributes.len() - 3].to_string()
                    } else {
                        return Err(VertexParseError::FrontmatterNotYaml {
                            path: path.to_path_buf(),
                        });
                    };
                    let frontmatter: MarkdownFrontmatter = serde_yaml::from_str(&attributes)
                        .map_err(|err| VertexParseError::InvalidFrontmatter {
                            path: path.to_path_buf(),
                            err,
                        })?;
                    (frontmatter.title, frontmatter.tags)
                }
                Format::Org => {
                    let mut title = None;
                    let mut tags: Option<Vec<String>> = None;
                    for line in document.attributes.lines() {
                        if line.to_lowercase().starts_with("#+title: ") {
                            title = Some(line.splitn(2, ": ").nth(1).unwrap());
                        }
                        if line.to_lowercase().starts_with("#+tags: ")
                            || line.to_lowercase().starts_with("#+filetags: ")
                        {
                            let tags_str = line.splitn(2, ": ").nth(1).unwrap();
                            // Tags can be delimited like `:hello:world:test:` or `hello world test`
                            // or `hello, world, test`. Helpfully, none of the delimiter characters are
                            // allowed within tags, so we can just split on all of them at once and go
                            // from there.
                            tags = Some(
                                tags_str
                                    .split(|c| c == ':' || c == ' ' || c == ',')
                                    .filter(|s| !s.is_empty())
                                    .map(|s| s.to_string())
                                    .collect(),
                            );
                        }
                    }

                    if title.is_none() {
                        return Err(VertexParseError::OrgNoTitle {
                            path: path.to_path_buf(),
                        });
                    }
                    (title.unwrap().to_string(), tags)
                }
            };
        // Resolve `None` to `Vec::new()`
        let tags = tags.unwrap_or_default();

        // Ensure all the tags are valid
        let valid_tags = &STARLING_CONFIG.get().tags;
        for tag in &tags {
            if valid_tags.iter().all(|t| t != tag) {
                return Err(VertexParseError::InvalidTag {
                    path: path.to_path_buf(),
                    tag: tag.to_string(),
                });
            }
        }

        // Implant the title and tags back into the document so we can parse more easily
        document.root.title = title;
        *document.root.tags = tags;

        // Parse connections for the whole document
        let connected_root = ConnectedNode::from_node(document.root, format);
        // Traverse the whole node tree and create a vertex for every node
        fn vertexify_tree(
            node: &VertexNode,
            connected_root: &ConnectedNode,
            full_path: &Path,
        ) -> Vec<Vertex> {
            // Start with the root vertex
            let mut vertices = vec![Vertex {
                id: *node.properties.id,
                path: full_path.to_path_buf(),
                // This will be front-extended by the caller in their recursion
                title: [node.title.clone()].into(),
                tags: (*node.tags).clone(),
                // This will be populated by the caller in their recursion
                parent_tags: Vec::new(),
                // Get the outbound connections for this particular node, but none of its children;
                // we're guaranteed to have the ID of this node present in the connected tree.
                connections_out: connected_root
                    .connections_for_uuid(*node.properties.id)
                    .unwrap()
                    .cloned()
                    .collect(),
                // This will be extended in our recursion
                child_connections_out: Vec::new(),
                connections_in: Vec::new(),
            }];
            // Create vertices for all the children
            for child in node.children() {
                let child_vertex_tree = vertexify_tree(child, connected_root, full_path);
                for mut child_vertex in child_vertex_tree {
                    child_vertex.title.push_front(node.title.clone());
                    child_vertex.parent_tags.extend((*node.tags).clone());
                    // Add the child's connections to the parent (this happens recursively, so will
                    // fill out fully)
                    vertices[0]
                        .child_connections_out
                        .extend(child_vertex.connections_out.iter().cloned());

                    vertices.push(child_vertex);
                }
            }

            vertices
        }
        let vertices = vertexify_tree(&connected_root.node, &connected_root, &full_path);

        Ok(vertices)
    }
}

#[derive(Deserialize)]
struct MarkdownFrontmatter {
    title: String,
    tags: Option<Vec<String>>,
}

/// The Orgish documents used in Starling, based heavily off the global configuration.
pub type VertexDocument = Document<VertexKeyword, ForceUuidId>;
/// The Orgish nodes used in Starling, based heavily off the global configuration.
pub type VertexNode = Node<VertexKeyword, ForceUuidId>;

/// A keyword parser for a vertex document that works off the keywords provided in a Starling
/// configuration.
pub struct VertexKeyword {
    keyword: String,
}
impl Keyword for VertexKeyword {
    fn from_str(keyword: &str) -> Option<Self> {
        let keywords = &STARLING_CONFIG.get().action_keywords;
        if keywords.iter().any(|k| k == keyword) {
            Some(Self {
                keyword: keyword.to_string(),
            })
        } else {
            None
        }
    }
    fn into_string(self) -> String {
        self.keyword
    }
    fn other(keyword: String) -> Self {
        Self { keyword }
    }
}
