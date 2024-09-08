use crate::graph::GraphUpdate;
use crate::{config::STARLING_CONFIG, connection::ConnectedDocument, error::PathParseError};
use orgish::{Document, ForceUuidId, Format, Keyword, Node as OrgishNode};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use uuid::Uuid;

/// A single path in a directory tracked by a Starling instance. This path is an Org/Markdown file,
/// which contains parseable nodes in each of its headings.
pub struct PathNode {
    /// The path where this node was found. Note that this will be a relative path.
    path: PathBuf,
    /// A parsed version of the document found at this path. If an error occurred during parsing,
    /// this might be [`None`].
    document: Option<ConnectedDocument>,
    /// The IDs of all the nodes at this path.
    ///
    /// We use a [`HashSet`] for this so we can easily compare which nodes have been
    /// added/removed when we update a path.
    node_ids: HashSet<Uuid>,

    /// An error that might have occurred while parsing this path. If this is present along with a
    /// `document`, the document will be the last successfully parsed version of the document, and
    /// this will be the latest error occurred on subsequent attempts to parse the document.
    pub error: Option<PathParseError>,
}

impl PathNode {
    /// Creates a new [`PathNode`] from the given path and the result of trying to read its
    /// contents. This returns both the new node and a series of updates to be performed to the
    /// containing graph to account for it.
    pub fn new(
        path: PathBuf,
        contents_res: Result<String, std::io::Error>,
    ) -> (PathNode, Vec<GraphUpdate>) {
        // This is an invalid state (both `None`s), but one will be fixed immediately by
        // `.update()`
        let dummy = Self {
            path: path.clone(),
            document: None,
            node_ids: HashSet::new(),
            error: None,
        };
        let (path_node, updates) = dummy.update(path, contents_res);
        (path_node, updates)
    }
    /// Creates a series of patches for the deletion of this path. This makes no changes to the
    /// actual contents of this path, it just generates the instructions necessary to remove if
    /// entirely from the graph.
    pub fn delete(&self) -> Vec<GraphUpdate> {
        let mut updates = Vec::new();
        if let Some(old_doc) = &self.document {
            // NOTE: This code is an exact duplica of that in `self._update()`.
            for removed_node_id in &self.node_ids {
                // Go through all the connections in this old node and prepare a list of backlinks
                // to be removed for the valid ones, and invalid connection records to be removed
                // for the invalid ones
                let removed_node = old_doc.root.node(removed_node_id).unwrap();
                for conn in removed_node.connections() {
                    if conn.is_valid() {
                        updates.push(GraphUpdate::RemoveBacklink {
                            on: conn.id(),
                            // The backlink is from the removed node, and found on the target of this
                            // connection
                            from: *removed_node_id,
                        })
                    } else {
                        updates.push(GraphUpdate::RemoveInvalidConnection {
                            from: *removed_node_id,
                            to: conn.id(),
                        })
                    }
                }
                // Any connections *to* this node must also be rendered invalid
                for backlink_id in removed_node.backlinks() {
                    updates.push(GraphUpdate::InvalidateConnection {
                        on: *backlink_id,
                        to: *removed_node_id,
                    })
                }

                // And then instruct the removal of the node entirely
                updates.push(GraphUpdate::RemoveNode(*removed_node_id))
            }
        }

        // Regardless of whether there was a document or not, remove the entire path
        updates.push(GraphUpdate::DeletePathNode(self.path()));

        updates
    }
    /// Updates this [`PathNode`] for a change to the path. This takes in both the result of trying
    /// to read the path's new contents, and the actual path itself (which might have changed).
    /// This returns a patch object of all the connections that were removed from the path (both
    /// valid and invalid), so the caller can remove backlinks and noted invalid connections
    /// accordingly. This returns the new [`PathNode`] and a series of updates to the rest of the
    /// graph.
    ///
    /// This expects the given path to have the extension `.org`, `.md`, or `.markdown`.
    pub fn update(
        &self,
        path: PathBuf,
        contents_res: Result<String, std::io::Error>,
    ) -> (PathNode, Vec<GraphUpdate>) {
        let mut new_self = PathNode {
            path: path.clone(),
            node_ids: self.node_ids.clone(),
            document: None,
            error: None,
        };

        match contents_res {
            Ok(contents) => {
                // We're guaranteed to have one of `org`, `md`, or `markdown` as our extensions
                let format = if path.extension().unwrap_or_default() == "org" {
                    Format::Org
                } else {
                    Format::Markdown
                };

                match self._update(&mut new_self, path, contents, format) {
                    Ok(updates) => {
                        new_self.error = None;
                        (new_self, updates)
                    }
                    Err(err) => {
                        new_self.error = Some(err);
                        // Unfortunately, we have to do this
                        new_self.document = self.document.clone();

                        (new_self, Vec::new())
                    }
                }
            }
            Err(err) => {
                // Leave the last state as-is, and just update the error
                new_self.error = Some(PathParseError::ReadFailed { path, err });
                // Unfortunately, we have to do this
                new_self.document = self.document.clone();

                (new_self, Vec::new())
            }
        }
    }
    /// Gets the path for this [`PathNode`].
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
    /// Returns the display title of the node with the given ID in this path, if it exists.
    pub fn display_title(&self, id: Uuid, conn_format: Format) -> Option<String> {
        Some(self.document()?.root.node(&id)?.title(conn_format))
    }
    /// Gets an iterator of the IDs of all the nodes in this path.
    pub fn ids(&self) -> impl Iterator<Item = &Uuid> {
        self.node_ids.iter()
    }
    /// Adds a backlink to the node in this path with the given ID, coming from the other node with
    /// the given ID. If the requested node to which the backlink should be added is not present in
    /// this path, this will do nothing.
    pub fn add_backlink(&mut self, on: Uuid, from: Uuid) {
        if let Some(node) = self
            .document
            .as_mut()
            .and_then(|doc| doc.root.node_mut(&on))
        {
            node.add_backlink(from);
        }
    }
    /// Removes a backlink from the node with the given ID on the node in this path with the
    /// given ID. If the node doesn't exist or the requested backlink isn't present, this will do
    /// nothing.
    pub fn remove_backlink(&mut self, on: Uuid, from: Uuid) {
        if let Some(node) = self
            .document
            .as_mut()
            .and_then(|doc| doc.root.node_mut(&on))
        {
            node.remove_backlink(&from);
        }
    }
    /// Invalidates a connection on the node with the given ID in this path to the node with the
    /// given ID. If the connection doesn't exist or is already invalid, this will do nothing.
    pub fn invalidate_connection(&mut self, on: Uuid, to: Uuid) {
        if let Some(node) = self
            .document
            .as_mut()
            .and_then(|doc| doc.root.node_mut(&on))
        {
            node.invalidate_connection(to);
        }
    }
    /// Renders the connection from the node in this path with the given ID to the other node with
    /// the given ID as valid, and updates its title to be the provided string.
    ///
    /// For clarity, this does not *check* that the connection is valid, it simply sets it as
    /// valid.
    pub fn validate_connection(&mut self, from: Uuid, to: Uuid, to_title: String) {
        if let Some(node) = self
            .document
            .as_mut()
            .and_then(|doc| doc.root.node_mut(&from))
        {
            node.validate_connection(to, to_title);
        }
    }

    /// Gets the root document of this path, if there is one.
    pub fn document(&self) -> Option<&ConnectedDocument> {
        self.document.as_ref()
    }

    /// Internal helper function for updating that returns any errors that occur. This is intended
    /// for ergonomically handling errors that occur in the case where reading was successful.
    fn _update(
        &self,
        new_self: &mut PathNode,
        path: PathBuf,
        contents: String,
        format: Format,
    ) -> Result<Vec<GraphUpdate>, PathParseError> {
        // Parse as a basic document first
        let mut document = StarlingDocument::from_str(&contents, format).map_err(|err| {
            PathParseError::DocumentParseFailed {
                path: path.clone(),
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
                        return Err(PathParseError::FrontmatterNotYaml {
                            path: path.to_path_buf(),
                        });
                    };
                    let frontmatter: MarkdownFrontmatter = serde_yaml::from_str(&attributes)
                        .map_err(|err| PathParseError::InvalidFrontmatter {
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
                        return Err(PathParseError::OrgNoTitle {
                            path: path.to_path_buf(),
                        });
                    }
                    (title.unwrap().to_string(), tags)
                }
            };
        // Resolve `None` to `Vec::new()`
        let tags = tags.unwrap_or_default();

        // Implant the title and tags back into the document so we can parse more easily
        document.root.title = title;
        *document.root.tags = tags;

        // Recursively iterate through the whole document and do the following:
        //  - Find any invalid tags
        //  - Accumulate all IDs
        fn traverse(
            node: &StarlingNode,
            valid_tags: &[String],
            path: PathBuf,
            ids: &mut HashSet<Uuid>,
        ) -> Result<(), PathParseError> {
            // Make sure this ID hasn't been accounted for before in this path (doesn't check
            // against the rest of the graph)
            if !ids.insert(*node.properties.id) {
                return Err(PathParseError::InternalDuplicateId {
                    path,
                    id: *node.properties.id,
                });
            }

            for tag in node.tags.iter() {
                if valid_tags.iter().all(|t| t != tag) {
                    return Err(PathParseError::InvalidTag {
                        path,
                        tag: tag.to_string(),
                    });
                }
            }
            for child in node.children() {
                traverse(child, valid_tags, path.clone(), ids)?;
            }
            Ok(())
        }
        let valid_tags = &STARLING_CONFIG.get().tags;
        let mut node_ids = HashSet::new();
        traverse(&document.root, valid_tags, path.clone(), &mut node_ids)?;

        // Parse connections for the whole document
        let mut connected_doc = ConnectedDocument::from_document(document, format);

        // If we're updating from a previous version of the document, we should transfer connection
        // information over (i.e. retained connections that were originally valid should remain
        // valid), and also determine which vertices (i.e. headings) have been maintained, added,
        // or removed.
        let mut updates = Vec::new();
        if let Some(old_doc) = &self.document {
            // Compare the nodes in this version with those in the old version to instruct graph
            // changes as necessary (those which have stayed the same will be checked in a moment)
            for removed_node_id in self.node_ids.difference(&node_ids) {
                // Go through all the connections in this old node and prepare a list of backlinks
                // to be removed for the valid ones, and invalid connection records to be removed
                // for the invalid ones
                let removed_node = old_doc.root.node(removed_node_id).unwrap();
                for conn in removed_node.connections() {
                    if conn.is_valid() {
                        updates.push(GraphUpdate::RemoveBacklink {
                            on: conn.id(),
                            // The backlink is from the removed node, and found on the target of this
                            // connection
                            from: *removed_node_id,
                        })
                    } else {
                        updates.push(GraphUpdate::RemoveInvalidConnection {
                            from: *removed_node_id,
                            to: conn.id(),
                        })
                    }
                }
                // Any connections *to* this node must also be rendered invalid
                for backlink_id in removed_node.backlinks() {
                    updates.push(GraphUpdate::InvalidateConnection {
                        on: *backlink_id,
                        to: *removed_node_id,
                    })
                }

                // And then instruct the removal of the node entirely
                updates.push(GraphUpdate::RemoveNode(*removed_node_id))
            }
            for new_node_id in node_ids.difference(&self.node_ids) {
                updates.push(GraphUpdate::AddNode {
                    id: *new_node_id,
                    path: path.clone(),
                });
                // Also be sure to validate any previously invalid connections to the ID of our new
                // node
                updates.push(GraphUpdate::ValidateInvalidConnection { to: *new_node_id });
                // We'll need to check all of this node's connections, they're all new (no point in
                // using info from other nodes in this tree to check validity, we'll need to create
                // backlinks anyway)
                for conn in connected_doc.root.node(new_node_id).unwrap().connections() {
                    updates.push(GraphUpdate::CheckConnection {
                        from: *new_node_id,
                        to: conn.id(),
                    })
                }
            }
            for retained_node_id in node_ids.intersection(&self.node_ids) {
                // This node was retained, let's check over the connections to transfer over
                // validities
                let new_node = connected_doc.root.node_mut(retained_node_id).unwrap();
                let old_node = old_doc.root.node(retained_node_id).unwrap();
                // We'll need to remove things from this map to see what's left over at the end, so
                // let's clone it
                let mut old_node_connections = old_node.connections_map().clone();

                for mut new_conn in new_node.connections_mut() {
                    if let Some(valid) = old_node_connections
                        .remove(&new_conn.id())
                        .map(|conn| conn.valid())
                    {
                        // A connection to this target was also in the old node, we can inherit its
                        // validity (if valid, that won't change except by a removal, which would
                        // be processed after this). We'll leave the types and titles as they are
                        // in the new version.
                        if valid {
                            new_conn.set_valid(true);
                        } else {
                            // This was invalid in the previous version, it will only become valid
                            // if it was to a node which was just created, in which case it was
                            // already accounted for in a map for just that, so we can leave this
                            // as-is
                            new_conn.set_valid(false);
                        }
                    } else {
                        // This connection is new, we'll need to validate it explicitly
                        updates.push(GraphUpdate::CheckConnection {
                            from: *retained_node_id,
                            to: new_conn.id(),
                        })
                    }
                }
                // Go through any connections left on the old node
                for (id, raw_conn) in old_node_connections.into_iter() {
                    // If this connection was valid, we should remove the backlink
                    if raw_conn.valid() {
                        updates.push(GraphUpdate::RemoveBacklink {
                            on: id,
                            from: *retained_node_id,
                        })
                    } else {
                        // If it was invalid, we should remove the invalid connection record
                        updates.push(GraphUpdate::RemoveInvalidConnection {
                            from: *retained_node_id,
                            to: id,
                        })
                    }
                }

                // Transfer all the backlinks over (graph updates from other updated nodes will
                // tell us which to heed)
                for backlink_id in old_node.backlinks() {
                    new_node.add_backlink(*backlink_id);
                }

                // Check if the title has been changed (remember this will apply to the root node
                // as well); it doesn't matter which format we use for this
                let old_title = old_node.title(Format::Markdown);
                let new_title = new_node.title(Format::Markdown);
                if old_title != new_title {
                    // The title has changed, we should revalidate all connections from other nodes
                    // to this one (i.e. the backlinks). We don't have all the backlinks that
                    // *will* be present, but we don't need to, because any new ones will see the
                    // new title anyway. We only care about the existing ones, and including ones
                    // that end up invalid is fine, because they'll be invalidated anyway!
                    for backlink_id in new_node.backlinks() {
                        updates.push(GraphUpdate::CheckConnection {
                            from: *backlink_id,
                            to: *retained_node_id,
                        });
                    }
                }
            }
        } else {
            // This is the first version of the document, we'll issue node adding and connection
            // checking instructions for everything (we assume that the `PathNode` has already
            // been added, or that the caller will issue that instruction separately)
            for node_id in &node_ids {
                updates.push(GraphUpdate::AddNode {
                    id: *node_id,
                    path: path.clone(),
                });
                for conn in connected_doc.root.node(node_id).unwrap().connections() {
                    updates.push(GraphUpdate::CheckConnection {
                        from: *node_id,
                        to: conn.id(),
                    })
                }
            }
        }

        // Actually update everything in-place!
        new_self.document = Some(connected_doc);
        new_self.node_ids = node_ids;

        Ok(updates)
    }
}

#[derive(Deserialize)]
struct MarkdownFrontmatter {
    title: String,
    tags: Option<Vec<String>>,
}

/// The Orgish documents used in Starling, based heavily off the global configuration.
pub type StarlingDocument = Document<StarlingKeyword, ForceUuidId>;
/// The Orgish nodes used in Starling, based heavily off the global configuration.
pub type StarlingNode = OrgishNode<StarlingKeyword, ForceUuidId>;

/// A keyword parser for a vertex document that works off the keywords provided in a Starling
/// configuration.
#[derive(Clone)]
pub struct StarlingKeyword {
    pub keyword: String,
}
impl Keyword for StarlingKeyword {
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
