use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur when parsing the configuration for a path tracked by Starling.
#[derive(Error, Debug)]
pub enum ConfigParseError {
    #[error("failed to read config file at '{path:?}'")]
    ReadFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("failed to parse config file at '{path:?}'")]
    ParseFailed {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },
    #[error("found no config file, but failed to write default to '{path:?}'")]
    WriteDefaultFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("cannot have the empty string as a valid link type (this will be handled as the default case automatically)")]
    EmptyLinkType,
}

/// Errors that can occur while parsing a single vertex in isolation.
#[derive(Error, Debug)]
pub enum PathParseError {
    #[error("failed to read vertex at '{path:?}'")]
    ReadFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("failed to parse vertex at '{path:?}' into a document in format '{format:?}'")]
    DocumentParseFailed {
        path: PathBuf,
        format: orgish::Format,
        #[source]
        err: orgish::error::ParseError,
    },
    #[error("found markdown vertex at '{path:?}' with non-yaml frontmatter (not yet supported!)")]
    FrontmatterNotYaml { path: PathBuf },
    #[error("failed to parse frontmatter for markdown vertex at '{path:?}': expected yaml with string `title` and array of strings `tags`")]
    InvalidFrontmatter {
        path: PathBuf,
        #[source]
        err: serde_yaml::Error,
    },
    #[error("failed to parse attributes for org vertex at '{path:?}': no title found")]
    OrgNoTitle { path: PathBuf },
    #[error(
        "found unknown tag '{tag}' in '{path:?}', all tags must be specified in global config"
    )]
    InvalidTag { path: PathBuf, tag: String },
    #[error("the unique id '{id}' appears more than once in '{path:?}'")]
    InternalDuplicateId { path: PathBuf, id: Uuid },
}

/// Errors that can occur while setting up a graph of vertices. These are the errors that would be
/// returned and would prevent the creation of a graph, as opposed to those that would simply
/// become part of the graph.
#[derive(Error, Debug)]
pub enum GraphSetupError {
    #[error("provided domain for graph construction is not a directory: '{path:?}'")]
    DomainNotDir { path: PathBuf },
}

/// Errors that can occur for a vertex while parsing its links and validating them against the rest
/// of the graph.
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("vertex references itself")]
    SelfReference,
    #[error("found reference to invalid vertex id '{bad_uuid}'")]
    InvalidVertexLink { bad_uuid: Uuid },
}

/// An error on a single path, which will be returned to the user.
#[derive(Error, Debug)]
pub enum PathError {
    /// An error in parsing the path itself, represented as the serialized string of the error
    /// message.
    #[error("failed to parse path: {0}")]
    ParseError(String),
    /// An invalid connection in one of the vertices associated with this path. Often, duplicates
    /// of these errors will appear, as a single child vertex with an invalid connection means its
    /// parents will also have that error.
    #[error("found connection to nonexistent vertex with id '{0}'")]
    InvalidConnection(Uuid),
}
