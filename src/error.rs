use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur in the main Starling application.
#[derive(Error, Debug)]
pub enum Error {
    #[error("{path:?} is not a valid directory")]
    InvalidDir { path: PathBuf },
    #[error(transparent)]
    Config(#[from] ConfigParseError),
    #[error("failed to watch directory for changes")]
    NotifyError(#[from] notify::Error),
    #[error("please provide a directory for Starling to track")]
    NoDir,
}

/// Errors that can occur when parsing the configuration for a path tracked by Starling.
#[derive(Error, Debug)]
pub enum ConfigParseError {
    #[error("failed to read config file at {path:?}")]
    ReadFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("failed to parse config file at {path:?}")]
    ParseFailed {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },
    #[error("found no config file, but failed to write default to {path:?}")]
    WriteDefaultFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("cannot have the empty string as a valid link type (this will be handled as the default case automatically)")]
    EmptyLinkType,
    #[error("{path:?} is not a valid directory (please create it)")]
    InvalidLogDir { path: PathBuf },
    #[error("could not retrieve default logging path from operating system, please set `log_directory` manually")]
    NoProjectDirs,
    #[error("failed to create default logging directory, please set `log_directory` manually")]
    CreateDefaultLogDirFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
}

/// Errors that can occur while parsing a single vertex in isolation.
#[derive(Error, Debug)]
pub enum PathParseError {
    #[error("failed to read vertex at {path:?}")]
    ReadFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("failed to parse vertex at {path:?} into a document in format '{format:?}'")]
    DocumentParseFailed {
        path: PathBuf,
        format: orgish::Format,
        #[source]
        err: orgish::error::ParseError,
    },
    #[error("found markdown vertex at {path:?} with non-yaml frontmatter (not yet supported!)")]
    FrontmatterNotYaml { path: PathBuf },
    #[error("failed to parse frontmatter for markdown vertex at {path:?}: expected yaml with string `title` and array of strings `tags`")]
    InvalidFrontmatter {
        path: PathBuf,
        #[source]
        err: serde_yaml::Error,
    },
    #[error("failed to parse attributes for org vertex at {path:?}: no title found")]
    OrgNoTitle { path: PathBuf },
    #[error("found unknown tag '{tag}' in {path:?}, all tags must be specified in global config")]
    InvalidTag { path: PathBuf, tag: String },
    #[error("the unique id '{id}' appears more than once in {path:?}")]
    InternalDuplicateId { path: PathBuf, id: Uuid },
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
