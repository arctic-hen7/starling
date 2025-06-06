use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur in the main Starling application.
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Dir(#[from] DirError),
    #[error(transparent)]
    Config(#[from] ConfigParseError),
    #[error("failed to watch directory for changes")]
    Notify(#[from] notify::Error),
    #[error("please provide a directory for Starling to track")]
    NoDir,
    #[error("failed to bind listener on {host}:{port}")]
    ListenFailed {
        host: String,
        port: u16,
        #[source]
        err: std::io::Error,
    },
    // This could happen at any time (e.g. firewall rules change)
    #[error("server failed")]
    ServeFailed {
        #[source]
        err: std::io::Error,
    },
}

/// Errors to do with parsing the directory in which Starling should run.
#[derive(Error, Debug)]
pub enum DirError {
    #[error("{path:?} is not a valid directory")]
    InvalidDir { path: PathBuf },
    #[error("failed to canonicalize path {path:?}")]
    CanonicalizeFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("path has non-utf8 characters: {path:?}")]
    NonUtf8 { path: PathBuf },
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
