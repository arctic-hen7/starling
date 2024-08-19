use std::path::PathBuf;

use thiserror::Error;

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

#[derive(Error, Debug)]
pub enum VertexParseError {
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
    #[error("failed to canonicalize path for vertex at '{path:?}'")]
    CanonicalizeFailed {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("found markdown vertex with non-yaml frontmatter (not yet supported!)")]
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
}
