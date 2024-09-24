use crate::error::ConfigParseError;
use directories::ProjectDirs;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::Deserialize;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;

/// The global configutation for a Starling instance. This starts as uninstantiated.
pub static STARLING_CONFIG: GlobalConfig = GlobalConfig::new();

/// A wrapper around the global configuration all parts of the program share. This allows
/// hot-updating.
///
/// **Important:** reading the global configuration should be done *once*, and values obtained
/// through different `.get()` calls should be assumed to be completely different and *totally
/// inconsistent*.
pub struct GlobalConfig {
    /// The inner configuration. Many can read this, and a writer will jump to the front of the
    /// queue when there's a validated change.
    ///
    /// This uses `parking_lot`'s `RwLock` to avoid writer starvation (quite important here!) and
    /// to keep things synchronous (this is used literally everywhere).
    config: RwLock<Option<Config>>,
    /// This is used in testing to make sure we don't double-configure, which can lead to
    /// deadlocks.
    #[cfg(test)]
    pub setup: AtomicBool,
}
impl GlobalConfig {
    /// Creates an uninstantiated global configuration. This *must* be instantiated before being
    /// extracted for the first time.
    pub const fn new() -> Self {
        Self {
            config: RwLock::new(None),
            #[cfg(test)]
            setup: AtomicBool::new(false),
        }
    }
    /// Gets the current state of the global configuration.
    ///
    /// # Panics
    ///
    /// This will panic if the global configuration has not yet been instantiated.
    pub fn get(&self) -> MappedRwLockReadGuard<Config> {
        RwLockReadGuard::map(self.config.read(), |c| c.as_ref().unwrap())
    }
    /// Writes the given configuration to all parts of Starling.
    pub fn set(&self, new_config: Config) {
        #[cfg(test)]
        self.setup.store(true, std::sync::atomic::Ordering::SeqCst);
        *self.config.write() = Some(new_config);
    }
}

/// Default paths that can contain the configuration file.
static TEST_PATHS: [&str; 4] = [
    "starling.toml",
    ".starling.toml",
    "config.toml",
    ".config.toml",
];
// Serde defaults
fn default_action_keywords() -> Vec<String> {
    vec!["TODO".to_string(), "DONE".to_string()]
}
fn default_link_types() -> Vec<String> {
    vec!["link".to_string()]
}
fn default_default_link_type() -> String {
    "link".to_string()
}
fn default_tags() -> Vec<String> {
    Vec::new()
}
fn default_host() -> String {
    "localhost".to_string()
}
fn default_port() -> u16 {
    3000
}
fn default_debounce_duration() -> u64 {
    300
}

/// The user's configuration of Starling. This is instantiated at the very start as a global
/// variable, and is used to manage many components of the overall system.
///
/// Currently, any modifications to the config will require a full restart.
// TODO: Automate that restart
#[derive(Deserialize)]
pub struct Config {
    /// The keywords used on action item headings. Typically, these would be something like `TODO`,
    /// `DONE`, `START`, `WAIT`, etc. These determine the state of an action item, and are passed
    /// to API callers. Within Starling itself, no keyword has any particular meaning.
    #[serde(default = "default_action_keywords")]
    pub action_keywords: Vec<String>,
    /// The types for links between vertices. These can be used to carry embedded metadata about
    /// the nature of a link from one vertex to another.
    ///
    /// None of these can be the empty string.
    #[serde(default = "default_link_types")]
    pub link_types: Vec<String>,
    /// The default type of link. This *must* be contained in `link_types`.
    #[serde(default = "default_default_link_type")]
    pub default_link_type: String,
    /// All the tags that vertices are allowed to have. This global listing prevents typos.
    #[serde(default = "default_tags")]
    pub tags: Vec<String>,
    /// A number of milliseconds to debounce events over. Essentially, updates from the filesystem
    /// need to be watched by Starling to reload the in-memory representation, but sometimes
    /// they'll come very rapidly, so we'll wait until there are no events for this long, and then
    /// we'll process them all in a batch. Very short values may lead to poor performance, and very
    /// long values may lead to poor responsiveness.
    #[serde(default = "default_debounce_duration")]
    pub debounce_duration: u64,
    /// The directory to write rolling daily log files to. Because retrieving the default for this
    /// can fail, this will start as `None` in the default and be set to the default log directory
    /// when instantiated properly.
    pub log_directory: Option<PathBuf>,
    /// The host to serve the Starling server on.
    #[serde(default = "default_host")]
    pub host: String,
    /// The port to serve the Starling server on.
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            action_keywords: default_action_keywords(),
            link_types: default_link_types(),
            default_link_type: default_default_link_type(),
            tags: default_tags(),
            debounce_duration: default_debounce_duration(),
            host: default_host(),
            port: default_port(),
            log_directory: None,
        }
    }
}
impl Config {
    /// Gets a configuration from the given Starling directory (the root of all tracked files in
    /// this instance). This will read from `config.toml`, `.config.toml`, `starling.toml`,
    /// `.starling.toml`, or create a new configuration if none of these files exist.
    pub fn from_dir(dir: &Path) -> Result<Self, ConfigParseError> {
        let config_res = {
            let config_path = TEST_PATHS.iter().map(|p| dir.join(p)).find(|p| p.exists());
            if let Some(path) = config_path {
                // Load the configuration from the file (we use `std::fs` because this happens at
                // program start)
                let contents =
                    std::fs::read_to_string(&path).map_err(|err| ConfigParseError::ReadFailed {
                        path: path.clone(),
                        err,
                    })?;
                let config: Config =
                    toml::from_str(&contents).map_err(|err| ConfigParseError::ParseFailed {
                        path: path.clone(),
                        err,
                    })?;
                Ok(config)
            } else {
                // Create a new configuration (don't bother writing it, that creates more trouble
                // than it's worth and clutters the filesystem if we only want to use this
                // directory once). This will be validated in a moment.
                Ok(Self::default())
            }
        };

        // Validate the config
        if let Ok(mut config) = config_res {
            config.validate()?;

            Ok(config)
        } else {
            // This is an error
            config_res
        }
    }
    /// Validates this configuration, returning an error if it finds an invalid part. This will
    /// also create expensive defaults if needed.
    fn validate(&mut self) -> Result<(), ConfigParseError> {
        if self.link_types.contains(&"".to_string()) {
            return Err(ConfigParseError::EmptyLinkType);
        }

        // The default link type not being accounted for is a soft error, we can automatically
        // correct it
        if !self.link_types.contains(&self.default_link_type) {
            self.link_types.push(self.default_link_type.clone());
        }

        // Validate the logging directory, or set one up if a custom one wasn't provided
        if let Some(log_dir) = &self.log_directory {
            if !log_dir.is_dir() {
                return Err(ConfigParseError::InvalidLogDir {
                    path: log_dir.clone(),
                });
            }
        } else {
            // We need to set up a default logging directory in a reasonable place
            if let Some(proj_dirs) = ProjectDirs::from("org", "starling", "starling") {
                let log_dir = proj_dirs.data_dir().join("logs");
                if !log_dir.exists() {
                    // Not async, but that's okay for a simple setup
                    std::fs::create_dir_all(&log_dir).map_err(|err| {
                        ConfigParseError::CreateDefaultLogDirFailed {
                            path: log_dir.clone(),
                            err,
                        }
                    })?;
                }

                // We don't have logging yet, but the user should know where logs are going
                println!("Logging to: {log_dir:#?}");
                self.log_directory = Some(log_dir);
            } else {
                return Err(ConfigParseError::NoProjectDirs);
            }
        }
        // By now, `self.log_directory` is guaranteed to be `Some(valid_dir)`

        Ok(())
    }
}
