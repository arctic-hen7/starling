use crate::error::ConfigParseError;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::atomic::AtomicBool};

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

/// The "ordinary default" path which we'll write to if no config file has been defined yet.
static DEFAULT_PATH: &str = "starling.toml";
/// Default paths that can contain the configuration file.
static TEST_PATHS: [&str; 4] = [
    "starling.toml",
    ".starling.toml",
    "config.toml",
    ".config.toml",
];

/// The user's configuration of Starling. This is instantiated at the very start as a global
/// variable, and is used to manage many components of the overall system.
///
/// Currently, any modifications to the config will require a full restart.
// TODO: Automate that restart
#[derive(Serialize, Deserialize)]
pub struct Config {
    /// The keywords used on action item headings. Typically, these would be something like `TODO`,
    /// `DONE`, `START`, `WAIT`, etc. These determine the state of an action item, and are passed
    /// to API callers. Within Starling itself, no keyword has any particular meaning.
    pub action_keywords: Vec<String>,
    /// The types for links between vertices. These can be used to carry embedded metadata about
    /// the nature of a link from one vertex to another.
    ///
    /// None of these can be the empty string.
    pub link_types: Vec<String>,
    /// The default type of link. This *must* be contained in `link_types`.
    pub default_link_type: String,
    /// All the tags that vertices are allowed to have. This global listing prevents typos.
    // TODO: Regex support?
    pub tags: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            action_keywords: vec!["TODO".to_string(), "DONE".to_string()],
            link_types: vec!["link".to_string()],
            default_link_type: "link".to_string(),
            tags: Vec::new(),
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
                // Create a new configuration and write it to the default path
                let config = Self::default();
                let path = dir.join(DEFAULT_PATH);
                std::fs::write(
                    dir.join(DEFAULT_PATH),
                    // If serializing the default fails, that's a programming error
                    toml::to_string(&config)
                        .expect("failed to serialize default configuration to string"),
                )
                .map_err(|err| ConfigParseError::WriteDefaultFailed {
                    path: path.clone(),
                    err,
                })?;
                Ok(config)
            }
        };

        // Validate the config
        if let Ok(mut config) = config_res {
            if config.link_types.contains(&"".to_string()) {
                return Err(ConfigParseError::EmptyLinkType);
            }

            // The default link type not being accounted for is a soft error, we can automatically
            // correct it
            if !config.link_types.contains(&config.default_link_type) {
                config.link_types.push(config.default_link_type.clone());
            }

            Ok(config)
        } else {
            // This is an error
            config_res
        }
    }
}
