use crate::builder::ImporterBuilder;
use crate::import_args::ImportArgs;
use crate::{SourceType, StoreType};
use anyhow::Context;
use std::path::PathBuf;
use std::str::FromStr;

/// location of the config file, relative to $HOME
const CONFIG_PATH: &str = "/.config/georocket/config.toml";
/// name of the tables in the config file
const STORE_TABLE: &str = "store";
const STORE_TYPE: &str = "type";
const STORE_CONFIG: &str = "config";

const STORE_ENV: &str = "GEOROCKET_STORE";
const STORE_CONFIG_ENV: &str = "GEOROCKET_STORE_CONFIG";

enum FileConfig {
    NotLoaded(PathBuf),
    Loaded(toml::Value),
}

impl FileConfig {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self::NotLoaded(path.into())
    }
    fn init_if_uninit(&mut self) -> anyhow::Result<()> {
        match self {
            FileConfig::NotLoaded(path) => {
                let file = std::fs::read_to_string(&path)
                    .with_context(|| format!("unable to read configuration file at {:?}", path))?;
                let contents = toml::from_str(&file).context("configuration malformed")?;
                *self = Self::Loaded(contents);
                Ok(())
            }
            FileConfig::Loaded(_) => Ok(()),
        }
    }
    fn get_store_type(&mut self) -> anyhow::Result<StoreType> {
        self.init_if_uninit()?;
        match self {
            FileConfig::Loaded(value) => {
                let store_type = value
                    .get(STORE_TABLE)
                    .and_then(|v| v.get(STORE_TYPE))
                    .context("the store was not configured")?;
                let toml::Value::String(store_type) = store_type else {
                    anyhow::bail!(
                        "store should be specified as a string. Received {}: {}",
                        store_type.type_str(),
                        store_type
                    )
                };
                store_type.parse()
            }
            FileConfig::NotLoaded(_) => {
                unreachable!("FileConfig should be initialized")
            }
        }
    }
    fn get_store_config(&mut self) -> anyhow::Result<String> {
        self.init_if_uninit()?;
        match self {
            FileConfig::Loaded(value) => {
                let store_config = {
                    let store_config = value
                        .get(STORE_TABLE)
                        .and_then(|v| v.get(STORE_CONFIG))
                        .context("the store was not configured")?;
                    let toml::Value::String(store_config) = store_config else {
                        anyhow::bail!(
                            "store configuration should be specified as a string. Received {}: {}",
                            store_config.type_str(),
                            store_config
                        )
                    };
                    store_config.clone()
                };
                Ok(store_config)
            }
            FileConfig::NotLoaded(_) => {
                unreachable!("FileConfig should be initialized")
            }
        }
    }
}

/// Final configuration of the import
pub struct ImportConfig {
    source: String,
    source_type: SourceType,
    store_type: StoreType,
    store_config: String,
}

impl ImportConfig {
    /// Attempts to create `ImportConfig` from `import_args`.
    /// Falls back to environment variables and the config file at $HOME/.config/georocket/config.toml
    /// if fields are missing.
    ///
    /// # Errors
    /// This function errors, if:
    /// - there are missing fields which are not specified in `import_args`,
    ///     the environment variables or the config file.
    /// - reading the config file fails
    pub fn from_import_args_with_fallback(import_args: ImportArgs) -> anyhow::Result<ImportConfig> {
        let config_path = {
            let mut path = std::env::vars()
                .find(|var| var.0 == "HOME")
                .context("$HOME not set")?
                .1;
            path.push_str(CONFIG_PATH);
            path
        };
        let mut config = FileConfig::new(config_path);
        let ImportArgs {
            source,
            source_type,
            destination,
            destination_type,
        } = import_args;
        let source_type = match source_type {
            None => unimplemented!("inferring source is not yet implement"),
            Some(source_type) => source_type,
        };
        let store_type = match destination_type {
            None => {
                if let Some(store_type) = store_type_from_env() {
                    store_type?
                } else {
                    config.get_store_type()?
                }
            }
            Some(destination_type) => destination_type,
        };
        let store_config = match destination {
            None => {
                if let Some(store_config) = store_config_from_env() {
                    store_config
                } else {
                    config.get_store_config()?
                }
            }
            Some(destination) => destination,
        };
        Ok(ImportConfig {
            source,
            source_type,
            store_type,
            store_config,
        })
    }
}

fn store_type_from_env() -> Option<anyhow::Result<StoreType>> {
    std::env::vars()
        .find(|var| var.0 == STORE_ENV)
        .map(|store| store.1)
        .map(|store| store.parse::<StoreType>())
}

fn store_config_from_env() -> Option<String> {
    std::env::vars()
        .find(|var| var.0 == STORE_CONFIG_ENV)
        .map(|store_config| store_config.1)
}

impl FromStr for StoreType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgis" => Ok(StoreType::PostGIS),
            "file_store" => Ok(StoreType::FileStore),
            _ => Err(anyhow::anyhow!("unknown store type: {}", s)),
        }
    }
}

impl From<ImportConfig> for ImporterBuilder {
    fn from(config: ImportConfig) -> Self {
        let source = config.source;
        let destination = config.store_config;
        let source_type = config.source_type;
        let store_type = config.store_type;
        ImporterBuilder::new()
            .with_source_config(source)
            .with_source_type(source_type)
            .with_store_config(destination)
            .with_store_type(store_type)
    }
}
