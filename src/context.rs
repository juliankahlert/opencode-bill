//! Filesystem configuration for locating OpenCode session data.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use directories::BaseDirs;

use crate::database::SqliteStore;
use crate::error::BillError;
use crate::legacy::LegacyStore;
use crate::store::{Session, SessionStore};

/// Name of OpenCode's SQLite session database.
pub(crate) const DATABASE_FILE: &str = "opencode.db";
/// Name of the model catalog file stored in the platform cache.
const MODELS_FILE: &str = "models.json";

/// Explicit filesystem state used to locate OpenCode session data.
pub struct StorageContext {
    pub(crate) data_dir: PathBuf,
    pub(crate) models_file: Option<PathBuf>,
}

impl StorageContext {
    /// Starts a context builder with its required field unset.
    #[must_use]
    pub fn builder() -> StorageContextBuilder<MissingDataDirectory> {
        StorageContextBuilder {
            data_dir: None,
            models_file: None,
            state: PhantomData,
        }
    }

    /// Selects the store matching this data directory's layout.
    pub(crate) fn store(&self) -> Store {
        let database = self.data_dir.join(DATABASE_FILE);

        if database.is_file() {
            return Store::Database(SqliteStore::new(database));
        }

        Store::Legacy(LegacyStore::new(self.data_dir.join("storage")))
    }
}

/// Concrete store selected from the data directory layout.
pub(crate) enum Store {
    /// Sessions in OpenCode's SQLite database.
    Database(SqliteStore),
    /// Sessions in the legacy JSON file layout.
    Legacy(LegacyStore),
}

impl SessionStore for Store {
    fn load(&self, reference: &str) -> Result<Session, BillError> {
        match self {
            Store::Database(store) => store.load(reference),
            Store::Legacy(store) => store.load(reference),
        }
    }
}

/// Marker for a context builder without a data directory.
pub struct MissingDataDirectory;

/// Marker for a context builder with a data directory.
pub struct DataDirectorySet;

/// Builder that requires an OpenCode data directory before construction.
pub struct StorageContextBuilder<State> {
    data_dir: Option<PathBuf>,
    models_file: Option<PathBuf>,
    state: PhantomData<State>,
}

impl StorageContextBuilder<MissingDataDirectory> {
    /// Uses an explicit OpenCode data directory.
    pub fn data_dir(self, path: impl AsRef<Path>) -> StorageContextBuilder<DataDirectorySet> {
        StorageContextBuilder {
            data_dir: Some(path.as_ref().to_path_buf()),
            models_file: self.models_file,
            state: PhantomData,
        }
    }

    /// Uses `$XDG_DATA_HOME/opencode`, or the platform-equivalent location.
    pub fn platform_data_dir(self) -> Result<StorageContextBuilder<DataDirectorySet>, BillError> {
        let base = BaseDirs::new().ok_or(BillError::MissingDataDirectory)?;

        Ok(StorageContextBuilder {
            data_dir: Some(base.data_local_dir().join("opencode")),
            models_file: Some(base.cache_dir().join("opencode").join(MODELS_FILE)),
            state: PhantomData,
        })
    }
}

impl StorageContextBuilder<DataDirectorySet> {
    /// Uses an explicit OpenCode model catalog instead of the platform cache.
    #[must_use]
    pub fn models_file(mut self, path: impl AsRef<Path>) -> Self {
        self.models_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Builds a valid storage context.
    pub fn build(self) -> Result<StorageContext, BillError> {
        let data_dir = self.data_dir.ok_or(BillError::MissingDataDirectory)?;

        if data_dir.as_os_str().is_empty() {
            return Err(BillError::MissingDataDirectory);
        }

        Ok(StorageContext {
            data_dir,
            models_file: self.models_file,
        })
    }
}
