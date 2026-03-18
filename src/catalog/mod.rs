use std::collections::BTreeMap;

use parking_lot::RwLock;
use thiserror::Error;

use crate::mvcc::{MvccStore, TransactionError};

use self::table::{TableDescriptor, TableValidationError};

pub mod column;
pub mod schema;
pub mod table;

const TABLE_PREFIX: &str = "__catalog__/tables/";

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("table name cannot be empty")]
    EmptyTableName,
    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),
    #[error("table '{0}' was not found")]
    TableNotFound(String),
    #[error("invalid table descriptor: {0}")]
    Validation(#[from] TableValidationError),
    #[error("mvcc transaction failed: {0}")]
    Transaction(#[from] TransactionError),
    #[error("failed to serialize table descriptor: {0}")]
    Serialization(String),
    #[error("failed to deserialize table descriptor at key '{key}': {message}")]
    Deserialization { key: String, message: String },
}

#[derive(Debug)]
pub struct Catalog {
    store: MvccStore,
    tables: RwLock<BTreeMap<String, TableDescriptor>>,
}

impl Catalog {
    pub fn open(store: MvccStore) -> Result<Self, CatalogError> {
        let catalog = Self { store, tables: RwLock::new(BTreeMap::new()) };
        catalog.refresh()?;
        Ok(catalog)
    }

    pub fn refresh(&self) -> Result<(), CatalogError> {
        let loaded = self.load_tables_from_store()?;
        *self.tables.write() = loaded;
        Ok(())
    }

    pub fn create_table(&self, descriptor: TableDescriptor) -> Result<(), CatalogError> {
        let mut descriptor = descriptor;
        let table_name = normalize_table_name(&descriptor.name)?.to_string();
        descriptor.name = table_name.clone();
        descriptor.validate()?;
        let key = table_storage_key(&table_name);
        let payload = bincode::serialize(&descriptor)
            .map_err(|err| CatalogError::Serialization(err.to_string()))?;

        let mut tx = self.store.begin_transaction();
        if tx.get(&key)?.is_some() {
            return Err(CatalogError::TableAlreadyExists(table_name));
        }

        tx.put(&key, &payload)?;
        match tx.commit() {
            Ok(_) => {
                self.tables.write().insert(table_name, descriptor);
                Ok(())
            }
            Err(err @ TransactionError::WriteWriteConflict { .. }) => {
                self.refresh()?;
                if self.tables.read().contains_key(&table_name) {
                    return Err(CatalogError::TableAlreadyExists(table_name.to_string()));
                }
                Err(CatalogError::Transaction(err))
            }
            Err(err) => Err(CatalogError::Transaction(err)),
        }
    }

    pub fn drop_table(&self, table_name: &str) -> Result<(), CatalogError> {
        let table_name = normalize_table_name(table_name)?;
        let key = table_storage_key(table_name);

        let mut tx = self.store.begin_transaction();
        if tx.get(&key)?.is_none() {
            return Err(CatalogError::TableNotFound(table_name.to_string()));
        }

        tx.delete(&key)?;
        match tx.commit() {
            Ok(_) => {
                self.tables.write().remove(table_name);
                Ok(())
            }
            Err(err @ TransactionError::WriteWriteConflict { .. }) => {
                self.refresh()?;
                if !self.tables.read().contains_key(table_name) {
                    return Err(CatalogError::TableNotFound(table_name.to_string()));
                }
                Err(CatalogError::Transaction(err))
            }
            Err(err) => Err(CatalogError::Transaction(err)),
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<TableDescriptor> {
        self.tables.read().get(table_name).cloned()
    }

    pub fn list_tables(&self) -> Vec<TableDescriptor> {
        self.tables.read().values().cloned().collect()
    }

    pub fn list_table_names(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    fn load_tables_from_store(&self) -> Result<BTreeMap<String, TableDescriptor>, CatalogError> {
        let mut loaded = BTreeMap::new();

        for (key, payload) in self.store.scan_prefix_latest(TABLE_PREFIX.as_bytes()) {
            let key_string = String::from_utf8_lossy(&key).into_owned();
            let table_name = table_name_from_key(&key)
                .ok_or_else(|| CatalogError::Deserialization {
                    key: key_string.clone(),
                    message: "table key does not use catalog table prefix".to_string(),
                })?
                .to_string();

            let descriptor: TableDescriptor = bincode::deserialize(&payload).map_err(|err| {
                CatalogError::Deserialization { key: key_string.clone(), message: err.to_string() }
            })?;

            descriptor.validate()?;
            if descriptor.name != table_name {
                return Err(CatalogError::Deserialization {
                    key: key_string,
                    message: format!(
                        "descriptor name '{}' does not match key table '{}'",
                        descriptor.name, table_name
                    ),
                });
            }

            loaded.insert(table_name, descriptor);
        }

        Ok(loaded)
    }
}

fn normalize_table_name(name: &str) -> Result<&str, CatalogError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(CatalogError::EmptyTableName);
    }
    Ok(trimmed)
}

fn table_storage_key(table_name: &str) -> Vec<u8> {
    format!("{TABLE_PREFIX}{table_name}").into_bytes()
}

fn table_name_from_key(key: &[u8]) -> Option<&str> {
    let key = std::str::from_utf8(key).ok()?;
    key.strip_prefix(TABLE_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::ColumnDescriptor;
    use crate::catalog::schema::ColumnType;

    fn users_table() -> TableDescriptor {
        TableDescriptor {
            name: "users".to_string(),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    column_type: ColumnType::BigInt,
                    nullable: false,
                    default: None,
                },
                ColumnDescriptor {
                    name: "email".to_string(),
                    column_type: ColumnType::Text,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn create_and_reload_table_descriptor() {
        let store = MvccStore::new();
        let catalog = Catalog::open(store.clone()).expect("open catalog");

        catalog.create_table(users_table()).expect("create users table");
        assert_eq!(catalog.list_table_names(), vec!["users".to_string()]);

        let reopened = Catalog::open(store).expect("reopen catalog");
        let table = reopened.get_table("users").expect("users table should exist");
        assert_eq!(table.primary_key, vec!["id".to_string()]);
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn duplicate_create_fails() {
        let store = MvccStore::new();
        let catalog = Catalog::open(store).expect("open catalog");
        catalog.create_table(users_table()).expect("first create");

        let err = catalog.create_table(users_table()).expect_err("duplicate create should fail");
        assert!(matches!(err, CatalogError::TableAlreadyExists(name) if name == "users"));
    }

    #[test]
    fn drop_table_removes_from_cache() {
        let store = MvccStore::new();
        let catalog = Catalog::open(store).expect("open catalog");
        catalog.create_table(users_table()).expect("create users table");

        catalog.drop_table("users").expect("drop users");
        assert!(catalog.get_table("users").is_none());
    }
}
