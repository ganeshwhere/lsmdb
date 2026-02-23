use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::column::{ColumnDescriptor, ColumnValidationError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDescriptor {
    pub name: String,
    pub columns: Vec<ColumnDescriptor>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TableValidationError {
    #[error("table name cannot be empty")]
    EmptyTableName,
    #[error("table must define at least one column")]
    NoColumns,
    #[error("column '{0}' is duplicated")]
    DuplicateColumn(String),
    #[error("primary key must contain at least one column")]
    EmptyPrimaryKey,
    #[error("primary key column '{0}' does not exist in table")]
    PrimaryKeyColumnMissing(String),
    #[error("primary key column '{0}' cannot be nullable")]
    PrimaryKeyColumnNullable(String),
    #[error("invalid column '{name}': {source}")]
    InvalidColumn {
        name: String,
        #[source]
        source: ColumnValidationError,
    },
}

impl TableDescriptor {
    pub fn validate(&self) -> Result<(), TableValidationError> {
        if self.name.trim().is_empty() {
            return Err(TableValidationError::EmptyTableName);
        }

        if self.columns.is_empty() {
            return Err(TableValidationError::NoColumns);
        }

        let mut seen = HashSet::new();
        for column in &self.columns {
            column.validate().map_err(|source| TableValidationError::InvalidColumn {
                name: column.name.clone(),
                source,
            })?;

            if !seen.insert(column.name.clone()) {
                return Err(TableValidationError::DuplicateColumn(column.name.clone()));
            }
        }

        if self.primary_key.is_empty() {
            return Err(TableValidationError::EmptyPrimaryKey);
        }

        for pk in &self.primary_key {
            let Some(column) = self.columns.iter().find(|column| &column.name == pk) else {
                return Err(TableValidationError::PrimaryKeyColumnMissing(pk.clone()));
            };

            if column.nullable {
                return Err(TableValidationError::PrimaryKeyColumnNullable(pk.clone()));
            }
        }

        Ok(())
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|column| column.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::ColumnType;

    fn column(name: &str, nullable: bool) -> ColumnDescriptor {
        ColumnDescriptor {
            name: name.to_string(),
            column_type: ColumnType::Text,
            nullable,
            default: None,
        }
    }

    #[test]
    fn validates_primary_key_presence_and_nullability() {
        let descriptor = TableDescriptor {
            name: "users".to_string(),
            columns: vec![column("id", false), column("email", false)],
            primary_key: vec!["id".to_string()],
        };

        assert!(descriptor.validate().is_ok());
    }

    #[test]
    fn rejects_nullable_primary_key() {
        let descriptor = TableDescriptor {
            name: "users".to_string(),
            columns: vec![column("id", true)],
            primary_key: vec!["id".to_string()],
        };

        let err = descriptor.validate().expect_err("validation should fail");
        assert_eq!(err, TableValidationError::PrimaryKeyColumnNullable("id".to_string()));
    }
}
