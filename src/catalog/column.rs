use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::schema::{ColumnType, DefaultValue};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDescriptor {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub default: Option<DefaultValue>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ColumnValidationError {
    #[error("column name cannot be empty")]
    EmptyName,
    #[error("default value does not match declared column type")]
    DefaultTypeMismatch,
}

impl ColumnDescriptor {
    pub fn validate(&self) -> Result<(), ColumnValidationError> {
        if self.name.trim().is_empty() {
            return Err(ColumnValidationError::EmptyName);
        }

        if let Some(default) = self.default.as_ref() {
            if !default.matches_type(self.column_type) {
                return Err(ColumnValidationError::DefaultTypeMismatch);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_default_type_alignment() {
        let column = ColumnDescriptor {
            name: "age".to_string(),
            column_type: ColumnType::Integer,
            nullable: false,
            default: Some(DefaultValue::Integer(0)),
        };
        assert!(column.validate().is_ok());
    }

    #[test]
    fn rejects_mismatched_default_type() {
        let column = ColumnDescriptor {
            name: "active".to_string(),
            column_type: ColumnType::Boolean,
            nullable: false,
            default: Some(DefaultValue::Text("true".to_string())),
        };

        let err = column.validate().expect_err("validation should fail");
        assert_eq!(err, ColumnValidationError::DefaultTypeMismatch);
    }
}
