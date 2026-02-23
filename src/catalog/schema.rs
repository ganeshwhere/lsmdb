use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Integer,
    BigInt,
    Float,
    Text,
    Boolean,
    Blob,
    Timestamp,
}

impl ColumnType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::BigInt => "BIGINT",
            ColumnType::Float => "FLOAT",
            ColumnType::Text => "TEXT",
            ColumnType::Boolean => "BOOLEAN",
            ColumnType::Blob => "BLOB",
            ColumnType::Timestamp => "TIMESTAMP",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DefaultValue {
    Integer(i32),
    BigInt(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Timestamp(i64),
}

impl DefaultValue {
    pub fn matches_type(&self, column_type: ColumnType) -> bool {
        matches!(
            (self, column_type),
            (DefaultValue::Integer(_), ColumnType::Integer)
                | (DefaultValue::BigInt(_), ColumnType::BigInt)
                | (DefaultValue::Float(_), ColumnType::Float)
                | (DefaultValue::Text(_), ColumnType::Text)
                | (DefaultValue::Boolean(_), ColumnType::Boolean)
                | (DefaultValue::Blob(_), ColumnType::Blob)
                | (DefaultValue::Timestamp(_), ColumnType::Timestamp)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_value_type_matching() {
        assert!(DefaultValue::Integer(1).matches_type(ColumnType::Integer));
        assert!(DefaultValue::Text("x".to_string()).matches_type(ColumnType::Text));
        assert!(!DefaultValue::Blob(vec![1]).matches_type(ColumnType::Boolean));
    }
}
