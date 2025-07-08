use diesel::result::{DatabaseErrorInformation, DatabaseErrorKind, Error as DieselError};

/// Trait for converting DuckDB errors to Diesel errors
pub trait MapDieselError<T> {
    fn map_diesel_error(self) -> diesel::QueryResult<T>;
}

/// DuckDB-specific database error information
#[derive(Debug)]
pub struct DuckDbErrorInformation {
    pub error_message: String,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub constraint_name: Option<String>,
    pub statement_position: Option<i32>,
}

impl DatabaseErrorInformation for DuckDbErrorInformation {
    fn message(&self) -> &str {
        &self.error_message
    }

    fn details(&self) -> Option<&str> {
        None
    }

    fn hint(&self) -> Option<&str> {
        None
    }

    fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    fn column_name(&self) -> Option<&str> {
        self.column_name.as_deref()
    }

    fn constraint_name(&self) -> Option<&str> {
        self.constraint_name.as_deref()
    }

    fn statement_position(&self) -> Option<i32> {
        self.statement_position
    }
}

impl<T> MapDieselError<T> for Result<T, duckdb::Error> {
    fn map_diesel_error(self) -> diesel::QueryResult<T> {
        self.map_err(|e| {
            use duckdb::Error;

            match e {
                // DuckDB-specific failures with error codes and optional messages
                Error::DuckDBFailure(error_code, message_opt) => {
                    let message =
                        message_opt.unwrap_or_else(|| format!("DuckDB error code: {}", error_code));
                    let error_info = DuckDbErrorInformation {
                        error_message: message.clone(),
                        table_name: extract_table_name(&message),
                        column_name: extract_column_name(&message),
                        constraint_name: extract_constraint_name(&message),
                        statement_position: None,
                    };

                    // Map based on error message content since DuckDB error codes vary
                    let message_lower = message.to_lowercase();
                    let kind = if message_lower.contains("unique")
                        || message_lower.contains("duplicate")
                    {
                        DatabaseErrorKind::UniqueViolation
                    } else if message_lower.contains("not null") {
                        DatabaseErrorKind::NotNullViolation
                    } else if message_lower.contains("foreign key") {
                        DatabaseErrorKind::ForeignKeyViolation
                    } else if message_lower.contains("check constraint") {
                        DatabaseErrorKind::CheckViolation
                    } else {
                        DatabaseErrorKind::Unknown
                    };

                    DieselError::DatabaseError(kind, Box::new(error_info))
                }

                // Column access errors
                Error::InvalidColumnIndex(index) => DieselError::DeserializationError(
                    format!("Invalid column index: {}", index).into(),
                ),

                Error::InvalidColumnName(name) => DieselError::DeserializationError(
                    format!("Invalid column name: {}", name).into(),
                ),

                Error::InvalidColumnType(index, name, type_name) => {
                    DieselError::DeserializationError(
                        format!(
                            "Invalid column type at index {}, name '{}': {}",
                            index, name, type_name
                        )
                        .into(),
                    )
                }

                // Parameter binding errors
                Error::InvalidParameterCount(expected, actual) => {
                    let error_info = DuckDbErrorInformation {
                        error_message: format!(
                            "Invalid parameter count: expected {}, got {}",
                            expected, actual
                        ),
                        table_name: None,
                        column_name: None,
                        constraint_name: None,
                        statement_position: None,
                    };
                    DieselError::DatabaseError(DatabaseErrorKind::Unknown, Box::new(error_info))
                }

                Error::StatementChangedRows(count) => DieselError::DeserializationError(
                    format!("Unexpected number of changed rows: {}", count).into(),
                ),

                // Path and file system errors
                Error::InvalidPath(path) => DieselError::DeserializationError(
                    format!("Invalid path: {}", path.display()).into(),
                ),

                // Conversion errors
                Error::ToSqlConversionFailure(err) => DieselError::SerializationError(err),

                Error::FromSqlConversionFailure(idx, name, err) => {
                    DieselError::DeserializationError(
                        format!("Conversion failure at column {} ('{}'): {}", idx, name, err)
                            .into(),
                    )
                }

                // String encoding errors
                Error::Utf8Error(err) => DieselError::DeserializationError(
                    format!("UTF-8 conversion error: {}", err).into(),
                ),

                Error::NulError(err) => DieselError::DeserializationError(
                    format!("Null byte in string: {}", err).into(),
                ),

                // Catch-all for any other error variants
                _ => {
                    let error_info = DuckDbErrorInformation {
                        error_message: e.to_string(),
                        table_name: None,
                        column_name: None,
                        constraint_name: None,
                        statement_position: None,
                    };
                    DieselError::DatabaseError(DatabaseErrorKind::Unknown, Box::new(error_info))
                }
            }
        })
    }
}

/// Extract table name from error message using common patterns
fn extract_table_name(message: &str) -> Option<String> {
    // Try common patterns for table name extraction
    if let Some(pos) = message.find("table ") {
        let after_table = &message[pos + 6..];
        if let Some(end) = after_table.find(|c: char| c.is_whitespace() || c == '.' || c == '"') {
            let table_name = &after_table[..end];
            return Some(table_name.trim_matches('"').to_string());
        }
    }
    None
}

/// Extract column name from error message using common patterns
fn extract_column_name(message: &str) -> Option<String> {
    if let Some(pos) = message.find("column ") {
        let after_column = &message[pos + 7..];
        if let Some(end) = after_column.find(|c: char| c.is_whitespace() || c == '.' || c == '"') {
            let column_name = &after_column[..end];
            return Some(column_name.trim_matches('"').to_string());
        }
    }
    None
}

/// Extract constraint name from error message using common patterns
fn extract_constraint_name(message: &str) -> Option<String> {
    if let Some(pos) = message.find("constraint ") {
        let after_constraint = &message[pos + 11..];
        if let Some(end) = after_constraint.find(|c: char| c.is_whitespace() || c == '"') {
            let constraint_name = &after_constraint[..end];
            return Some(constraint_name.trim_matches('"').to_string());
        }
    }
    None
}
