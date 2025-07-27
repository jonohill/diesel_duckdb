// Support for chrono types in diesel_duckdb
// This module provides the necessary FromSqlRow implementations for chrono types

use crate::DuckDb;
use diesel::deserialize::{FromSqlRow, Result as DeserializeResult};
use diesel::row::Row;
use diesel::sql_types::{Date, Time, Timestamp};

// For NaiveDate
impl FromSqlRow<Date, DuckDb> for chrono::NaiveDate {
    fn build_from_row<'a>(row: &impl Row<'a, DuckDb>) -> DeserializeResult<Self> {
        use diesel::deserialize::FromSql;
        use diesel::row::Field;
        
        let field = row.get(0).ok_or("No field at index 0")?;
        Self::from_nullable_sql(field.value())
    }
}

// For NaiveTime
impl FromSqlRow<Time, DuckDb> for chrono::NaiveTime {
    fn build_from_row<'a>(row: &impl Row<'a, DuckDb>) -> DeserializeResult<Self> {
        use diesel::deserialize::FromSql;
        use diesel::row::Field;
        
        let field = row.get(0).ok_or("No field at index 0")?;
        Self::from_nullable_sql(field.value())
    }
}

// For NaiveDateTime
impl FromSqlRow<Timestamp, DuckDb> for chrono::NaiveDateTime {
    fn build_from_row<'a>(row: &impl Row<'a, DuckDb>) -> DeserializeResult<Self> {
        use diesel::deserialize::FromSql;
        use diesel::row::Field;
        
        let field = row.get(0).ok_or("No field at index 0")?;
        Self::from_nullable_sql(field.value())
    }
}

// Implement StaticallySizedRow for chrono types to work with tuples
impl diesel::deserialize::StaticallySizedRow<Date, DuckDb> for chrono::NaiveDate {
    const FIELD_COUNT: usize = 1;
}

impl diesel::deserialize::StaticallySizedRow<Time, DuckDb> for chrono::NaiveTime {
    const FIELD_COUNT: usize = 1;
}

impl diesel::deserialize::StaticallySizedRow<Timestamp, DuckDb> for chrono::NaiveDateTime {
    const FIELD_COUNT: usize = 1;
}
