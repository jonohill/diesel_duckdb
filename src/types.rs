use diesel::{deserialize::FromSql, serialize::IsNull, sql_types::*};
use duckdb::types::ValueRef;

use crate::DuckDb;


macro_rules! duckdb_to_sql_diesel {
    ($rust_type:ty, $diesel_type:ty) => {
        impl diesel::serialize::ToSql<$diesel_type, DuckDb> for $rust_type {
            fn to_sql<'b>(
                &'b self,
                out: &mut diesel::serialize::Output<'b, '_, DuckDb>,
            ) -> diesel::serialize::Result {
                let value = duckdb::ToSql::to_sql(self)?;
                out.set_value(value);
                Ok(IsNull::No)
            }
        }
    };
}

macro_rules! sql_diesel_to_duckdb {
    ($rust_type:ty, $diesel_type:ty) => {
        impl FromSql<$diesel_type, DuckDb> for $rust_type {
            fn from_sql(duckdb_value: <DuckDb as diesel::backend::Backend>::RawValue<'_>) -> diesel::deserialize::Result<Self> {
                use duckdb::types::ToSqlOutput;
                
                let value_ref = match duckdb_value {
                    ToSqlOutput::Borrowed(v) => v,
                    ToSqlOutput::Owned(ref v) => ValueRef::from(v),
                    _ => unimplemented!()
                };

                let value = duckdb::types::FromSql::column_result(value_ref)?;
                Ok(value)
            }
        }
    };
}

duckdb_to_sql_diesel!(i32, Integer);
duckdb_to_sql_diesel!(&str, Text);

sql_diesel_to_duckdb!(String, Text);
sql_diesel_to_duckdb!(i32, Integer);

// Boolean type support
duckdb_to_sql_diesel!(bool, Bool);
sql_diesel_to_duckdb!(bool, Bool);

// Integer types support
duckdb_to_sql_diesel!(i8, TinyInt);
duckdb_to_sql_diesel!(i16, SmallInt);
duckdb_to_sql_diesel!(i64, BigInt);
sql_diesel_to_duckdb!(i8, TinyInt);
sql_diesel_to_duckdb!(i16, SmallInt);
sql_diesel_to_duckdb!(i64, BigInt);

// Floating point types support
duckdb_to_sql_diesel!(f32, Float);
duckdb_to_sql_diesel!(f64, Double);
sql_diesel_to_duckdb!(f32, Float);
sql_diesel_to_duckdb!(f64, Double);

// Binary data support (only for slice, Vec<u8> conflicts with diesel's impl)
duckdb_to_sql_diesel!(&[u8], Binary);
sql_diesel_to_duckdb!(Vec<u8>, Binary);

// Date and time support
duckdb_to_sql_diesel!(chrono::NaiveDate, Date);
duckdb_to_sql_diesel!(chrono::NaiveTime, Time); 
duckdb_to_sql_diesel!(chrono::NaiveDateTime, Timestamp);

sql_diesel_to_duckdb!(chrono::NaiveDate, Date);
sql_diesel_to_duckdb!(chrono::NaiveTime, Time);
sql_diesel_to_duckdb!(chrono::NaiveDateTime, Timestamp);
