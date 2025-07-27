// Test file to verify NaiveDateTime works in a simple context
use diesel::prelude::*;
use chrono::NaiveDateTime;
use crate::DuckDb;

#[derive(Debug, Clone)]
#[derive(Queryable, Selectable)]
#[diesel(table_name = test_table)]
#[diesel(check_for_backend(DuckDb))]
pub struct TestPrice {
    pub date: NaiveDateTime,
    pub store_id: i64,
    pub price_cent: i64,
}

diesel::table! {
    test_table (date, store_id) {
        date -> diesel::sql_types::Timestamp,
        store_id -> diesel::sql_types::BigInt,
        price_cent -> diesel::sql_types::BigInt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_struct_compiles() {
        // If this compiles, it means our FromSqlRow implementation works
        let _test = TestPrice {
            date: chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            store_id: 1,
            price_cent: 100,
        };
        
        // Test that the Queryable trait is available
        assert_eq!(std::any::type_name::<TestPrice>(), "diesel_duckdb::tests::chrono_test::TestPrice");
    }
}
