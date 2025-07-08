use diesel::query_builder::QueryBuilder;

use crate::DuckDb;

#[derive(Debug, Default)]
pub struct DuckDBQueryBuilder {
    sql: String,
}

impl DuckDBQueryBuilder {
    pub fn new() -> Self {
        Self { sql: String::new() }
    }
}

impl QueryBuilder<DuckDb> for DuckDBQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> diesel::QueryResult<()> {
        self.sql.push('"');
        self.sql.push_str(&identifier.replace('"', "\"\""));
        self.sql.push('"');
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.sql.push('?');
    }

    fn finish(self) -> String {
        self.sql
    }
}
