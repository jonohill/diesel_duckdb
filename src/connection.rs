use diesel::{
    connection::{
        get_default_instrumentation, statement_cache::StatementCache, AnsiTransactionManager,
        ConnectionSealed, DefaultLoadingMode, Instrumentation, LoadConnection, SimpleConnection,
    },
    expression::QueryMetadata,
    migration::{MigrationConnection, CREATE_MIGRATIONS_TABLE},
    query_builder::{Query, QueryFragment, QueryId},
    result::{ConnectionError, ConnectionResult},
    row::{Field, PartialRow, Row, RowIndex, RowSealed},
    sql_query, Connection, QueryResult, RunQueryDsl,
};
use duckdb::Connection as DuckDBConn;

use crate::error::MapDieselError;
use crate::{bind_collector::DuckDbBindCollector, DuckDb};
use diesel::connection::statement_cache::MaybeCached;
use std::marker::PhantomData;

// Cursor type for iterating over query results
pub struct DuckDbCursor<'conn, 'query> {
    rows: std::vec::IntoIter<DuckDbRow<'conn, 'query>>,
    _phantom: PhantomData<&'query ()>,
}

impl<'conn, 'query> DuckDbCursor<'conn, 'query> {
    fn new(rows: Vec<DuckDbRow<'conn, 'query>>) -> Self {
        Self {
            rows: rows.into_iter(),
            _phantom: PhantomData,
        }
    }
}

impl<'conn, 'query> Iterator for DuckDbCursor<'conn, 'query> {
    type Item = QueryResult<DuckDbRow<'conn, 'query>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }
}

// Row type for individual database rows
pub struct DuckDbRow<'conn, 'query> {
    // Store the raw data instead of the duckdb::Row directly
    values: Vec<duckdb::types::Value>,
    column_names: Vec<String>,
    _phantom: PhantomData<(&'conn (), &'query ())>,
}

impl<'conn, 'query> DuckDbRow<'conn, 'query> {
    fn from_duckdb_row(row: &duckdb::Row) -> diesel::QueryResult<Self> {
        let column_count = row.as_ref().column_count();
        let mut values = Vec::with_capacity(column_count);
        let mut column_names = Vec::with_capacity(column_count);

        for i in 0..column_count {
            // Extract the value
            let value = row
                .get::<_, duckdb::types::Value>(i)
                .map_err(|e| diesel::result::Error::DeserializationError(e.into()))?;
            values.push(value);

            // Extract the column name
            let name = row
                .as_ref()
                .column_name(i)
                .map_err(|e| diesel::result::Error::DeserializationError(e.into()))?;
            column_names.push(name.to_string());
        }

        Ok(Self {
            values,
            column_names,
            _phantom: PhantomData,
        })
    }
}

impl<'conn, 'query> RowSealed for DuckDbRow<'conn, 'query> {}

impl<'conn, 'query> Row<'conn, DuckDb> for DuckDbRow<'conn, 'query> {
    type Field<'f>
        = DuckDbField<'f>
    where
        Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.values.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        Self: 'b + RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(DuckDbField::new(self, idx))
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for DuckDbRow<'_, '_> {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.field_count() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'idx> RowIndex<&'idx str> for DuckDbRow<'_, '_> {
    fn idx(&self, field_name: &'idx str) -> Option<usize> {
        for (i, name) in self.column_names.iter().enumerate() {
            if name == field_name {
                return Some(i);
            }
        }
        None
    }
}

// Field type for individual column values
pub struct DuckDbField<'row> {
    row: &'row DuckDbRow<'row, 'row>,
    idx: usize,
}

impl<'row> DuckDbField<'row> {
    fn new(row: &'row DuckDbRow<'row, 'row>, idx: usize) -> Self {
        Self { row, idx }
    }
}

impl<'row> Field<'row, DuckDb> for DuckDbField<'row> {
    fn field_name(&self) -> Option<&str> {
        self.row.column_names.get(self.idx).map(|s| s.as_str())
    }

    fn is_null(&self) -> bool {
        matches!(
            self.row.values.get(self.idx),
            Some(duckdb::types::Value::Null)
        )
    }

    fn value(&self) -> Option<<DuckDb as diesel::backend::Backend>::RawValue<'_>> {
        self.row
            .values
            .get(self.idx)
            .map(|value| duckdb::types::ToSqlOutput::Owned(value.clone()))
    }
}

pub struct DuckDbConnection {
    statement_cache: StatementCache<DuckDb, String>,
    connection: DuckDBConn,
    transaction_state: AnsiTransactionManager,
    instrumentation: Option<Box<dyn Instrumentation>>,
}

impl AsRef<DuckDBConn> for DuckDbConnection {
    fn as_ref(&self) -> &DuckDBConn {
        &self.connection
    }
}

impl ConnectionSealed for DuckDbConnection {}

impl LoadConnection<DefaultLoadingMode> for DuckDbConnection {
    type Cursor<'conn, 'query> = DuckDbCursor<'conn, 'query>;
    type Row<'conn, 'query> = DuckDbRow<'conn, 'query>;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let stmt = self.statement_cache.cached_statement(
            &source,
            &DuckDb,
            &[],
            |sql, _| Ok(sql.to_owned()),
            &mut self.instrumentation,
        )?;

        let mut binds = DuckDbBindCollector::default();
        source.collect_binds(&mut binds, &mut (), &DuckDb)?;
        let params = binds.into_params();

        let rows = match stmt {
            MaybeCached::Cached(sql) => {
                let mut q = self.connection.prepare_cached(sql).map_diesel_error()?;
                let mut rows = q.query(params).map_diesel_error()?;
                let mut result_rows = Vec::new();

                while let Some(row) = rows.next().map_diesel_error()? {
                    result_rows.push(DuckDbRow::from_duckdb_row(row)?);
                }
                result_rows
            }
            MaybeCached::CannotCache(sql) => {
                let mut q = self.connection.prepare(&sql).map_diesel_error()?;
                let mut rows = q.query(params).map_diesel_error()?;
                let mut result_rows = Vec::new();

                while let Some(row) = rows.next().map_diesel_error()? {
                    result_rows.push(DuckDbRow::from_duckdb_row(row)?);
                }
                result_rows
            }
            _ => panic!("Unexpected statement cache state"),
        };

        Ok(DuckDbCursor::new(rows))
    }
}

impl SimpleConnection for DuckDbConnection {
    fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        self.connection.execute_batch(query).map_diesel_error()
    }
}

impl Connection for DuckDbConnection {
    type Backend = DuckDb;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let instrumentation = get_default_instrumentation();
        // instrumentation.on_connection_event(InstrumentationEvent::StartEstablishConnection {
        //     url: database_url,
        // });

        let conn_result = DuckDBConn::open(database_url);

        // instrumentation.on_connection_event(InstrumentationEvent::FinishEstablishConnection {
        //     url: database_url,
        //     error: conn_result.as_ref().err(),
        // });

        let connection = conn_result.map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(Self {
            connection,
            transaction_state: AnsiTransactionManager::default(),
            instrumentation,
            statement_cache: StatementCache::new(),
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        // self.instrumentation
        //     .on_connection_event(InstrumentationEvent::StartQuery {
        //         query: &diesel::debug_query(&source),
        //     });

        let stmt = self.statement_cache.cached_statement(
            &source,
            &DuckDb,
            &[],
            |sql, _| Ok(sql.to_owned()), // hack, passthrough and let underlying duckdb library do it
            &mut self.instrumentation,
        )?;

        let mut binds = DuckDbBindCollector::default();
        source.collect_binds(&mut binds, &mut (), &DuckDb)?;
        let params = binds.into_params();

        let count = match stmt {
            MaybeCached::Cached(sql) => {
                let mut q = self.connection.prepare_cached(sql).map_diesel_error()?;
                q.execute(params).map_diesel_error()?
            }
            MaybeCached::CannotCache(sql) => {
                let mut q = self.connection.prepare(&sql).map_diesel_error()?;
                q.execute(params).map_diesel_error()?
            }
            _ => panic!("Unexpected statement cache state"),
        };

        Ok(count)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_state
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Some(Box::new(instrumentation));
    }
}

impl MigrationConnection for DuckDbConnection {
    fn setup(&mut self) -> QueryResult<usize> {
        sql_query(CREATE_MIGRATIONS_TABLE).execute(self)
    }
}
