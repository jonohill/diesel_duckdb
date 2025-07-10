use crate::backend::DuckDb;
use diesel::{
    query_builder::BindCollector,
    serialize::{IsNull, Output},
};
use duckdb::types::ToSqlOutput;
use duckdb::{params_from_iter, ParamsFromIter};

// impl From<ToSqlOutput<'_>> for ValueWrapper<'_> {
//     fn from(output: ToSqlOutput<'_>) -> Self {
//         match output {
//             ToSqlOutput::Owned(value) => ValueWrapper(&mut value),
//             ToSqlOutput::Borrowed(value) => ValueWrapper(&mut value.clone()),
//         }
//     }
// }

#[derive(Default)]
pub struct DuckDbBindCollector<'a> {
    binds: Vec<ToSqlOutput<'a>>,
}

impl<'a> DuckDbBindCollector<'a> {
    pub fn into_params(self) -> ParamsFromIter<Vec<ToSqlOutput<'a>>> {
        params_from_iter(self.binds)
    }
}

impl<'a> BindCollector<'a, DuckDb> for DuckDbBindCollector<'a> {
    type Buffer = ToSqlOutput<'a>;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'a U,
        metadata_lookup: &mut (),
    ) -> diesel::QueryResult<()>
    where
        U: diesel::serialize::ToSql<T, DuckDb> + ?Sized + 'a,
    {
        let value = ToSqlOutput::Owned(duckdb::types::Value::Null);
        let mut to_sql_output = Output::new(value, metadata_lookup);
        let is_null = bind
            .to_sql(&mut to_sql_output)
            .map_err(diesel::result::Error::SerializationError)?;
        let bind = to_sql_output.into_inner();
        let bind_value = match is_null {
            IsNull::No => bind,
            IsNull::Yes => ToSqlOutput::Owned(duckdb::types::Value::Null),
        };
        self.binds.push(bind_value);
        Ok(())
    }

    fn push_null_value(&mut self, _metadata: ()) -> diesel::QueryResult<()> {
        self.binds
            .push(ToSqlOutput::Owned(duckdb::types::Value::Null));
        Ok(())
    }
}
