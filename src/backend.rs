use crate::bind_collector::DuckDbBindCollector;
use diesel::backend::{
    sql_dialect, Backend, DieselReserveSpecialization, SqlDialect, TrustedBackend,
};
use diesel::sql_types::{HasSqlType, TypeMetadata};

use crate::query_builder::DuckDBQueryBuilder;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct DuckDb;

impl Backend for DuckDb {
    type QueryBuilder = DuckDBQueryBuilder;
    type BindCollector<'a> = DuckDbBindCollector<'a>;
    type RawValue<'a> = duckdb::types::ToSqlOutput<'a>;
}

impl SqlDialect for DuckDb {
    type ReturningClause = sql_dialect::returning_clause::DoesNotSupportReturningClause;
    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;
    type InsertWithDefaultKeyword =
        sql_dialect::default_keyword_for_insert::DoesNotSupportDefaultKeyword;
    type BatchInsertSupport = sql_dialect::batch_insert_support::PostgresLikeBatchInsertSupport;
    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;
    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;
    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;
    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;
}

impl DieselReserveSpecialization for DuckDb {}

impl TrustedBackend for DuckDb {}

impl TypeMetadata for DuckDb {
    type TypeMetadata = ();
    type MetadataLookup = ();
}

impl HasSqlType<diesel::sql_types::SmallInt> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {}
}

impl HasSqlType<diesel::sql_types::Integer> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        
    }
}

impl HasSqlType<diesel::sql_types::BigInt> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Float> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Double> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Text> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Binary> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Bool> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Date> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Time> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}

impl HasSqlType<diesel::sql_types::Timestamp> for DuckDb {
    fn metadata(_: &mut ()) -> Self::TypeMetadata {
        ()
    }
}
