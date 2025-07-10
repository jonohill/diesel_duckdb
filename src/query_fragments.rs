use crate::DuckDb;
use diesel::query_builder::{
    AstPass, BoxedLimitOffsetClause, IntoBoxedClause, LimitClause, LimitOffsetClause,
    NoLimitClause, NoOffsetClause, OffsetClause, QueryFragment,
};
use diesel::result::QueryResult;

// ============================================================================
// LimitOffsetClause implementations
// ============================================================================

impl QueryFragment<DuckDb> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    fn walk_ast<'b>(&'b self, _out: AstPass<'_, 'b, DuckDb>) -> QueryResult<()> {
        Ok(())
    }
}

impl<L> QueryFragment<DuckDb> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    LimitClause<L>: QueryFragment<DuckDb>,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, DuckDb>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<O> QueryFragment<DuckDb> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    OffsetClause<O>: QueryFragment<DuckDb>,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, DuckDb>) -> QueryResult<()> {
        // DuckDB supports OFFSET without LIMIT (like PostgreSQL)
        self.offset_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<L, O> QueryFragment<DuckDb> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    LimitClause<L>: QueryFragment<DuckDb>,
    OffsetClause<O>: QueryFragment<DuckDb>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DuckDb>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out.reborrow())?;
        self.offset_clause.walk_ast(out.reborrow())?;
        Ok(())
    }
}

// ============================================================================
// BoxedLimitOffsetClause implementations
// ============================================================================

impl QueryFragment<DuckDb> for BoxedLimitOffsetClause<'_, DuckDb> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DuckDb>) -> QueryResult<()> {
        if let Some(ref limit) = self.limit {
            limit.walk_ast(out.reborrow())?;
        }
        if let Some(ref offset) = self.offset {
            offset.walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}

// ============================================================================
// IntoBoxedClause implementations
// ============================================================================

impl<'a> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: None,
        }
    }
}

impl<'a, L> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: None,
        }
    }
}

impl<'a, O> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<DuckDb> + Send + 'a,
    O: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}
