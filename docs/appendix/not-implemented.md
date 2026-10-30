# Not yet implemented

Accepted design not yet in the engine:

- **Cross-source pushdown**, the full Trino model. Filters move below a join and
  across an equijoin key, and a chain of CTEs over one read sends all its filters
  as one `WHERE` ([Pushdown](../language/pushdown.md)); what is missing is the
  rest. An aggregate descends only in the whole-aggregate shape and a join never
  does (Trino's
  `applyAggregation`/`applyJoin` are more general). A join's keys narrow the
  other side's read ([Key pushdown](../language/joins.md#key-pushdown)) and the
  reads of the joins before it ([Runtime
  filters](../language/joins.md#runtime-filters)), but no filter flows back from
  an earlier join to a later one's side (the backward pass of predicate
  transfer). A filter on a join's right side by a bare name
  (`WHERE valor > 0` rather than `i.valor`) stays above the join, as does one
  after an outer join.
