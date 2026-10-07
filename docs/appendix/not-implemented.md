# Not yet implemented

Accepted design not yet in the engine:

- **Whole-CTE / cross-source pushdown**, the full Trino model. Filters move below
  a join and across an equijoin key ([Pushdown](../language/pushdown.md)); what is
  missing is the rest. A multi-stage CTE that is entirely one connection is not
  collapsed into a single descended query, an aggregate descends only in the
  whole-aggregate shape and a join never does (Trino's
  `applyAggregation`/`applyJoin` are more general), and there is no
  runtime/dynamic filter — the build side's key values are not sent back to the
  probe scan, so a selective predicate on a *non-key* dimension column still
  reads the whole fact table. A filter on a join's right side by a bare name
  (`WHERE valor > 0` rather than `i.valor`) stays above the join, as does one
  after an outer join.
