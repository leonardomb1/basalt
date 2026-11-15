# Not yet implemented

Accepted design not yet in the engine:

- **More pushdown into sources.** Filters move below a join and across an
  equijoin key, a chain of CTEs over one read sends all its filters as one `WHERE`
  ([Pushdown](../language/pushdown.md)), a whole aggregate runs in the source, and
  a join's keys narrow the other side's read and the reads of the joins before it
  ([Key pushdown](../language/joins.md#key-pushdown), [Runtime
  filters](../language/joins.md#runtime-filters)). Not yet:
  - a join of two tables on one connection sent to it as one query;
  - an aggregate in shapes other than the whole one (a partial aggregate, one over
    a join);
  - a filter flowing back from an earlier join to a later one's side (the backward
    pass of predicate transfer);
  - a filter on a join's right side by a bare name (`WHERE valor > 0` rather than
    `i.valor`), and one on the side an outer join null-extends, which stay above
    the join; a filter on the preserved side still moves.
