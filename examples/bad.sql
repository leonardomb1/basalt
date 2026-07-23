-- Basalt SQL rewrite of bad.bsl (golden: examples/golden/bad.plan)
-- Negative test: truncated expression. `check` must fail with a parse error
-- pointing at end of input, exit code 1.

SELECT * FROM x.QUERY($$q$$) WHERE a >
