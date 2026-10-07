# Regular expressions

The regex engine behind `regexp_matches`, `regexp_extract` and
`regexp_replace` is a subset, sized for the patterns
SQL actually carries rather than for a dependency. It supports `^ $ . |`,
character classes with ranges and negation, capturing and non-capturing
groups, the escapes `\d \w \s` (and their negations), the quantifiers
`* + ?` and counted `{n} {n,} {n,m}` — each greedy, or lazy with a `?`
suffix (`*?`, `{2,4}?`), and `\n \t \r` and an escaped punctuation character
(`\.`, `\$`). Lookaround, backreferences (`\1`) and every other letter or
digit escape (`\b`, `\A`) are **rejected at compile time** rather than read as
literal characters, so an unsupported pattern is an error and never a silently
wrong match.
