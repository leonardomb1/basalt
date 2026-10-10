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

Patterns match characters, not bytes: `.`, a class (`[ç]`, `[à-ú]`, `[^a]`) and
a literal each take one UTF-8 code point, so `regexp_matches('ação', '^a.ão$')`
is true and `regexp_extract('ação', '[ç]')` returns `ç`; a byte that starts no
valid UTF-8 sequence counts as one character. `\d \w \s` cover ASCII only, and
`i` folds ASCII letters.

The matcher backtracks, so a pattern that backtracks without end — a nested
quantifier like `(a+)+` against text it fails on — stops with an error rather
than hanging. A repeated literal, `.` or class (`a+`, `.*`, `[^,]{2,}`) repeats
over text of any length; a repeated group (`(ab)+`, `(?:a|x)*`) repeats at most
about 2,000 times in one match, past which the match fails with the same error —
write a group of single characters as a class (`[ax]*`).
