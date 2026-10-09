# REPL

`basalt repl` is an interactive session: type a statement, see its result, and
keep the session's connections and functions from one entry to the next.

## Results

The REPL is where a person looks at data, so there the table is fitted to the
terminal: a row of types under the names, numbers right-aligned, `NULL` spelled
out, cells cut at 40 columns, and `…` for what does not fit — the middle columns,
in the manner of pandas, and the middle rows past 40. The footer always gives the
true size:

```text
 id  customer_name       amount  …  country  last_col
int  string               float  …  string   string
---  ------------------  ------  …  -------  --------
  0  customer number 0        0  …  BR       tail-0
  …  …                        …  …  …        …
 99  customer number 99   148.5  …  BR       tail-99
(100 rows × 10 columns, 5 shown — \view scrolls it)
```

A REPL result keeps its first 10,000 rows (and the last 20) rather than all of
it. Values are coloured by their column's type — numbers cyan and a negative one
red, dates and times magenta, `true` green and `false` red, arrays and structs
yellow, `NULL` dim — with the names bold and the types dim; only foreground
colours, so a light terminal reads it as well as a dark one. Set `NO_COLOR` to
drop all of it.

## Viewing a result

In the REPL, `\view` (or `\v`) opens the last result full-screen, the names and
types pinned. ←/→ move a column cursor (its name lit; the view follows it),
↑/↓ scroll by row, Ctrl-arrows and PgUp/PgDn move a screenful, Home/End jump to
the first and last columns, `g`/`G` to the first and last rows, `q` leaves.
`s` sorts by the cursor's column — ascending, again descending, again off; by
value for a number, as text otherwise (which orders ISO dates rightly), `NULL`
last either way. `/` filters it as you type: `text` keeps the rows containing it
in any case, `!text` those that do not, and `>= 100`, `< 2026-01-01`, `= SP`,
`!= 0` compare — by value on a number column. Filters on several columns all
apply; the header marks a filtered column `≈` and the sorted one `↑`/`↓`. Enter
keeps a filter, Esc while typing puts it back, Esc after clears the filters and
then the sort. `f` finds text across every column: type the search and press
Enter, and the rows narrow to those holding it, each match highlighted where it
shows. Words are AND-ed and may be in any column, `-word` keeps the rows without
it, `col:word` looks in that column only, and `"two words"` is one phrase — all in
any case. Esc clears a find before the filters and the sort. Sorting, filtering
and finding rearrange the rows kept and never run the query again — so on a
result larger than 10,000 rows they see only the first 10,000, and the status
line says so.

## Commands

`basalt repl` executes on a top-level `;` and carries `CREATE CONNECTION` /
`CREATE FUNCTION` / `PARAM` declarations across entries (re-declaring a name
replaces it). Meta commands: `\connections` list the session's declarations ·
`\reset` drop them · `\clear` (or `clear`, `cls`, `^L`) clear the screen ·
`\format table|json|csv|tsv` switch result output · `\view` scroll, sort and
filter the last result · `\help` · `\q`. Short forms: `\c` for
`\connections`, `\e` for `\edit`, `\f` for `\format`, `\v` for `\view`,
`\source` for `\i`; `\h`, `help` or `?` for help, and `\quit`, `:q`, `quit` or
`exit` to leave.

## Editing

The entry is a small text editor rather than a single line, with an editor's
habits. Enter runs the entry when it ends in a top-level `;` and the cursor is
at its end; anywhere else it opens a line — and after `(` it steps in and puts
the `)` on a line of its own. Ctrl+J runs the entry as it stands, `;` or not
(Ctrl+Enter too, where the terminal delivers it). Brackets and quotes close
themselves, typing the closer steps over it, and over a selection they wrap it;
the matching bracket is underlined. Alt+Up/Down move the line or selected lines,
Shift+Alt+Up/Down duplicate them, Tab and Shift+Tab indent and dedent a
selection, Ctrl+/ comments lines out with `--` and back in, Esc drops the
selection. The entry is coloured as you type (keywords, strings, numbers,
comments, `$params`; `NO_COLOR` turns it off), a multi-line entry gets line
numbers in its gutter, and a parse error is shown with a caret under the column
it names.

## Making a connection

`\connect [type]` makes a connection with a form: the type picked from a list
(↑/↓, its number, or the first letters of its name), then the fields its
connector needs on one screen — host with the type's usual port, database, user,
password (blank means the `env(NAME_USER)` / `env(NAME_PASS)` convention;
`env:VAR` names another variable). ↑/↓ and Tab move between fields, ←/→ turn a
choice such as `tls` or `auth`, a field edits with the entry's own keys, Enter
goes on and submits from the last field, Esc cancels with nothing made. A field
only some answers need, such as the Kerberos `realm`, shows only then. It shows
the `CREATE CONNECTION` it built (a typed password masked), registers it, and
offers to reach it and to save it to the startup file. `^R` searches the history
incrementally (type to narrow, `^R` for an older match, Enter keeps it). A
session starts by running `~/.config/basalt/repl.sql` (or
`$XDG_CONFIG_HOME/basalt/repl.sql`) when it exists — the place for the
connections you always want, with `env()` for the secrets — and `\save` writes
the session's declarations there (or to a named file); `\i <file>` runs any file
so its declarations join the session; `\connections` shows them as a table with
host, database and whether the session has reached them (`\c test` reaches each
now); `\edit` opens the last entry in `$EDITOR` and runs what comes back.

## Completion

Tab completes the word under the cursor: keywords (in the case you are typing),
the session's connections, functions and `$params`, the entry's CTEs, a path
inside an unclosed quote, `conn.` followed by that connection's tables, the
columns of every table and file the entry names, and the built-in functions —
scalar, aggregate and window. Tables and columns are asked of the source once
per session, on first use, through the same `information_schema` queries `SHOW
TABLES` and `DESCRIBE` run. A lone match is taken; several fill in what they
share and come up as a row of choices that Tab cycles through. Arrows travel the
whole entry — Ctrl+arrows by word, Home/End the line, Ctrl+Home/End the entry —
and Up or Down past its edge recall history, where an entry comes back whole
however many lines it had (`~/.basalt_history`). Shift with any of those
selects; typing, Backspace and Delete replace the selection. `^A` selects all,
`^C` copies a selection (and drops the entry when there is none), `^X` cuts,
`^V` pastes, `^Z`/`^Y` undo and redo. A paste is inserted as text, never run
line by line, and a copy reaches the system clipboard where the terminal
supports OSC 52. Two things a terminal cannot deliver: the mouse, and
Ctrl+Enter, which arrives as plain Enter.
