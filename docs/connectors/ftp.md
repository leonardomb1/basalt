# FTP

Plain FTP is read by URL, `ftp://[user[:password]@]host[:port]/path`. Public
data is still published this way, and anonymous login is the default:

```sql
SELECT * FROM 'ftp://ftp.example.org/dados/2026/arquivo.csv' WITH (delimiter = ';', encoding = 'latin1');
SELECT * FROM 'ftp://ftp.example.org/dados/pacote.zip :: tabela.csv';
SELECT * FROM 'ftp://ftp.example.org/dados/mensal/';       -- every file under the folder
```

A server that needs a login can be named once with a connection, whose paths are
then `ftp://name/path`:

```sql
CREATE CONNECTION parceiro TYPE ftp OPTIONS (host = 'ftp.example.org');  -- PARCEIRO_USER / PARCEIRO_PASS
SELECT * FROM 'ftp://parceiro/saida/2026-10.csv';
```

Options are `host port user password`; `user`/`password` follow the
`NAME_USER`/`NAME_PASS` convention, and without them the login is anonymous.
Without a connection the login is the URL's, else `FTP_USER` / `FTP_PASSWORD`
from the environment, else `anonymous`. A name with spaces or accents is
percent-encoded in the URL (`arquivo%20um.csv`). A connection name is
process-wide, as an SFTP one is.

FTP has no cheap random access, so each file is downloaded whole to a temporary
folder the first time a run reads it and read from there. Every format works,
zip members, Parquet and Excel included, and a CSV still splits across `-j`
lanes. A second read of the same URL in the run uses the copy, and the copies
are removed when the run ends; error messages name the URL, not the copy.

A trailing `/` reads a folder: every file under it, subfolders included, with
names starting `_` or `.` skipped, as for any [folder](../language/sources.md)
read. Folders are listed by MLSD, or by NLST and a `CWD` to each name on servers
without it. A folder of more than 10,000 files is refused rather than copied, so
a URL one level too high fails fast: read a subfolder or a file instead.

Transfers are binary over a passive data connection (EPSV, or PASV where the
server refuses it). The address a PASV reply advertises is ignored in favour of
the server's own host, as a server behind NAT often advertises a private one.
Plain FTP sends the password in clear text, and FTPS (FTP over TLS) is not
supported; use [SFTP](sftp.md) for anything private.

A busy server (a 4xx reply) or a dropped connection fails the run as transient
(exit 75), worth retrying; a missing file (550) or a refused login fails it for
good, with the server's reply in the message. FTP is read only: `LOAD INTO
'ftp://…'` is refused by `check`.
