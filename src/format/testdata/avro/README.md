# Avro test files

`orders_{null,deflate,snappy,zstandard}.avro` hold the same three records, written
by fastavro 1.13.1 with `gen.py` under each codec, in blocks of about 200 bytes so
every file has several. The schema covers every primitive, nested and nullable
records, arrays, maps, enums, fixed and bytes decimals, every date, time and
timestamp logical type, `uuid`, a mixed union and a record that names itself.
`src/format/avro/read.zig` checks that basalt reads all four alike, value by value.

Regenerate them with:

```sh
docker run --rm -v "$PWD":/out python:3.12-slim sh -c \
  "pip install fastavro cramjam backports.zstd && python /out/gen.py"
```
