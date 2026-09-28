"""Nested parquet columns against pyarrow's own reading of them.

Usage: python it/parquet_nested.py <basalt> <scratch dir>
Random rows (seeded) with nulls at every level — lists of structs holding
lists and structs, maps of lists, lists of lists of structs, a struct holding a
list and a map — written in three page layouts. Every cell basalt returns must
equal pyarrow's value, rendered as JSON. Exits non-zero on any mismatch.
"""
import sys, json, random, subprocess, datetime as dt
import pyarrow as pa, pyarrow.parquet as pq
random.seed(7)
B, S = sys.argv[1], sys.argv[2]
def maybe(v, p=0.15): return None if random.random() < p else v
def strs(): return maybe([maybe(random.choice(['a','bé','"q"','x\\y',''])) for _ in range(random.randint(0,3))])
N = 2000
rows = []
for i in range(N):
    rows.append({
        'id': i,
        'recs': maybe([maybe({'a': maybe(random.randint(-5,5)), 'tags': strs(), 'inner': maybe({'x': maybe(random.random())})}) for _ in range(random.randint(0,3))]),
        'mp': maybe([(k, maybe([maybe(random.randint(0,9)) for _ in range(random.randint(0,2))])) for k in random.sample(['k1','k2','k3'], random.randint(0,3))]),
        'll': maybe([maybe([maybe({'d': maybe(dt.date(2020,1,1)+dt.timedelta(days=random.randint(-9999,9999))), 'n': random.randint(0,3)}) for _ in range(random.randint(0,2))]) for _ in range(random.randint(0,2))]),
        'st': maybe({'tags': strs(), 'kv': maybe([('z', maybe(1.5))]), 'v': maybe(3)}),
    })
schema = pa.schema([
    ('id', pa.int64()),
    ('recs', pa.list_(pa.struct([('a', pa.int32()), ('tags', pa.list_(pa.string())), ('inner', pa.struct([('x', pa.float64())]))]))),
    ('mp', pa.map_(pa.string(), pa.list_(pa.int64()))),
    ('ll', pa.list_(pa.list_(pa.struct([('d', pa.date32()), ('n', pa.int64())])))),
    ('st', pa.struct([('tags', pa.list_(pa.string())), ('kv', pa.map_(pa.string(), pa.float64())), ('v', pa.int64())])),
])
t = pa.Table.from_pylist(rows, schema=schema)
def norm(v, ty):
    if v is None: return None
    if pa.types.is_map(ty): return {str(k): norm(x, ty.item_type) for k, x in v}
    if pa.types.is_list(ty) or pa.types.is_large_list(ty): return [norm(x, ty.value_type) for x in v]
    if pa.types.is_struct(ty): return {f.name: norm(v.get(f.name), f.type) for f in ty}
    if isinstance(v, dt.date): return v.isoformat()
    return v
bad = 0
for name, kw in [('v1', dict(data_page_version='1.0', data_page_size=512, row_group_size=1700)), ('v2', dict(data_page_version='2.0', compression='zstd', data_page_size=300)), ('plain', dict(use_dictionary=False, row_group_size=999))]:
    path = f'{S}/nested_{name}.parquet'
    pq.write_table(t, path, **kw)
    out = subprocess.run([B, 'run', '-q', '--format', 'json', '-c', f"SELECT * FROM '{path}' ORDER BY id;"], capture_output=True, text=True)
    if out.returncode != 0: print(name, 'FAILED', out.stderr[:500]); bad += 1; continue
    got = [json.loads(l) for l in out.stdout.splitlines()]
    assert len(got) == N, (name, len(got))
    exp_rows = t.to_pylist()
    # flat struct fields: st.v is a flat column; st.tags and st.kv are nested
    for r, e in zip(got, exp_rows):
        for col, ty in [('recs', schema.field('recs').type), ('mp', schema.field('mp').type), ('ll', schema.field('ll').type)]:
            want = norm(e[col], ty)
            have = None if r[col] is None else json.loads(r[col])
            if want != have:
                bad += 1
                if bad < 5: print(name, 'row', e['id'], col, 'want', want, 'have', have)
        st = e['st']
        for sub, ty in [('tags', schema.field('st').type.field('tags').type), ('kv', schema.field('st').type.field('kv').type)]:
            want = None if st is None else norm(st.get(sub), ty)
            have = None if r['st.' + sub] is None else json.loads(r['st.' + sub])
            if want != have:
                bad += 1
                if bad < 5: print(name, 'row', e['id'], 'st.' + sub, 'want', want, 'have', have)
        if (None if st is None else st['v']) != r['st.v']:
            bad += 1
    print(name, 'checked', N, 'rows; columns:', sorted(got[0].keys()))
print('MISMATCHES', bad)

sys.exit(1 if bad else 0)
