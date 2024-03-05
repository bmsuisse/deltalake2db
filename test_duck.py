import duckdb

res = duckdb.execute(
    r"""select name from parquet_schema('C:\Projects\BMS_Github\deltalake2db\tests\data\faker2\Ec\*')"""
)
print([c[0] for c in res.description])
print(res.fetch_df())
