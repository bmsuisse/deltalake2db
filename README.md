# Deltalake2DB

This is a simple project that uses Metadata from `deltalake` package to provide methods to read Delta Lake Tables
to either Polars or DuckDB with better Protocol Support as the main `deltalake` package.

## Use with Duckdb

Install `deltalake2db` and `duckdb` using pip/poetry/whatever you use.

Then you can do like this:

```python

from deltalake2db import get_sql_for_delta

with duckdb.connect() as con:
    dt = DeltaTable("tests/data/faker2")
    sql = get_sql_for_delta(dt, duck_con=con) # get select statement
    print(sql)
    con.execute("create view delta_table as " + sql)

    con.execute("select * from delta_table").fetch_all()
```

If you'd like to manipulate you can use `get_sql_for_delta_expr` which returns a SqlGlot Object

## Use with Polars

Install `deltalake2db` and `polars` using pip/poetry/whatever you use.

```python
dt = DeltaTable("tests/data/faker2")
from deltalake2db import polars_scan_delta
lazy_df = polars_scan_delta(dt)
df = lazy_df.collect()

```

## Protocol Support

- [x] Column Mapping
- [x] Almost Data Types, including Structs/Lists, Map yet to be done
- [ ] Deletion Vectors

This does not do a Delta Lake Feature check yet, which is problematic for Deletion Vectors which are not supported yet.
