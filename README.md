# Deltalake2DB

This is a simple project that uses Metadata from `deltalake` package to provide methods to read Delta Lake Tables
to either Polars or DuckDB with better Protocol Support as the main `deltalake` package.

## Use with Duckdb

Install `deltalake2db` and `duckdb` using pip/poetry/whatever you use.

Then you can do like this:

```python

from deltalake2db import get_sql_for_delta,

with duckdb.connect() as con:
    dt = DeltaTable("tests/data/faker2")
    sql = get_sql_for_delta(dt, duck_con=con) # get select statement
    print(sql)
    duckdb_create_view_for_delta(con, dt, "delta_table") # or let it create a view for you. will point to the data at this point in time

    con.execute("select * from delta_table").fetch_all()
```

If you'd like to manipulate you can use `get_sql_for_delta_expr` which returns a SqlGlot Object

## Use with Polars

Install `deltalake2db` and `polars>=1.12` using pip/poetry/whatever you use.

```python
dt = DeltaTable("tests/data/faker2")
from deltalake2db import polars_scan_delta
lazy_df = polars_scan_delta(dt)
df = lazy_df.collect()

```

## Protocol Support

- [x] Column Mapping
- [x] Almost Data Types, including Structs/Lists, Map yet to be done
- [ ] Test data types, including datetime
- [ ] Deletion Vectors

In case there is an unsupported DeltaLake Feature, this will just throw `DeltaProtocolError` as does delta-rs

## Cloud Support

For now, only az:// Url's for Azure are tested and supported in DuckDB. For polars it's a lot easier, since polars just uses `object_store` create, so it should just work.

The package does some work to make DuckDB's "Azure Storage Options" work in Polars, to be able to use the same options.

This means you can:

- pass an absolute DuckDB-style Path to Polars, meaning something like `abfss://⟨my_storage_account⟩.dfs.core.windows.net/⟨my_filesystem⟩/⟨path⟩`
- pass "chain" as option, which will act like [DuckDB's Credential Chain](https://duckdb.org/docs/extensions/azure.html#credential_chain-provider). This requires `azure-identity` Package

## Looking for something different? :)

We also have the following projects around deltalake:

- [LakeAPI](https://github.com/bmsuisse/lakeapi) for providing deltalake Tables
- [Odbc2deltalake](https://github.com/bmsuisse/odbc2deltalake) to load MS SQL Server/ODBC Tables to Deltalake

Or projects from other people:

- [polars-deltalake](https://github.com/ion-elgreco/polars-deltalake) An experimental native polars deltalake reader
