from typing import (
    Sequence,
    Union,
    Protocol,
    TYPE_CHECKING,
    TypedDict,
    Literal,
    Optional,
)
import json

if TYPE_CHECKING:
    import duckdb
    import pyarrow.fs as pafs

PrimitiveType = Literal[
    "string",
    "integer",
    "long",
    "float",
    "double",
    "boolean",
    "binary",
    "date",
    "timestamp",
    "timestamp without time zone",
    "decimal",
]


class PrimitiveField(TypedDict):
    name: str
    type: PrimitiveType


class MapType(TypedDict):
    type: Literal["map"]
    keyType: PrimitiveType
    valueType: "Union[StructType, PrimitiveType, ArrayType, MapType]"
    valueContainsNull: bool


class MapField(MapType):
    name: str


class ArrayType(TypedDict):
    type: Literal["array"]
    elementType: "Union[StructType, PrimitiveType, ArrayType, MapType]"


class ArrayField(ArrayType):
    name: str


Field = Union["StructField", "PrimitiveField", "ArrayField", "MapField"]


class StructType(TypedDict):
    type: Literal["struct"]
    fields: "Sequence[Field]"


class StructField(StructType):
    name: str


DataType = Union[StructType, PrimitiveType, ArrayType, MapType]


def is_primitive_type(t: DataType) -> bool:
    return isinstance(t, str)


class DeltaProtocol(TypedDict):
    minReaderVersion: int
    minWriterVersion: int
    readerFeatures: Optional[Sequence[str]]
    writerFeatures: Optional[Sequence[str]]


class MetaState:
    last_metadata: Union[dict, None] = None
    protocol: Union[DeltaProtocol, None] = None
    add_actions: dict[str, dict] = {}
    last_commit_info: Union[dict, None] = None
    version: int = 0

    @property
    def last_write_time(self):
        from datetime import datetime, timezone

        assert self.last_commit_info is not None
        ts = self.last_commit_info.get("timestamp", None)
        assert ts is not None
        return datetime.fromtimestamp(ts / 1000.0, timezone.utc)

    @property
    def schema(self) -> Union[StructType, None]:
        if self.last_metadata:
            sc = self.last_metadata.get("schemaString", None)
            if sc:
                return json.loads(sc)

        return None

    def __init__(self) -> None:
        self.last_metadata = None
        self.add_actions = {}


def process_meta_data(actions: dict, state: MetaState, version: int):
    if actions.get("metaData"):
        state.last_metadata = actions["metaData"]
    if actions.get("protocol"):
        state.protocol = actions["protocol"]
    if actions.get("add"):
        path = actions["add"]["path"]
        state.add_actions[path] = actions["add"]
    if actions.get("commitInfo"):
        state.last_commit_info = actions["commitInfo"]
    if actions.get("remove"):
        path = actions["remove"]["path"]
        state.add_actions.pop(path, None)
    state.version = version


class MetadataEngine(Protocol):
    def read_jsonl(self, path: str) -> Sequence[dict]: ...

    def read_parquet(self, path: str) -> Sequence[dict]: ...


class PyArrowEngine(MetadataEngine):
    def __init__(self, fs: "Optional[pafs.FileSystem]" = None) -> None:
        super().__init__()
        self.fs = fs or pafs.LocalFileSystem()

    def read_jsonl(self, path: str) -> Sequence[dict]:
        import json

        result = []
        with self.fs.open_input_stream(path) as f:
            for line in f.readlines():
                result.append(json.loads(line))
        return result

    def read_parquet(self, path: str) -> Sequence[dict]:
        import pyarrow.parquet as pq

        with self.fs.open_input_stream(path) as f:
            table = pq.read_table(f)
            return table.to_pylist()


class PolarsEngine(MetadataEngine):
    def __init__(self, storage_options: Optional[dict]) -> None:
        super().__init__()
        self.storage_options = storage_options

    def read_jsonl(self, path: str) -> Sequence[dict]:
        try:
            import polars as pl

            df = pl.read_ndjson(path, storage_options=self.storage_options)
            return df.to_dicts()
        except OSError as e:
            if "404" in str(e) or "No such file or directory" in str(e):
                raise FileNotFoundError from e
            raise

    def read_parquet(self, path: str) -> Sequence[dict]:
        try:
            import polars as pl

            df = pl.read_parquet(path, storage_options=self.storage_options)
            return df.to_dicts()
        except OSError as e:
            if "BlobNotFound" in str(e):
                raise FileNotFoundError from e
            if "404" in str(e) or "No such file or directory" in str(e):
                raise FileNotFoundError from e
            raise


class DuckDBEngine(MetadataEngine):
    def __init__(self, con: "duckdb.DuckDBPyConnection") -> None:
        super().__init__()
        self.con = con

    def read_jsonl(self, path: str) -> Sequence[dict]:
        import duckdb

        q = f"SELECT * FROM read_json('{path}', format='newline_delimited')"
        with self.con.cursor() as cur:
            try:
                cur.execute(q)
                assert cur.description is not None
                desc = [d[0] for d in cur.description]
                return [dict(zip(desc, row)) for row in cur.fetchall()]
            except duckdb.IOException as e:
                if "BlobNotFound" in str(e):
                    raise FileNotFoundError from e
                if "No files found" in str(e):
                    raise FileNotFoundError from e
                raise

    def read_parquet(self, path: str) -> Sequence[dict]:
        import duckdb

        q = f"SELECT * FROM read_parquet('{path}')"
        with self.con.cursor() as cur:
            try:
                cur.execute(q)
                assert cur.description is not None
                desc = [d[0] for d in cur.description]
                return [dict(zip(desc, row)) for row in cur.fetchall()]
            except duckdb.IOException as e:
                if "No files found" in str(e):
                    raise FileNotFoundError from e
                raise


def _delta_fn(version: int) -> str:
    return f"{version:020d}"


def field_to_type(field: Field) -> DataType:
    if field["type"] == "struct":
        return field
    elif field["type"] == "array":
        return field
    elif field["type"] == "map":
        return field
    else:
        return field["type"]


def get_meta(
    engine: MetadataEngine, delta_path: str, *, version: Optional[int] = None
) -> MetaState:
    try:
        checkpoint = engine.read_jsonl(
            delta_path.rstrip("/") + "/_delta_log/_last_checkpoint"
        )[0]
    except FileNotFoundError:
        checkpoint = None
    state = MetaState()
    if checkpoint:
        check_point_version = checkpoint.get("version", 0)
        if version is not None and version < check_point_version:
            check_point_version = (
                version - version % 10
            )  # nearest lower multiple of 10, since most engines write checkpoints every 10 versions

        try:
            check_point_file = (
                delta_path.rstrip("/")
                + f"/_delta_log/{_delta_fn(check_point_version)}.checkpoint.parquet"
            )
            check_point_data = engine.read_parquet(check_point_file)
            for action in check_point_data:
                process_meta_data(action, state, check_point_version)
            start_version = check_point_version + 1
        except FileNotFoundError:
            start_version = 0
    else:
        start_version = 0
    current_version = start_version
    while version is None or current_version <= version:
        commit_file = (
            delta_path.rstrip("/") + f"/_delta_log/{_delta_fn(current_version)}.json"
        )
        try:
            commit_data = engine.read_jsonl(commit_file)
        except FileNotFoundError:
            break
        for action in commit_data:
            process_meta_data(action, state, current_version)
        current_version += 1
    return state
