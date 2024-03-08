from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError


supported_reader_features = [
    "columnMapping",
    "timestampNtz",
]  # not: deletionVectors, v2Checkpoint


def is_protocol_supported(dt: DeltaTable):
    prot = dt.protocol()
    if prot.min_reader_version <= 3:
        return True
    assert prot.reader_features is not None
    un_supported = [
        f for f in prot.reader_features if f not in supported_reader_features
    ]
    return not any(un_supported)


def check_is_supported(dt: DeltaTable):
    prot = dt.protocol()
    if prot.min_reader_version <= 3:
        return
    assert prot.reader_features is not None
    un_supported = [
        f for f in prot.reader_features if f not in supported_reader_features
    ]
    if not any(un_supported):
        return
    raise DeltaProtocolError(
        f"Delta table features not supported: {', '.join(un_supported)}"
    )
