from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deltalake2db.delta_meta_retrieval import MetaState


supported_reader_features = [
    "columnMapping",
    "timestampNtz",
    "typeWidening",
    "vacuumProtocolCheck",
]  # not: deletionVectors, v2Checkpoint


def is_protocol_supported(dt: "MetaState") -> bool:
    prot = dt.protocol
    assert prot is not None
    if prot["minReaderVersion"] <= 3:
        return True
    assert prot["readerFeatures"] is not None
    un_supported = [
        f for f in prot["readerFeatures"] if f not in supported_reader_features
    ]
    return not any(un_supported)


class DeltaProtocolError(Exception):
    pass


def check_is_supported(dt: "MetaState") -> None:
    prot = dt.protocol
    assert prot is not None
    if prot["minReaderVersion"] <= 3:
        return
    assert prot["readerFeatures"] is not None
    un_supported = [
        f for f in prot["readerFeatures"] if f not in supported_reader_features
    ]
    if not any(un_supported):
        return
    raise DeltaProtocolError(
        f"Delta table features not supported: {', '.join(un_supported)}"
    )
