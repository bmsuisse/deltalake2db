import logging
import json
from datetime import date, datetime
from typing import Mapping, Optional, Any

from deltalake2db.delta_meta_retrieval import PrimitiveType, StructType

logger = logging.getLogger(__name__)


def _partition_value_to_python(value: str, type: PrimitiveType):
    if value is None:
        return None
    if type == "string":
        return value
    if type in ["integer", "byte", "short"]:
        return int(value)
    if type == "long":
        return int(value)
    if type == "float":
        return float(value)
    if type == "double":
        return float(value)
    if type == "boolean":
        return value.lower() in ("true", "1", "t", "y", "yes")
    if type == "date":
        return date.fromisoformat(value)
    if type in ("timestamp", "timestamp_ntz"):
        return datetime.fromisoformat(value)
    if type == "binary":
        return bytes.fromhex(value)
    if type == "decimal":
        from decimal import Decimal

        return Decimal(value)
    if type.startswith("decimal(") and type.endswith(")"):
        from decimal import Decimal

        return Decimal(value)
    raise ValueError(f"Unknown partition type: {type}")


def _serialize_partition_value(value, type: PrimitiveType):
    # implements https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    if value is None:
        return None
    if type == "string":
        return value
    if type in ["integer", "byte", "short", "long"]:
        return str(value)
    if type == "date":
        return value.isoformat() if isinstance(value, date) else str(value)
    if type in ("timestamp", "timestamp_ntz"):
        # {year}-{month}-{day} {hour}:{minute}:{second} or {year}-{month}-{day} {hour}:{minute}:{second}.{microsecond}
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return str(value)
    if type == "boolean":
        return "true" if value else "false"
    if type == "binary":
        # code as "\u0001\u0002\u0003"
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        return str(value)
    return value


def _to_dict(pv):
    if isinstance(pv, list):
        return {k["key"]: k["value"] for k in pv}
    return pv


def _can_filter(
    action: dict,
    conditions: Mapping[str, Any],
    typeMap: Optional[Mapping[str, PrimitiveType]] = None,
) -> bool:
    # see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics
    try:
        typeMap = typeMap or {}
        for key, value in conditions.items():
            part_type = typeMap.get(key, "string")
            part_vl = _to_dict(action.get("partitionValues", {})).get(key, None)
            serialized_value = _serialize_partition_value(value, part_type)
            if part_vl is not None and part_vl != serialized_value:
                return True
            stats = {}
            if action.get("stats"):
                stats = action["stats"]
                if isinstance(stats, str):
                    stats = json.loads(stats)
                if stats.get("numRecords", 0) == 0:
                    return True
            min_vl = stats.get("minValues", {}).get(key, None)
            max_vl = stats.get("maxValues", {}).get(key, None)
            null_count = stats.get("nullCount", {}).get(key, None)
            if null_count == 0 and value is None:
                return True
            if isinstance(min_vl, str) and isinstance(value, str):
                value = value[0 : len(min_vl)]
            if (
                min_vl is not None
                and max_vl is not None
                and (value < min_vl or value > max_vl)  # type: ignore
            ):
                return True
        return False
    except Exception as e:
        logger.warning(f"Could not filter: {e}")
        return False
