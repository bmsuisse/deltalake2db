import logging
import json
from datetime import date, datetime
from typing import Mapping, Optional, Any, Sequence, Union, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from deltalake2db.delta_meta_retrieval import PrimitiveType


logger = logging.getLogger(__name__)


def _partition_value_to_python(value: str, type: "PrimitiveType"):
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


def _serialize_partition_value(value, type: "PrimitiveType"):
    if isinstance(value, (list, tuple, set)):
        return [_serialize_partition_value(v, type) for v in value]
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


Operator = Literal["<", "=", ">", ">=", "<=", "<>", "in", "not in"]


def _can_value_filter(
    value, num_records, null_count, min_vl, max_vl
) -> Optional[Literal[True]]:
    if value is not None and (num_records is not None and num_records == null_count):
        return True
    if null_count == 0 and value is None:
        return True
    if isinstance(min_vl, str) and isinstance(value, str):
        value = value[0 : len(min_vl)]
    if (
        min_vl is not None and max_vl is not None and (value < min_vl or value > max_vl)  # type: ignore
    ):
        return True
    return None


FilterTypeOld = Mapping[str, Any]
FilterType = Sequence[tuple[str, Operator, Any]]


def to_new_filter_type(conditions: Union[FilterTypeOld, FilterType]) -> FilterType:
    if isinstance(conditions, Sequence):
        return conditions
    return [(k, "=", v) for k, v in conditions.items()]


def _can_filter(
    action: dict,
    conditions: FilterType,
    typeMap: "Mapping[str, PrimitiveType]",
    logical2physical: "Mapping[str, str]",
) -> bool:
    # see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics
    try:
        typeMap = typeMap or {}
        for logical_name, operator, value in conditions:
            physical_name = logical2physical.get(logical_name, logical_name)
            part_type = typeMap.get(logical_name, "string")
            part_vls = _to_dict(action.get("partitionValues", {}))
            if physical_name in part_vls:
                part_vl = part_vls.get(physical_name, None)
                if operator == "in":
                    assert isinstance(value, (list, tuple, set, Sequence))
                    serialized_value = _serialize_partition_value(value, part_type)
                    if part_vl not in serialized_value:  # type: ignore
                        return True
                elif operator == "=":
                    if part_vl != _serialize_partition_value(value, part_type):
                        return True
                elif operator == "<>":
                    if part_vl == _serialize_partition_value(value, part_type):
                        return True
                elif operator in [">", ">=", "<", "<="] and part_vl is None:
                    return True
                elif operator in [">", ">=", "<", "<="] and isinstance(
                    value, (float, int)
                ):
                    if isinstance(value, float):
                        part_vl = float(part_vl)  # type: ignore
                    else:
                        part_vl = int(part_vl)  # type: ignore
                    if operator == ">" and part_vl <= value:
                        return True
                    elif operator == ">=" and part_vl < value:
                        return True
                    elif operator == "<" and part_vl >= value:
                        return True
                    elif operator == "<=" and part_vl > value:
                        return True
                elif operator == "not in":
                    assert isinstance(value, (list, tuple, set, Sequence))
                    serialized_value = _serialize_partition_value(value, part_type)
                    if part_vl in serialized_value:  # type: ignore
                        return True

            stats = {}
            if action.get("stats"):
                stats = action["stats"]
                if isinstance(stats, str):
                    stats = json.loads(stats)
                if stats.get("numRecords", 0) == 0:
                    return True
            num_records = stats.get("numRecords", None)
            min_vl = stats.get("minValues", {}).get(physical_name, None)
            max_vl = stats.get("maxValues", {}).get(physical_name, None)
            null_count = stats.get("nullCount", {}).get(physical_name, None)
            if num_records == 0:
                return True
            if operator == "in":
                assert isinstance(value, (list, tuple, set))
                if all(
                    _can_value_filter(v, num_records, null_count, min_vl, max_vl)
                    for v in value
                ):
                    return True
            elif operator == "=":
                if _can_value_filter(value, num_records, null_count, min_vl, max_vl):
                    return True
            elif operator == "<" and min_vl is not None and value <= min_vl:
                return True
            elif operator == "<=" and min_vl is not None and value < min_vl:
                return True
            elif operator == ">" and max_vl is not None and value >= max_vl:
                return True
            elif operator == ">=" and max_vl is not None and value > max_vl:
                return True
            elif operator == "not in":
                assert isinstance(value, (list, tuple, set))
                if any(
                    _can_value_filter(v, num_records, null_count, min_vl, max_vl)
                    for v in value
                ):
                    return True

        return False
    except Exception as e:
        logger.warning(f"Could not filter: {e}")
        return False
