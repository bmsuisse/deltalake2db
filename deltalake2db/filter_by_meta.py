import logging
import json

logger = logging.getLogger(__name__)


def _can_filter(action: dict, conditions: dict):
    # see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics
    try:
        for key, value in conditions.items():
            part_vl = action.get("partitionValues", {}).get(key, None)
            if part_vl is not None and part_vl != value:
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
            if isinstance(min_vl, str):
                value = value[0 : len(min_vl)]
            if (
                min_vl is not None
                and max_vl is not None
                and (value < min_vl or value > max_vl)
            ):
                return True
        return False
    except Exception as e:
        logger.warning(f"Could not filter: {e}")
        return False
