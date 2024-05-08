import logging

logger = logging.getLogger(__name__)


def _can_filter(action: dict, conditions: dict):
    # partition.COLNAME
    # min.COLNAME
    # max.COLNAME
    try:
        for key, value in conditions.items():
            part_vl = action.get("partition." + key, None)
            if part_vl is not None and part_vl != value:
                return True
            min_vl = action.get("min." + key, None)
            max_vl = action.get("max." + key, None)
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
