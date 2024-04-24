from itertools import chain

from hedgepy.common.utils import dtwrapper


def _contrast_keys(basis: dict, other: dict) -> tuple[dict, dict]:
    discrepancy = {}
    for key in basis:
        if key not in other:
            discrepancy[key] = {}
    return discrepancy


def _compare_keys(basis: dict, other: dict) -> tuple[dict, dict]:
    common = {}
    for key in basis:
        if key in other:
            common[key] = {}
    return common


def _compare_schema(expected: dict[dict], actual: dict[dict]) -> tuple[dict, dict, dict]:
    missing_schemas = _contrast_keys(expected, actual)
    orphaned_schemas = _contrast_keys(actual, expected)
    common_schemas = _compare_keys(expected, actual)
    return missing_schemas, orphaned_schemas, common_schemas


def _compare_tables(
    expected: dict[dict], 
    actual: dict[dict], 
    missing: dict, 
    orphaned: dict, 
    common: dict) -> tuple[dict, dict, dict]:
    for schema_name in set(chain(expected.keys(), actual.keys())):
        expected_tables = expected.get(schema_name, {})
        actual_tables = actual.get(schema_name, {})
        if len(missing_tables := _contrast_keys(expected_tables, actual_tables)) > 0:
            missing[schema_name] = missing_tables
        if len(orphaned_tables := _contrast_keys(actual_tables, expected_tables)) > 0:
            orphaned[schema_name] = orphaned_tables
        if len(common_tables := _compare_keys(expected_tables, actual_tables)) > 0:
            common[schema_name] = common_tables
    return missing, orphaned, common


def _compare_columns_and_dates(
    expected: dict[dict], 
    actual: dict[dict], 
    missing: dict, 
    orphaned: dict, 
    common: dict) -> tuple[dict, dict, dict]:
    for schema_name in set(chain(expected.keys(), actual.keys())):
        expected_schema = expected.get(schema_name, {})
        actual_schema = actual.get(schema_name, {})
        for table_name in set(chain(expected_schema.keys(), actual_schema.keys())):
            expected_table = expected_schema.get(table_name, {})
            actual_table = actual_schema.get(table_name, {})
            
            expected_columns = expected_table.get("columns", [])
            actual_columns = actual_table.get("columns", [])
            missing_columns = [column for column in expected_columns if column not in actual_columns]
            orphaned_columns = [column for column in actual_columns if column not in expected_columns]
            common_columns = [column for column in expected_columns if column in actual_columns]
            
            if missing_columns:
                missing[schema_name][table_name]["columns"] = missing_columns
            if orphaned_columns:
                orphaned[schema_name][table_name]["columns"] = orphaned_columns
            if common_columns:
                common[schema_name][table_name]["columns"] = common_columns
            
            expected_start, expected_end = expected_table.get("date_range", (None, None))
            actual_start, actual_end = actual_table.get("date_range", (None, None))
            orphaned_start, missing_start, common_start = None, None, None
            orphaned_end, missing_end, common_end = None, None, None

            if expected_start:
                if actual_start:
                    if dtwrapper.str_to_dt(expected_start) < dtwrapper.str_to_dt(actual_start):
                        missing_start = (expected_start, actual_start)
                    elif dtwrapper.str_to_dt(expected_start) > dtwrapper.str_to_dt(actual_start):
                        orphaned_start = (actual_start, expected_start)
                    else:
                        common_start = actual_start = expected_start
                else:
                    missing_start = (expected_start, None)
            if expected_end:
                if actual_end:
                    if dtwrapper.str_to_dt(expected_end) > dtwrapper.str_to_dt(actual_end):
                        missing_end = (actual_end, expected_end)
                    elif dtwrapper.str_to_dt(expected_end) < dtwrapper.str_to_dt(actual_end):
                        orphaned_end = (expected_end, actual_end)
                    else:
                        common_end = actual_end = expected_end
                else:
                    missing_end = (None, expected_end)

            if orphaned_start or orphaned_end:
                orphaned[schema_name][table_name]["date_range"] = {"start": orphaned_start, "end": orphaned_end}
            if missing_start or missing_end:
                missing[schema_name][table_name]["date_range"] = {"start": missing_start, "end": missing_end}
            if common_start or common_end:
                common[schema_name][table_name]["date_range"] = {"start": common_start, "end": common_end}
                
    return missing, orphaned, common


def diff(expected: dict[dict], actual: dict[dict]) -> tuple[dict, dict]:
    missing, orphaned, common = _compare_schema(actual, expected)
    missing, orphaned, common = _compare_tables(actual, expected, missing, orphaned, common)
    missing, orphaned, common = _compare_columns_and_dates(actual, expected, missing, orphaned, common)
    return missing, orphaned, common
    