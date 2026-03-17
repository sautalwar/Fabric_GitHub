# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters
target_environment = "Dev"  # Dev, UAT, or Prod
expected_version = "v002"   # Expected migration version

# CELL ********************

from pyspark.sql import functions as F


def version_number(version_text: str) -> int:
    digits = "".join(character for character in str(version_text) if character.isdigit())
    return int(digits) if digits else 0


def print_section(title: str) -> None:
    print(f"\n{'═' * 55}\n{title}\n{'═' * 55}")


def normalize_type(type_name: str) -> str:
    normalized = str(type_name).strip().lower()
    aliases = {
        "integer": "int",
        "bool": "boolean",
    }
    return aliases.get(normalized, normalized)


def get_table_schema(table_name: str) -> dict:
    describe_rows = spark.sql(f"DESCRIBE TABLE `{table_name}`").collect()
    schema = {}

    for row in describe_rows:
        column_name = (row["col_name"] or "").strip()
        data_type = (row["data_type"] or "").strip()

        if not column_name or column_name.startswith("#"):
            break

        schema[column_name.lower()] = normalize_type(data_type)

    return schema


def get_available_tables() -> set:
    show_tables_df = spark.sql("SHOW TABLES")
    return {row["tableName"].lower() for row in show_tables_df.collect()}


def table_exists(table_name: str, available_tables: set) -> bool:
    return table_name.lower() in available_tables or spark.catalog.tableExists(table_name)


def record_result(store: dict, category: str, passed: bool, message: str) -> None:
    store[category]["total"] += 1
    if passed:
        store[category]["passed"] += 1
    else:
        store[category]["failures"].append(message)

    status_icon = "✅" if passed else "❌"
    print(f"{status_icon} {message}")


def search_history_for_version(history_df, version_text: str) -> bool:
    searchable_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit(""))
        for column_name in history_df.columns
    ]

    history_search_df = history_df.select(
        F.lower(F.concat_ws("||", *searchable_columns)).alias("search_text")
    )

    return history_search_df.filter(F.col("search_text").contains(version_text.lower())).limit(1).count() > 0


expected_schema = {
    "data_centers": {
        "dc_id": "string",
        "dc_name": "string",
        "region": "string",
        "city": "string",
        "country": "string",
        "total_capacity_kw": "double",
        "commissioned_date": "date",
        "tier_level": "int",
        "status": "string",
    },
    "power_consumption": {
        "record_id": "string",
        "dc_id": "string",
        "measurement_timestamp": "timestamp",
        "total_power_kw": "double",
        "it_load_kw": "double",
        "cooling_load_kw": "double",
        "lighting_load_kw": "double",
        "pue_ratio": "double",
    },
    "cooling_metrics": {
        "record_id": "string",
        "dc_id": "string",
        "measurement_timestamp": "timestamp",
        "supply_temp_celsius": "double",
        "return_temp_celsius": "double",
        "humidity_pct": "double",
        "chiller_efficiency": "double",
        "cooling_method": "string",
    },
}

v002_schema = {
    "capacity_utilization": {
        "record_id": "string",
        "dc_id": "string",
        "measurement_date": "date",
        "total_racks": "int",
        "occupied_racks": "int",
        "utilization_pct": "double",
        "reserved_racks": "int",
        "available_power_kw": "double",
    },
    "sla_incidents": {
        "incident_id": "string",
        "dc_id": "string",
        "incident_start": "timestamp",
        "incident_end": "timestamp",
        "severity": "string",
        "affected_systems": "string",
        "root_cause": "string",
        "resolution_notes": "string",
        "downtime_minutes": "double",
        "sla_breached": "boolean",
    },
}

if version_number(expected_version) >= 2:
    expected_schema["power_consumption"].update(
        {
            "renewable_pct": "double",
            "carbon_intensity_kg": "double",
        }
    )
    expected_schema.update(v002_schema)

required_tables = list(expected_schema.keys())
expected_versions = ["v001"]
if version_number(expected_version) >= 2:
    expected_versions.append("v002")

results = {
    "tables": {"passed": 0, "total": 0, "failures": []},
    "columns": {"passed": 0, "total": 0, "failures": []},
    "migration": {"passed": 0, "total": 0, "failures": []},
}

data_failures = []

print_section(f"Fabric Lakehouse Schema Validation — {target_environment}")
print(f"Target environment: {target_environment}")
print(f"Expected migration version: {expected_version}")

available_tables = get_available_tables()
existing_tables = []
row_counts = {}

print_section("1. Table Existence Check")
for table_name in required_tables:
    exists = table_exists(table_name, available_tables)
    record_result(results, "tables", exists, f"Table `{table_name}` exists")
    if exists:
        existing_tables.append(table_name)

print_section("2. Column Validation")
for table_name in required_tables:
    if not table_exists(table_name, available_tables):
        print(f"❌ Skipping column validation for `{table_name}` because the table does not exist")
        continue

    actual_schema = get_table_schema(table_name)
    for column_name, expected_type in expected_schema[table_name].items():
        actual_type = actual_schema.get(column_name)
        passed = actual_type == expected_type
        details = f"expected {expected_type}, found {actual_type or 'missing'}"
        record_result(
            results,
            "columns",
            passed,
            f"`{table_name}.{column_name}` -> {details}",
        )

print_section("3. v002-Specific Checks")
if version_number(expected_version) >= 2:
    power_schema = get_table_schema("power_consumption") if table_exists("power_consumption", available_tables) else {}
    renewable_ok = power_schema.get("renewable_pct") == "double"
    carbon_ok = power_schema.get("carbon_intensity_kg") == "double"
    capacity_ok = table_exists("capacity_utilization", available_tables)
    incidents_ok = table_exists("sla_incidents", available_tables)

    v002_checks = [
        (renewable_ok, "power_consumption includes `renewable_pct` (DOUBLE)"),
        (carbon_ok, "power_consumption includes `carbon_intensity_kg` (DOUBLE)"),
        (capacity_ok, "Table `capacity_utilization` exists for v002"),
        (incidents_ok, "Table `sla_incidents` exists for v002"),
    ]

    for passed, message in v002_checks:
        status_icon = "✅" if passed else "❌"
        print(f"{status_icon} {message}")
        if not passed:
            data_failures.append(message)
else:
    print("ℹ️ Expected version is below v002, so v002-specific checks are skipped")

print_section("4. Row Count Check")
for table_name in existing_tables:
    row_count = spark.table(table_name).count()
    row_counts[table_name] = row_count
    if row_count > 0:
        print(f"✅ `{table_name}` row count: {row_count}")
    else:
        print(f"⚠️ `{table_name}` row count: 0 (schema validated, no sample data found)")

print_section("5. Data Integrity Checks")
key_columns = {
    "data_centers": ["dc_id"],
    "power_consumption": ["record_id", "dc_id"],
    "cooling_metrics": ["record_id", "dc_id"],
    "capacity_utilization": ["record_id", "dc_id"],
    "sla_incidents": ["incident_id", "dc_id"],
}

for table_name, columns in key_columns.items():
    if table_name not in existing_tables:
        continue

    actual_schema = get_table_schema(table_name)
    for column_name in columns:
        if column_name not in actual_schema:
            continue
        null_count = spark.table(table_name).filter(F.col(column_name).isNull()).count()
        passed = null_count == 0
        status_icon = "✅" if passed else "❌"
        message = f"`{table_name}.{column_name}` null check -> {null_count} null rows"
        print(f"{status_icon} {message}")
        if not passed:
            data_failures.append(message)

if "power_consumption" in existing_tables:
    invalid_pue = (
        spark.table("power_consumption")
        .filter(F.col("pue_ratio").isNotNull() & ((F.col("pue_ratio") < 1.0) | (F.col("pue_ratio") > 3.0)))
        .count()
    )
    pue_passed = invalid_pue == 0
    pue_message = f"`power_consumption.pue_ratio` range check -> {invalid_pue} invalid rows"
    print(f"{'✅' if pue_passed else '❌'} {pue_message}")
    if not pue_passed:
        data_failures.append(pue_message)

if "capacity_utilization" in existing_tables:
    invalid_utilization = (
        spark.table("capacity_utilization")
        .filter(
            F.col("utilization_pct").isNotNull()
            & ((F.col("utilization_pct") < 0) | (F.col("utilization_pct") > 100))
        )
        .count()
    )
    utilization_passed = invalid_utilization == 0
    utilization_message = f"`capacity_utilization.utilization_pct` range check -> {invalid_utilization} invalid rows"
    print(f"{'✅' if utilization_passed else '❌'} {utilization_message}")
    if not utilization_passed:
        data_failures.append(utilization_message)

print_section("6. Migration History Check")
migration_history_exists = table_exists("_migration_history", available_tables)
record_result(results, "migration", migration_history_exists, "Table `_migration_history` exists")

migration_ok = migration_history_exists
if migration_history_exists:
    migration_history_df = spark.table("_migration_history")
    history_row_count = migration_history_df.count()
    print(f"ℹ️ `_migration_history` row count: {history_row_count}")

    for version_name in expected_versions:
        version_found = search_history_for_version(migration_history_df, version_name)
        record_result(results, "migration", version_found, f"Migration history contains `{version_name}`")
        migration_ok = migration_ok and version_found
else:
    migration_ok = False

all_tables_passed = results["tables"]["passed"] == results["tables"]["total"]
all_columns_passed = results["columns"]["passed"] == results["columns"]["total"]
all_data_passed = len(data_failures) == 0
all_migration_passed = migration_ok and results["migration"]["passed"] == results["migration"]["total"]
overall_passed = all_tables_passed and all_columns_passed and all_data_passed and all_migration_passed

print("\n" + "═" * 39)
print(f"Schema Validation Report — {target_environment}")
print("═" * 39)
print(
    f"Tables:     {results['tables']['passed']}/{results['tables']['total']} "
    f"{'✅' if all_tables_passed else '❌'}"
)
print(
    f"Columns:    {results['columns']['passed']}/{results['columns']['total']} "
    f"{'✅' if all_columns_passed else '❌'}"
)
print(f"Data:       {'All checks passed ✅' if all_data_passed else 'Issues found ❌'}")
print(f"Migration:  {expected_version} applied {'✅' if all_migration_passed else '❌'}")
print("═" * 39)
print(f"RESULT: {'PASS ✅' if overall_passed else 'FAIL ❌'}")

if not overall_passed:
    failure_messages = (
        results["tables"]["failures"]
        + results["columns"]["failures"]
        + data_failures
        + results["migration"]["failures"]
    )
    raise Exception(
        "Schema validation failed for "
        f"{target_environment}. Issues: {'; '.join(failure_messages)}"
    )
