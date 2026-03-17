"""Migration v002: extend the Digital Realty data center operations demo with capacity, SLA, and sustainability schema changes."""

from delta.tables import DeltaTable


def _create_table(table_name: str, ddl: str) -> None:
    try:
        spark.sql(ddl)
        print(f"✅ Created table: {table_name}")
    except Exception as create_error:
        print(f"❌ Failed to create table {table_name}: {create_error}")
        raise


def _table_has_rows(table_name: str) -> bool:
    return DeltaTable.forName(spark, table_name).toDF().limit(1).count() > 0


def _seed_table(table_name: str, insert_sql: str) -> None:
    try:
        if _table_has_rows(table_name):
            print(f"ℹ️ Skipping sample data for {table_name}; rows already exist.")
            return

        spark.sql(insert_sql)
        row_count = DeltaTable.forName(spark, table_name).toDF().count()
        print(f"✅ Inserted sample data into {table_name} ({row_count} rows total)")
    except Exception as seed_error:
        print(f"❌ Failed to seed table {table_name}: {seed_error}")
        raise


def _add_sustainability_columns() -> None:
    try:
        if not spark.catalog.tableExists("power_consumption"):
            raise ValueError("Required table 'power_consumption' does not exist. Run v001_baseline_tables.py first.")

        existing_columns = {field.name for field in spark.table("power_consumption").schema.fields}
        missing_columns = []

        if "renewable_pct" not in existing_columns:
            missing_columns.append("renewable_pct DOUBLE COMMENT 'Percentage of consumed power sourced from renewable energy.'")
        if "carbon_intensity_kg" not in existing_columns:
            missing_columns.append("carbon_intensity_kg DOUBLE COMMENT 'Carbon intensity in kilograms of CO2 per kilowatt-hour.'")

        if missing_columns:
            spark.sql(
                f"""
                ALTER TABLE power_consumption
                ADD COLUMNS (
                    {', '.join(missing_columns)}
                )
                """
            )
            print("✅ Added sustainability columns to table: power_consumption")
        else:
            print("ℹ️ Sustainability columns already exist on table: power_consumption")
    except Exception as alter_error:
        print(f"❌ Failed to evolve table power_consumption: {alter_error}")
        raise


try:
    _create_table(
        "capacity_utilization",
        """
        CREATE TABLE IF NOT EXISTS capacity_utilization (
            record_id STRING COMMENT 'Logical unique identifier for each capacity utilization record.',
            dc_id STRING COMMENT 'Logical foreign key reference to data_centers.dc_id.',
            measurement_date DATE COMMENT 'Date for the utilization snapshot.',
            total_racks INT COMMENT 'Total number of racks provisioned in the facility.',
            occupied_racks INT COMMENT 'Number of racks currently occupied.',
            utilization_pct DOUBLE COMMENT 'Percentage of rack capacity currently utilized.',
            reserved_racks INT COMMENT 'Number of racks reserved for upcoming customer deployments.',
            available_power_kw DOUBLE COMMENT 'Remaining available power capacity in kilowatts.'
        )
        USING DELTA
        COMMENT 'Daily rack-level capacity and power availability metrics for Digital Realty data centers. record_id is the logical key and dc_id logically references data_centers.'
        """,
    )

    _create_table(
        "sla_incidents",
        """
        CREATE TABLE IF NOT EXISTS sla_incidents (
            incident_id STRING COMMENT 'Logical primary key for each incident record.',
            dc_id STRING COMMENT 'Logical foreign key reference to data_centers.dc_id.',
            incident_start TIMESTAMP COMMENT 'Timestamp when the incident began.',
            incident_end TIMESTAMP COMMENT 'Timestamp when the incident ended.',
            severity STRING COMMENT 'Incident severity classification: critical, major, or minor.',
            affected_systems STRING COMMENT 'Description of systems or services impacted by the incident.',
            root_cause STRING COMMENT 'Documented root cause of the incident.',
            resolution_notes STRING COMMENT 'Summary of remediation and resolution steps taken.',
            downtime_minutes DOUBLE COMMENT 'Total downtime attributed to the incident in minutes.',
            sla_breached BOOLEAN COMMENT 'Indicates whether the incident breached the applicable SLA.'
        )
        USING DELTA
        COMMENT 'Incident and uptime tracking for Digital Realty facilities. incident_id is the logical key and dc_id logically references data_centers.'
        """,
    )

    _add_sustainability_columns()

    _seed_table(
        "capacity_utilization",
        """
        INSERT INTO capacity_utilization VALUES
            ('CAP-0001', 'DC-ASH-001', CAST('2026-03-12' AS DATE), 1200, 1080, 90.0, 60, 1750.0),
            ('CAP-0002', 'DC-DFW-001', CAST('2026-03-12' AS DATE), 980, 745, 76.0, 55, 2200.0),
            ('CAP-0003', 'DC-LON-001', CAST('2026-03-12' AS DATE), 1100, 968, 88.0, 72, 1540.0),
            ('CAP-0004', 'DC-SIN-001', CAST('2026-03-12' AS DATE), 900, 702, 78.0, 44, 1890.0)
        """,
    )

    _seed_table(
        "sla_incidents",
        """
        INSERT INTO sla_incidents VALUES
            ('INC-0001', 'DC-ASH-001', CAST('2026-02-14 02:15:00' AS TIMESTAMP), CAST('2026-02-14 03:05:00' AS TIMESTAMP), 'major', 'UPS A feed and customer cages in hall 3', 'Battery module degradation', 'Shifted load to redundant UPS, replaced failing module, and validated power stability.', 50.0, FALSE),
            ('INC-0002', 'DC-LON-001', CAST('2026-01-28 11:40:00' AS TIMESTAMP), CAST('2026-01-28 13:45:00' AS TIMESTAMP), 'critical', 'Chilled water loop serving suites 4 through 6', 'Valve actuator failure', 'Activated backup loop, replaced the actuator, and rebalanced supply temperatures.', 125.0, TRUE),
            ('INC-0003', 'DC-SIN-001', CAST('2026-03-01 20:10:00' AS TIMESTAMP), CAST('2026-03-01 20:28:00' AS TIMESTAMP), 'minor', 'Remote hands ticketing portal', 'Scheduled network maintenance overlap', 'Rerouted management traffic and updated maintenance window controls.', 18.0, FALSE)
        """,
    )

    print("🎉 Migration v002 completed successfully.")
except Exception as migration_error:
    print(f"❌ Migration v002 failed: {migration_error}")
    raise
