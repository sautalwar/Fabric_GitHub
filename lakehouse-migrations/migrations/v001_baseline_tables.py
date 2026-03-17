"""Migration v001: create baseline Delta tables and seed sample data for the Digital Realty data center operations demo."""

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


try:
    _create_table(
        "data_centers",
        """
        CREATE TABLE IF NOT EXISTS data_centers (
            dc_id STRING COMMENT 'Logical primary key for the data center facility.',
            dc_name STRING COMMENT 'Human-readable name of the data center.',
            region STRING COMMENT 'Operating region for the facility.',
            city STRING COMMENT 'City where the facility is located.',
            country STRING COMMENT 'Country where the facility is located.',
            total_capacity_kw DOUBLE COMMENT 'Total provisioned power capacity of the site in kilowatts.',
            commissioned_date DATE COMMENT 'Date when the data center entered service.',
            tier_level INT COMMENT 'Logical tier classification from 1 through 4.',
            status STRING COMMENT 'Operational status: active, maintenance, or decommissioned.'
        )
        USING DELTA
        COMMENT 'Facility master data for Digital Realty data centers. dc_id is the logical primary key.'
        """,
    )

    _seed_table(
        "data_centers",
        """
        INSERT INTO data_centers VALUES
            ('DC-ASH-001', 'Digital Realty Ashburn Campus', 'North America', 'Ashburn', 'USA', 24000.0, CAST('2015-06-15' AS DATE), 4, 'active'),
            ('DC-DFW-001', 'Digital Realty Dallas Plaza', 'North America', 'Dallas', 'USA', 18500.0, CAST('2017-09-01' AS DATE), 3, 'active'),
            ('DC-LON-001', 'Digital Realty London Metro', 'EMEA', 'London', 'United Kingdom', 22000.0, CAST('2012-11-20' AS DATE), 3, 'active'),
            ('DC-SIN-001', 'Digital Realty Singapore Hub', 'APAC', 'Singapore', 'Singapore', 19800.0, CAST('2019-04-10' AS DATE), 4, 'maintenance')
        """,
    )

    _create_table(
        "power_consumption",
        """
        CREATE TABLE IF NOT EXISTS power_consumption (
            record_id STRING COMMENT 'Logical unique identifier for each telemetry record.',
            dc_id STRING COMMENT 'Logical foreign key reference to data_centers.dc_id.',
            measurement_timestamp TIMESTAMP COMMENT 'Timestamp when the power measurement was captured.',
            total_power_kw DOUBLE COMMENT 'Total facility power draw in kilowatts.',
            it_load_kw DOUBLE COMMENT 'IT equipment load in kilowatts.',
            cooling_load_kw DOUBLE COMMENT 'Cooling system load in kilowatts.',
            lighting_load_kw DOUBLE COMMENT 'Lighting load in kilowatts.',
            pue_ratio DOUBLE COMMENT 'Power Usage Effectiveness (PUE) ratio for the measurement.'
        )
        USING DELTA
        COMMENT 'Energy usage telemetry for each data center. record_id is the logical key and dc_id logically references data_centers.'
        """,
    )

    _seed_table(
        "power_consumption",
        """
        INSERT INTO power_consumption VALUES
            ('PWR-0001', 'DC-ASH-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 18250.0, 12600.0, 4300.0, 350.0, 1.45),
            ('PWR-0002', 'DC-DFW-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 13800.0, 9800.0, 3760.0, 240.0, 1.41),
            ('PWR-0003', 'DC-LON-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 16150.0, 11600.0, 4270.0, 280.0, 1.39),
            ('PWR-0004', 'DC-SIN-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 14980.0, 10800.0, 3920.0, 260.0, 1.39)
        """,
    )

    _create_table(
        "cooling_metrics",
        """
        CREATE TABLE IF NOT EXISTS cooling_metrics (
            record_id STRING COMMENT 'Logical unique identifier for each cooling metrics record.',
            dc_id STRING COMMENT 'Logical foreign key reference to data_centers.dc_id.',
            measurement_timestamp TIMESTAMP COMMENT 'Timestamp when the cooling metrics were captured.',
            supply_temp_celsius DOUBLE COMMENT 'Supply air or liquid temperature in degrees Celsius.',
            return_temp_celsius DOUBLE COMMENT 'Return air or liquid temperature in degrees Celsius.',
            humidity_pct DOUBLE COMMENT 'Relative humidity percentage recorded at the facility.',
            chiller_efficiency DOUBLE COMMENT 'Cooling system or chiller efficiency reading.',
            cooling_method STRING COMMENT 'Cooling method used by the facility: air, liquid, or hybrid.'
        )
        USING DELTA
        COMMENT 'Temperature, humidity, and cooling performance telemetry for Digital Realty facilities. record_id is the logical key and dc_id logically references data_centers.'
        """,
    )

    _seed_table(
        "cooling_metrics",
        """
        INSERT INTO cooling_metrics VALUES
            ('CLG-0001', 'DC-ASH-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 18.5, 27.9, 45.0, 0.91, 'hybrid'),
            ('CLG-0002', 'DC-DFW-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 19.1, 28.7, 47.5, 0.88, 'air'),
            ('CLG-0003', 'DC-LON-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 18.2, 26.8, 43.2, 0.93, 'liquid'),
            ('CLG-0004', 'DC-SIN-001', CAST('2026-03-12 08:00:00' AS TIMESTAMP), 20.0, 29.5, 52.4, 0.86, 'air')
        """,
    )

    print("🎉 Migration v001 completed successfully.")
except Exception as migration_error:
    print(f"❌ Migration v001 failed: {migration_error}")
    raise
