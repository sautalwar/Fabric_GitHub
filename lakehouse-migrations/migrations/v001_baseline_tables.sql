-- v001_baseline_tables.sql
-- Baseline Lakehouse tables for the Digital Realty data center operations demo.

-- Table: data_centers
-- Stores facility master data for each Digital Realty data center.
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
COMMENT 'Facility master data for Digital Realty data centers. dc_id is the logical primary key.';

-- Table: power_consumption
-- Stores timestamped energy telemetry for each facility.
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
COMMENT 'Energy usage telemetry for each data center. record_id is the logical key and dc_id logically references data_centers.';

-- Table: cooling_metrics
-- Stores environmental and cooling efficiency measurements for each facility.
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
COMMENT 'Temperature, humidity, and cooling performance telemetry for Digital Realty facilities. record_id is the logical key and dc_id logically references data_centers.';
