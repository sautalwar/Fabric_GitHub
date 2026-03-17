/*
Version: v002
Date: 2026-03-12
Description: Adds capacity utilization and SLA incident tables, and extends power_consumption with sustainability columns.
*/

-- Table: capacity_utilization
-- Stores daily rack and power utilization metrics for each data center.
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
COMMENT 'Daily rack-level capacity and power availability metrics for Digital Realty data centers. record_id is the logical key and dc_id logically references data_centers.';

-- Table: sla_incidents
-- Tracks uptime-impacting incidents and SLA performance by data center.
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
COMMENT 'Incident and uptime tracking for Digital Realty facilities. incident_id is the logical key and dc_id logically references data_centers.';

-- Fabric Delta tables support schema evolution with ALTER TABLE ... ADD COLUMNS.
ALTER TABLE power_consumption
ADD COLUMNS (
    renewable_pct DOUBLE COMMENT 'Percentage of consumed power sourced from renewable energy.',
    carbon_intensity_kg DOUBLE COMMENT 'Carbon intensity in kilograms of CO2 per kilowatt-hour.'
);
