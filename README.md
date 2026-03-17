# Fabric CI/CD Demo — Lakehouse Schema Evolution

## Overview

This repository demonstrates how to implement a fully automated CI/CD process for **Microsoft Fabric** artifacts across **Dev → UAT → Prod** workspaces, with a focus on solving the **Lakehouse schema evolution gap**.

### The Problem

Microsoft Fabric's Git integration tracks most artifacts (notebooks, pipelines, semantic models, reports) but does **not** version-control Lakehouse table schemas. When a user creates or modifies a table in a Dev Lakehouse, those changes are not captured in Git and cannot be automatically promoted to UAT or Prod.

### The Solution: Schema-as-Code

We use version-controlled migration scripts (SQL and PySpark) stored in Git. A migration runner notebook applies these scripts to each environment, triggered automatically by CI/CD pipelines.

## Repository Structure

```
Fabric_GitHub/
├── presentation/                    # PowerPoint slide deck for the demo
├── lakehouse-migrations/
│   ├── migrations/                  # Versioned schema migration scripts
│   │   ├── v001_baseline_tables.sql / .py
│   │   └── v002_capacity_and_sustainability.sql / .py
│   ├── migration_runner.py          # Fabric notebook — applies migrations
│   └── migration_state/             # Tracks which migrations have been applied
├── pipelines/
│   ├── azure-pipelines.yml          # Azure DevOps CI/CD pipeline
│   └── .github/workflows/           # GitHub Actions workflow
├── notebooks/                       # Supporting notebooks (data quality checks)
├── scripts/                         # REST API helper and schema extraction scripts
└── docs/                            # Demo runbook
```

## Data Domain

The demo uses a **data center operations** schema designed for infrastructure monitoring:

| Table | Description |
|-------|-------------|
| `data_centers` | Facility information (name, region, capacity) |
| `power_consumption` | Energy usage telemetry |
| `cooling_metrics` | Temperature, humidity, PUE ratio |
| `capacity_utilization` | Rack utilization (added in v2) |
| `sla_incidents` | Uptime incidents (added in v2) |

## Getting Started

1. **Set up Fabric workspaces** — Create `Dev`, `UAT`, and `Prod` workspaces in Microsoft Fabric
2. **Create a Deployment Pipeline** — Link the 3 workspaces in order
3. **Connect Dev to Git** — Use Azure DevOps or GitHub
4. **Push this repo** — Push these files to your connected repo
5. **Run migrations** — Open `migration_runner.py` as a notebook and run it

See `docs/demo_runbook.md` for detailed step-by-step instructions.

## CI/CD Workflow

```
Developer pushes migration script to Git
           │
           ▼
   CI/CD Pipeline triggers
           │
           ▼
  ┌────────────────────┐
  │   Stage 1: UAT     │  Authenticate with Service Principal
  │   Run migrations   │  Trigger migration_runner notebook via REST API
  │   Validate schema  │  Run data quality checks
  └────────┬───────────┘
           │
     Manual Approval Gate
           │
           ▼
  ┌────────────────────┐
  │   Stage 2: Prod    │  Same process as UAT
  │   Run migrations   │
  │   Validate schema  │
  └────────────────────┘
```

## Schema Extraction Utility

Use `scripts/schema_extractor.py` to read the Lakehouse SQL Analytics Endpoint metadata and export version-controlled DDL files plus a `manifest.json` snapshot for drift detection. The script supports interactive, managed identity, and service principal authentication through Microsoft Entra ID.

Use `scripts/drift_detector.py` to compare the Git export with either a live Lakehouse SQL endpoint or a second offline export. It reports added, removed, and modified tables, views, and stored procedures; performs normalized DDL comparison; and highlights column drift for data center operations tables such as `data_centers`, `power_consumption`, `cooling_metrics`, `capacity_utilization`, and `sla_incidents`.

## Key References

- [Lakehouse Git Integration](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-git-deployment-pipelines)
- [Fabric CI/CD Best Practices](https://learn.microsoft.com/en-us/fabric/cicd/best-practices-cicd)
- [Fabric REST APIs](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation)
- [Deployment Pipeline Automation](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/pipeline-automation-fabric)
