# Lakehouse Schema Migrations

## Why Schema-as-Code?

Microsoft Fabric's Git integration tracks most artifacts (notebooks, pipelines, semantic models) but does NOT version-control Lakehouse table schemas. When you create or modify tables in a Dev Lakehouse, those changes are not captured in Git and cannot be automatically promoted to UAT or Prod.

**Schema-as-Code** solves this by storing table definitions as version-controlled migration scripts.

## How It Works

```
migrations/
├── v001_baseline_tables.sql      # Creates initial tables
├── v001_baseline_tables.py       # PySpark equivalent
├── v002_capacity_and_sustainability.sql   # Adds new tables + columns
└── v002_capacity_and_sustainability.py    # PySpark equivalent
```

### Migration Naming Convention
- Format: `v{NNN}_{description}.{sql|py}`
- Versions run in order: v001 → v002 → v003 → ...
- Each version has both SQL and PySpark variants (use whichever fits your workflow)

### Migration Runner
The `migration_runner.py` notebook:
1. Reads available migration files
2. Checks `_migration_history` table for already-applied migrations
3. Applies only NEW migrations in version order
4. Records results in `_migration_history` for audit

### Key Principles
| Principle | How |
|-----------|-----|
| **Idempotent** | Uses `CREATE TABLE IF NOT EXISTS` and checks _migration_history |
| **Ordered** | Versions are numbered (v001, v002, ...) and run sequentially |
| **Auditable** | Every migration run is logged in _migration_history |
| **Dual-format** | Both SQL and PySpark for flexibility |
| **CI/CD ready** | Parameterized notebook triggered by pipeline |

## Writing a New Migration

1. Create a new file: `v{next_number}_{description}.sql` (and optionally `.py`)
2. Use `CREATE TABLE IF NOT EXISTS` for new tables
3. Use `ALTER TABLE ... ADD COLUMNS` for schema changes
4. Test locally in Dev workspace
5. Commit to Git → CI/CD pipeline auto-deploys to UAT (and Prod after approval)

## Example: Adding a New Table

```sql
-- v003_network_metrics.sql
-- Version: v003
-- Description: Add network bandwidth monitoring table

CREATE TABLE IF NOT EXISTS network_metrics (
    record_id       STRING COMMENT 'Unique record identifier',
    dc_id           STRING COMMENT 'Data center identifier',
    measurement_ts  TIMESTAMP,
    bandwidth_gbps  DOUBLE,
    latency_ms      DOUBLE,
    packet_loss_pct DOUBLE
)
USING DELTA
COMMENT 'Network performance metrics per data center';
```

## CI/CD Integration

Schema migrations are triggered automatically when migration files are pushed to Git:

- **Azure DevOps**: `pipelines/azure-pipelines.yml` watches `lakehouse-migrations/**`
- **GitHub Actions**: `pipelines/.github/workflows/deploy-schema.yml` watches the same path

Both pipelines:
1. Validate migration syntax
2. Run migrations in UAT
3. Wait for manual approval
4. Run migrations in Prod

## References
- [Lakehouse Git Integration](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-git-deployment-pipelines)
- [Fabric CI/CD Best Practices](https://learn.microsoft.com/en-us/fabric/cicd/best-practices-cicd)
- [Delta Lake DDL](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-reference)
