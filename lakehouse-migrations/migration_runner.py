# Parameters - these are set by the CI/CD pipeline or manually
target_environment = "Dev"       # Dev, UAT, or Prod
migration_type = "sql"           # sql or pyspark
migrations_path = "/lakehouse/default/Files/migrations"  # path in Lakehouse Files
dry_run = False                   # If True, just shows what would run

# This notebook is the core schema migration engine for a Fabric Lakehouse.
# It discovers versioned migration files, checks what has already been applied,
# executes only the pending migrations in order, and records every outcome in
# a Delta-backed history table for auditing and repeatability.

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Set


# ------------------------------
# Section 1: Helper structures
# ------------------------------
# A small immutable structure keeps each discovered migration easy to reason
# about as it flows through discovery, filtering, execution, and reporting.
@dataclass(frozen=True)
class Migration:
    version: int
    migration_id: str
    migration_name: str
    path: str
    migration_type: str


# ------------------------------
# Section 2: Utility functions
# ------------------------------
# Fabric notebooks usually expose notebookutils.fs. Some environments still use
# mssparkutils.fs, so we support both to make the notebook portable.
def get_fs_utils():
    try:
        return notebookutils.fs  # type: ignore[name-defined]
    except NameError:
        pass

    try:
        return mssparkutils.fs  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Fabric file utilities are not available. Run this notebook inside Microsoft Fabric."
        ) from exc


# A consistent banner and section formatting makes CI/CD logs much easier to
# scan when the notebook is triggered automatically by a pipeline.
def print_banner() -> None:
    print("=" * 80)
    print("📋 Microsoft Fabric Lakehouse Migration Runner")
    print(f"📋 Target environment : {target_environment}")
    print(f"📋 Migration type     : {migration_type}")
    print(f"📋 Migrations path    : {migrations_path}")
    print(f"📋 Dry run            : {dry_run}")
    print("=" * 80)


# The notebook accepts only two execution modes: SQL files or PySpark files.
# We normalize that to a file extension once so all later logic can reuse it.
def resolve_extension(selected_type: str) -> str:
    normalized_type = selected_type.strip().lower()
    if normalized_type == "sql":
        return ".sql"
    if normalized_type == "pyspark":
        return ".py"

    raise ValueError("migration_type must be either 'sql' or 'pyspark'.")


# The naming convention is v{NNN}_{description}.sql or .py. The migration_id is
# stored without the extension so SQL and PySpark variants of the same logical
# migration are treated as the same migration for idempotency checks.
def parse_migration(file_name: str, file_path: str, selected_type: str) -> Optional[Migration]:
    extension = resolve_extension(selected_type)
    pattern = re.compile(r"^(v(\d+)_([A-Za-z0-9_\-]+))" + re.escape(extension) + r"$")
    match = pattern.match(file_name)
    if not match:
        return None

    migration_id = match.group(1)
    version = int(match.group(2))

    return Migration(
        version=version,
        migration_id=migration_id,
        migration_name=file_name,
        path=file_path,
        migration_type=selected_type.lower(),
    )


# File listing objects differ slightly across Fabric runtimes, so this helper
# centralizes how we determine whether a returned item is a directory.
def is_directory(file_info) -> bool:
    if hasattr(file_info, "isDir"):
        try:
            return bool(file_info.isDir)
        except Exception:
            pass

    name = str(getattr(file_info, "name", ""))
    path = str(getattr(file_info, "path", ""))
    return name.endswith("/") or path.endswith("/")


# Discover migrations of the requested type and sort them by version number so
# v001 always runs before v002, regardless of file system ordering.
def discover_migrations(base_path: str, selected_type: str) -> List[Migration]:
    fs_utils = get_fs_utils()
    extension = resolve_extension(selected_type)

    try:
        file_infos = fs_utils.ls(base_path)
    except Exception as exc:
        raise RuntimeError(f"Unable to list migrations from path: {base_path}") from exc

    migrations: List[Migration] = []
    skipped_files: List[str] = []

    for file_info in file_infos:
        if is_directory(file_info):
            continue

        file_name = str(getattr(file_info, "name", ""))
        file_path = str(getattr(file_info, "path", ""))

        if not file_name.endswith(extension):
            continue

        parsed = parse_migration(file_name=file_name, file_path=file_path, selected_type=selected_type)
        if parsed is None:
            skipped_files.append(file_name)
            continue

        migrations.append(parsed)

    migrations.sort(key=lambda item: (item.version, item.migration_id))

    print(f"📋 Discovered {len(migrations)} {selected_type.lower()} migration file(s).")
    for migration in migrations:
        print(f"   • {migration.migration_name}")

    if skipped_files:
        print("⏳ Skipped files that do not match the version naming convention:")
        for file_name in skipped_files:
            print(f"   • {file_name}")

    return migrations


# Migration scripts are expected to be small text files stored in Lakehouse
# Files. Spark can read them reliably, and we join the lines back into a single
# script body for execution.
def read_text_file(file_path: str) -> str:
    rows = spark.read.text(file_path).collect()
    return "\n".join(row["value"] for row in rows)


# Spark SQL generally executes one statement at a time. This splitter lets a
# single migration file contain multiple semicolon-terminated SQL statements
# while still executing them via spark.sql().
def split_sql_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    in_single_quote = False
    in_double_quote = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if not in_single_quote and not in_double_quote and not in_backtick:
            if char == "-" and next_char == "-":
                current.append(char)
                current.append(next_char)
                in_line_comment = True
                index += 2
                continue

            if char == "/" and next_char == "*":
                current.append(char)
                current.append(next_char)
                in_block_comment = True
                index += 2
                continue

        if char == "'" and not in_double_quote and not in_backtick:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not in_backtick:
            in_double_quote = not in_double_quote
        elif char == "`" and not in_single_quote and not in_double_quote:
            in_backtick = not in_backtick

        if char == ";" and not any([in_single_quote, in_double_quote, in_backtick]):
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    final_statement = "".join(current).strip()
    if final_statement:
        statements.append(final_statement)

    return statements


# Comment-only fragments should not be sent to spark.sql(). This lightweight
# cleaner removes SQL comments so we can check whether a statement still has
# executable content remaining.
def is_executable_sql(statement: str) -> bool:
    without_block_comments = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    without_line_comments = re.sub(r"--.*?$", "", without_block_comments, flags=re.MULTILINE)
    return bool(without_line_comments.strip())


# The history table is the audit trail for every notebook run. We query only
# successful migrations when deciding what is still pending.
def get_applied_migration_ids() -> Set[str]:
    if not spark.catalog.tableExists("_migration_history"):
        return set()

    rows = (
        spark.table("_migration_history")
        .filter("status = 'success'")
        .select("migration_id")
        .distinct()
        .collect()
    )
    return {row["migration_id"] for row in rows}


# Capture the identity of the current user if available. In automated pipeline
# runs this may resolve to a service principal or workspace identity.
def get_current_user() -> str:
    try:
        return spark.sql("SELECT current_user() AS current_user").collect()[0]["current_user"]
    except Exception:
        return os.getenv("USER") or os.getenv("USERNAME") or "unknown"


# Appending history rows through Spark keeps the schema strongly typed and avoids
# building INSERT statements manually.
def log_migration_result(
    migration: Migration,
    status: str,
    applied_by: str,
    applied_at: datetime,
    duration_seconds: float,
) -> None:
    history_rows = [
        (
            migration.migration_id,
            migration.migration_name,
            applied_at,
            applied_by,
            migration.migration_type,
            status,
            float(duration_seconds),
        )
    ]

    history_schema = (
        "migration_id STRING, migration_name STRING, applied_at TIMESTAMP, "
        "applied_by STRING, migration_type STRING, status STRING, duration_seconds DOUBLE"
    )

    spark.createDataFrame(history_rows, schema=history_schema).write.mode("append").insertInto(
        "_migration_history"
    )


# SQL migrations are executed statement by statement. PySpark migrations are
# executed in the current notebook scope so they can use spark and any standard
# helpers already available in the notebook session.
def execute_migration(migration: Migration, script_text: str) -> None:
    if migration.migration_type == "sql":
        statements = [statement for statement in split_sql_statements(script_text) if is_executable_sql(statement)]
        if not statements:
            print(f"⏳ No executable SQL statements found in {migration.migration_name}.")
            return

        for statement in statements:
            spark.sql(statement)
        return

    exec(compile(script_text, migration.path, "exec"), globals(), globals())


# ------------------------------
# Section 3: Notebook execution
# ------------------------------
print_banner()
run_started_at = time.perf_counter()
current_user = get_current_user()
applied_migrations: List[str] = []

print("⏳ Initializing migration history table...")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS _migration_history (
        migration_id STRING,
        migration_name STRING,
        applied_at TIMESTAMP,
        applied_by STRING,
        migration_type STRING,
        status STRING,
        duration_seconds DOUBLE
    )
    """
)
print("✅ Migration history table is ready.")

print("⏳ Discovering available migrations...")
available_migrations = discover_migrations(migrations_path, migration_type)

print("⏳ Checking migration history for completed runs...")
applied_migration_ids = get_applied_migration_ids()
print(f"📋 Already applied logical migrations: {len(applied_migration_ids)}")
for migration_id in sorted(applied_migration_ids):
    print(f"   • {migration_id}")

pending_migrations = [
    migration for migration in available_migrations if migration.migration_id not in applied_migration_ids
]

print(f"📋 Pending {migration_type.lower()} migrations: {len(pending_migrations)}")
for migration in pending_migrations:
    print(f"   • {migration.migration_name}")

if not pending_migrations:
    total_elapsed = time.perf_counter() - run_started_at
    print(f"✅ No pending migrations found for {migration_type.lower()}.")
    print(f"✅ Migration run completed in {total_elapsed:.2f} second(s).")
elif dry_run:
    total_elapsed = time.perf_counter() - run_started_at
    print("⏳ Dry run enabled - no migrations were executed.")
    print(f"📋 {len(pending_migrations)} migration(s) would run in this order:")
    for migration in pending_migrations:
        print(f"   • {migration.migration_name}")
    print(f"✅ Dry run completed in {total_elapsed:.2f} second(s).")
else:
    print("⏳ Applying pending migrations...")

    for migration in pending_migrations:
        print(f"⏳ Starting migration: {migration.migration_name}")
        migration_started_at = time.perf_counter()
        applied_at = datetime.utcnow()

        try:
            script_text = read_text_file(migration.path)
            execute_migration(migration, script_text)

            duration_seconds = time.perf_counter() - migration_started_at
            log_migration_result(
                migration=migration,
                status="success",
                applied_by=current_user,
                applied_at=applied_at,
                duration_seconds=duration_seconds,
            )

            applied_migrations.append(migration.migration_name)
            print(
                f"✅ Migration succeeded: {migration.migration_name} "
                f"({duration_seconds:.2f} second(s))"
            )
        except Exception as exc:
            duration_seconds = time.perf_counter() - migration_started_at

            try:
                log_migration_result(
                    migration=migration,
                    status="failed",
                    applied_by=current_user,
                    applied_at=applied_at,
                    duration_seconds=duration_seconds,
                )
            except Exception as logging_exc:
                print(f"❌ Failed to log migration failure for {migration.migration_name}: {logging_exc}")

            total_elapsed = time.perf_counter() - run_started_at
            print(
                f"❌ Migration failed: {migration.migration_name} "
                f"after {duration_seconds:.2f} second(s)."
            )
            print(f"❌ Error details: {exc}")
            print(
                f"📋 Migration run stopped after applying {len(applied_migrations)} "
                f"migration(s) in {total_elapsed:.2f} second(s)."
            )
            raise

    total_elapsed = time.perf_counter() - run_started_at
    print("=" * 80)
    print("✅ Migration summary")
    print(f"✅ Applied migrations : {len(applied_migrations)}")
    print(f"✅ Total time         : {total_elapsed:.2f} second(s)")
    if applied_migrations:
        print("📋 Applied migration list:")
        for migration_name in applied_migrations:
            print(f"   • {migration_name}")
    else:
        print("📋 No migrations were applied.")
    print("=" * 80)
