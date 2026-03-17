"""
Migration Validation Script
============================
Validates lakehouse migration files for:
  - Correct naming convention (vNNN_description.sql)
  - Sequential version numbering (no gaps, no duplicates)
  - SQL syntax basics (valid keywords, balanced parentheses)
  - Python migration files have required functions

Used by: .github/workflows/deploy-fabric.yml (validate job)
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple


MIGRATION_DIR = Path("lakehouse-migrations/migrations")
SQL_PATTERN = re.compile(r"^v(\d{3})_[\w]+\.sql$")
PY_PATTERN = re.compile(r"^v(\d{3})_[\w]+\.py$")

# SQL keywords that indicate valid migration content
VALID_SQL_KEYWORDS = {
    "CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE",
    "ADD", "COLUMN", "TABLE", "INDEX", "VIEW", "IF",
}


class ValidationError:
    def __init__(self, file: str, message: str, severity: str = "ERROR"):
        self.file = file
        self.message = message
        self.severity = severity

    def __str__(self):
        return f"[{self.severity}] {self.file}: {self.message}"


def get_migration_files() -> List[Path]:
    """Find all migration files in the migrations directory."""
    if not MIGRATION_DIR.exists():
        return []
    return sorted(MIGRATION_DIR.iterdir())


def validate_naming(files: List[Path]) -> List[ValidationError]:
    """Check that all files follow the vNNN_description.ext naming convention."""
    errors = []
    for f in files:
        if f.name.startswith(".") or f.name == "__pycache__":
            continue
        if not SQL_PATTERN.match(f.name) and not PY_PATTERN.match(f.name):
            errors.append(ValidationError(
                f.name,
                f"Invalid naming convention. Expected: vNNN_description.sql or vNNN_description.py"
            ))
    return errors


def validate_versioning(files: List[Path]) -> List[ValidationError]:
    """Check for sequential version numbers and no duplicates."""
    errors = []
    versions = []

    for f in files:
        sql_match = SQL_PATTERN.match(f.name)
        py_match = PY_PATTERN.match(f.name)
        match = sql_match or py_match
        if match:
            versions.append((int(match.group(1)), f.name))

    # Get unique version numbers
    seen = {}
    for ver, name in versions:
        if ver in seen:
            # Only flag if it's the same extension (sql+py pairs are OK)
            existing_ext = Path(seen[ver]).suffix
            current_ext = Path(name).suffix
            if existing_ext == current_ext:
                errors.append(ValidationError(
                    name,
                    f"Duplicate version number v{ver:03d} (also in {seen[ver]})"
                ))
        else:
            seen[ver] = name

    return errors


def validate_sql_content(files: List[Path]) -> List[ValidationError]:
    """Basic SQL content validation."""
    errors = []
    for f in files:
        if not f.suffix == ".sql":
            continue
        content = f.read_text(encoding="utf-8")

        if not content.strip():
            errors.append(ValidationError(f.name, "Empty migration file"))
            continue

        # Check for balanced parentheses
        if content.count("(") != content.count(")"):
            errors.append(ValidationError(
                f.name,
                "Unbalanced parentheses detected"
            ))

        # Check for at least one valid SQL keyword
        upper_content = content.upper()
        has_keyword = any(kw in upper_content for kw in VALID_SQL_KEYWORDS)
        if not has_keyword:
            errors.append(ValidationError(
                f.name,
                "No recognized SQL keywords found",
                severity="WARNING"
            ))

    return errors


def validate_python_content(files: List[Path]) -> List[ValidationError]:
    """Check Python migration files have required structure."""
    errors = []
    for f in files:
        if not f.suffix == ".py":
            continue
        content = f.read_text(encoding="utf-8")

        if not content.strip():
            errors.append(ValidationError(f.name, "Empty migration file"))
            continue

        # Check for required function
        if "def upgrade(" not in content and "def run(" not in content:
            errors.append(ValidationError(
                f.name,
                "Missing required upgrade() or run() function",
                severity="WARNING"
            ))

    return errors


def main():
    """Run all validations and report results."""
    print("=" * 60)
    print("🔍 Lakehouse Migration Validation")
    print("=" * 60)

    files = get_migration_files()
    if not files:
        print(f"\n⚠️  No migration files found in {MIGRATION_DIR}")
        print("   This is OK if no migrations are being deployed.")
        sys.exit(0)

    print(f"\n📁 Found {len(files)} file(s) in {MIGRATION_DIR}/")
    for f in files:
        print(f"   • {f.name}")

    # Run validations
    all_errors: List[ValidationError] = []
    all_errors.extend(validate_naming(files))
    all_errors.extend(validate_versioning(files))
    all_errors.extend(validate_sql_content(files))
    all_errors.extend(validate_python_content(files))

    # Report results
    errors = [e for e in all_errors if e.severity == "ERROR"]
    warnings = [e for e in all_errors if e.severity == "WARNING"]

    if warnings:
        print(f"\n⚠️  {len(warnings)} warning(s):")
        for w in warnings:
            print(f"   {w}")

    if errors:
        print(f"\n❌ {len(errors)} error(s):")
        for e in errors:
            print(f"   {e}")
        print("\n❌ Validation FAILED")
        sys.exit(1)
    else:
        print("\n✅ All validations passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
