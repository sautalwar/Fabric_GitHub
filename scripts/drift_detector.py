"""Detect schema drift between Git-exported Fabric Lakehouse DDL and a live Lakehouse.

This module is part of the Schema Bridge solution for Microsoft Fabric Lakehouse
CI/CD. It compares the version-controlled schema export in Git with either:

* a live Fabric Lakehouse SQL Analytics Endpoint, or
* another offline schema export directory.

The comparison is tailored to the data center operations demo domain used in this
repository, including tables such as ``data_centers``, ``power_consumption``,
``cooling_metrics``, ``capacity_utilization``, and ``sla_incidents``.

Examples
--------
Compare Git to a live Lakehouse:
    python drift_detector.py --git-schema-dir schema-export --endpoint <sql-endpoint> --database <lakehouse>

Compare two export directories offline:
    python drift_detector.py --git-schema-dir schema-export --live-schema-dir schema-export-live --format markdown

Use as a module:
    from scripts.drift_detector import detect_drift, render_report
    report = detect_drift(git_schema_dir="schema-export", live_schema_dir="schema-export-live")
    print(render_report(report, output_format="text"))
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

try:  # pragma: no cover - import shape depends on how the script is executed.
    from . import schema_extractor
except ImportError:  # pragma: no cover - direct CLI execution path.
    import schema_extractor  # type: ignore[no-redef]


LOGGER = logging.getLogger("drift_detector")
MANAGED_CATEGORIES = ("tables", "views", "procedures")
CATEGORY_LABELS = {"tables": "tables", "views": "views", "procedures": "procedures"}
SINGULAR_CATEGORY = {"tables": "table", "views": "view", "procedures": "procedure"}
STATUS_ORDER = {"modified": 0, "added": 1, "removed": 2, "unchanged": 3}
ANSI_COLORS = {
    "green": "\033[32m",
    "yellow": "\033[33m",
    "red": "\033[31m",
    "cyan": "\033[36m",
    "bold": "\033[1m",
    "reset": "\033[0m",
}


class DriftDetectionError(RuntimeError):
    """Raised when schema drift detection cannot complete successfully."""


@dataclass(frozen=True)
class ColumnSignature:
    """Represents the exported definition for a table column."""

    name: str
    ordinal: int
    type_sql: str
    normalized_type_sql: str
    normalized_definition: str


@dataclass(frozen=True)
class SnapshotObject:
    """Represents a schema object loaded from an export directory."""

    category: str
    qualified_name: str
    relative_path: str
    ddl: str
    normalized_ddl: str
    columns: Dict[str, ColumnSignature] = field(default_factory=dict)


@dataclass
class ColumnDrift:
    """Describes a single column-level difference for a table."""

    column_name: str
    status: str
    left_type: Optional[str] = None
    right_type: Optional[str] = None


@dataclass
class ObjectDrift:
    """Describes drift for one table, view, or stored procedure."""

    object_type: str
    qualified_name: str
    status: str
    details: List[str] = field(default_factory=list)
    column_differences: List[ColumnDrift] = field(default_factory=list)
    left_relative_path: Optional[str] = None
    right_relative_path: Optional[str] = None


@dataclass
class DriftReport:
    """Structured schema drift report for programmatic and CLI use."""

    comparison_timestamp: str
    git_schema_dir: str
    comparison_schema_dir: str
    comparison_source: str
    git_extraction_timestamp: Optional[str]
    comparison_extraction_timestamp: Optional[str]
    ignore_patterns: List[str] = field(default_factory=list)
    ignored_objects: Dict[str, List[str]] = field(
        default_factory=lambda: {category: [] for category in MANAGED_CATEGORIES}
    )
    tables: List[ObjectDrift] = field(default_factory=list)
    views: List[ObjectDrift] = field(default_factory=list)
    procedures: List[ObjectDrift] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(init=False)
    has_drift: bool = field(init=False)

    def __post_init__(self) -> None:
        self.refresh_summary()

    def refresh_summary(self) -> None:
        """Recompute summary counts and the overall drift flag."""
        summary: Dict[str, Any] = {}
        total_drifted = 0

        for category in MANAGED_CATEGORIES:
            objects = getattr(self, category)
            counts = {status: 0 for status in ("added", "removed", "modified", "unchanged")}
            for item in objects:
                counts[item.status] += 1
            counts["drifted"] = counts["added"] + counts["removed"] + counts["modified"]
            total_drifted += counts["drifted"]
            summary[category] = counts

        summary["total_drifted"] = total_drifted
        self.summary = summary
        self.has_drift = total_drifted > 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the report into a JSON-friendly dictionary."""
        return asdict(self)


@dataclass(frozen=True)
class SchemaSnapshot:
    """Loaded export snapshot containing manifest metadata and DDL files."""

    export_dir: str
    manifest: Dict[str, Any]
    objects: Dict[str, Dict[str, SnapshotObject]]
    ignored_objects: Dict[str, List[str]]


def detect_drift(
    git_schema_dir: str | os.PathLike[str],
    *,
    endpoint: Optional[str] = None,
    database: Optional[str] = None,
    live_schema_dir: Optional[str | os.PathLike[str]] = None,
    auth_method: str = "interactive",
    ignore_patterns: Optional[Sequence[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> DriftReport:
    """Detect schema drift between a Git export and a live or offline comparison snapshot.

    Parameters
    ----------
    git_schema_dir:
        Path to the Git-controlled schema export directory containing ``manifest.json``.
    endpoint, database:
        Live Lakehouse SQL endpoint connection details. When provided, the module
        invokes :mod:`schema_extractor` and stores the live export in a temporary
        directory before comparing.
    live_schema_dir:
        Path to a second export directory for offline comparison.
    auth_method:
        Authentication mode forwarded to :func:`schema_extractor.export_schema`.
    ignore_patterns:
        Optional glob-style patterns used to exclude objects from comparison. Patterns
        are matched against ``schema.object``, ``<category>:schema.object``, and the
        relative SQL path such as ``tables/dbo.data_centers.sql``.
    logger:
        Optional logger for extraction and comparison progress.

    Returns
    -------
    DriftReport
        The full structured drift report.
    """
    active_logger = logger or LOGGER
    patterns = normalize_ignore_patterns(ignore_patterns)
    git_dir = Path(git_schema_dir).resolve()

    if bool(endpoint) != bool(database):
        raise DriftDetectionError("--endpoint and --database must be provided together")
    if endpoint and live_schema_dir:
        raise DriftDetectionError("Provide either --endpoint/--database or --live-schema-dir, not both")
    if not endpoint and not live_schema_dir:
        raise DriftDetectionError("Provide --endpoint/--database or --live-schema-dir")

    if endpoint and database:
        with tempfile.TemporaryDirectory(prefix="fabric_live_schema_") as temp_dir:
            temp_path = Path(temp_dir)
            active_logger.info(
                "Extracting live schema from %s / %s into temporary directory %s",
                schema_extractor.normalize_endpoint(endpoint),
                database,
                temp_path,
            )
            schema_extractor.export_schema(
                endpoint=endpoint,
                database=database,
                output_dir=temp_path,
                auth_method=auth_method,
                dry_run=False,
                logger=active_logger,
            )
            comparison_source = f"live endpoint {schema_extractor.normalize_endpoint(endpoint)} / {database}"
            return compare_schema_directories(
                git_schema_dir=git_dir,
                comparison_schema_dir=temp_path,
                comparison_source=comparison_source,
                ignore_patterns=patterns,
            )

    comparison_dir = Path(live_schema_dir).resolve()  # type: ignore[arg-type]
    comparison_source = f"offline export {comparison_dir}"
    return compare_schema_directories(
        git_schema_dir=git_dir,
        comparison_schema_dir=comparison_dir,
        comparison_source=comparison_source,
        ignore_patterns=patterns,
    )


def compare_schema_directories(
    git_schema_dir: str | os.PathLike[str],
    comparison_schema_dir: str | os.PathLike[str],
    *,
    comparison_source: Optional[str] = None,
    ignore_patterns: Optional[Sequence[str]] = None,
) -> DriftReport:
    """Compare two exported schema directories and build a :class:`DriftReport`."""
    patterns = normalize_ignore_patterns(ignore_patterns)
    git_snapshot = load_schema_snapshot(Path(git_schema_dir), ignore_patterns=patterns)
    comparison_snapshot = load_schema_snapshot(Path(comparison_schema_dir), ignore_patterns=patterns)

    report = DriftReport(
        comparison_timestamp=schema_extractor.utc_now_iso(),
        git_schema_dir=git_snapshot.export_dir,
        comparison_schema_dir=comparison_snapshot.export_dir,
        comparison_source=comparison_source or str(comparison_snapshot.export_dir),
        git_extraction_timestamp=git_snapshot.manifest.get("extraction_timestamp"),
        comparison_extraction_timestamp=comparison_snapshot.manifest.get("extraction_timestamp"),
        ignore_patterns=list(patterns),
        ignored_objects=merge_ignored_objects(git_snapshot.ignored_objects, comparison_snapshot.ignored_objects),
        tables=compare_category("tables", git_snapshot, comparison_snapshot),
        views=compare_category("views", git_snapshot, comparison_snapshot),
        procedures=compare_category("procedures", git_snapshot, comparison_snapshot),
    )
    report.recommendations = build_recommendations(report)
    report.refresh_summary()
    return report


def load_schema_snapshot(
    export_dir: Path,
    *,
    ignore_patterns: Optional[Sequence[str]] = None,
) -> SchemaSnapshot:
    """Load manifest metadata and DDL files from a schema export directory."""
    manifest_path = export_dir / "manifest.json"
    if not manifest_path.exists():
        raise DriftDetectionError(f"manifest.json was not found under {export_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    patterns = normalize_ignore_patterns(ignore_patterns)
    objects: Dict[str, Dict[str, SnapshotObject]] = {category: {} for category in MANAGED_CATEGORIES}
    ignored_objects: Dict[str, List[str]] = {category: [] for category in MANAGED_CATEGORIES}

    for category in MANAGED_CATEGORIES:
        manifest_names = sorted({str(name) for name in manifest.get(category, [])})
        for qualified_name in manifest_names:
            relative_path = resolve_relative_path(export_dir, manifest, category, qualified_name)
            if should_ignore(category, qualified_name, relative_path, patterns):
                ignored_objects[category].append(qualified_name)
                continue

            file_path = export_dir / Path(relative_path)
            if not file_path.exists():
                raise DriftDetectionError(
                    f"Expected DDL file for {qualified_name!r} was not found at {file_path}"
                )

            ddl = file_path.read_text(encoding="utf-8")
            columns = parse_table_columns(ddl) if category == "tables" else {}
            objects[category][qualified_name] = SnapshotObject(
                category=category,
                qualified_name=qualified_name,
                relative_path=relative_path,
                ddl=ddl,
                normalized_ddl=normalize_ddl(ddl),
                columns=columns,
            )

    return SchemaSnapshot(
        export_dir=str(export_dir),
        manifest=manifest,
        objects=objects,
        ignored_objects={category: sorted(values) for category, values in ignored_objects.items()},
    )


def compare_category(category: str, git_snapshot: SchemaSnapshot, comparison_snapshot: SchemaSnapshot) -> List[ObjectDrift]:
    """Compare one object category across two snapshots."""
    git_objects = git_snapshot.objects[category]
    comparison_objects = comparison_snapshot.objects[category]
    git_names = set(git_objects)
    comparison_names = set(comparison_objects)

    results: List[ObjectDrift] = []

    for qualified_name in sorted(comparison_names - git_names):
        current = comparison_objects[qualified_name]
        results.append(
            ObjectDrift(
                object_type=SINGULAR_CATEGORY[category],
                qualified_name=qualified_name,
                status="added",
                details=["Present in live schema but missing from Git export."],
                right_relative_path=current.relative_path,
            )
        )

    for qualified_name in sorted(git_names - comparison_names):
        baseline = git_objects[qualified_name]
        results.append(
            ObjectDrift(
                object_type=SINGULAR_CATEGORY[category],
                qualified_name=qualified_name,
                status="removed",
                details=["Present in Git export but missing from live schema."],
                left_relative_path=baseline.relative_path,
            )
        )

    for qualified_name in sorted(git_names & comparison_names):
        baseline = git_objects[qualified_name]
        current = comparison_objects[qualified_name]

        if baseline.normalized_ddl == current.normalized_ddl:
            results.append(
                ObjectDrift(
                    object_type=SINGULAR_CATEGORY[category],
                    qualified_name=qualified_name,
                    status="unchanged",
                    details=["Normalized DDL matches."],
                    left_relative_path=baseline.relative_path,
                    right_relative_path=current.relative_path,
                )
            )
            continue

        column_differences = compare_table_columns(baseline, current) if category == "tables" else []
        details = ["Normalized DDL changed."]
        if category == "tables" and not column_differences:
            details.append(
                "Table definition changed outside column type additions/removals (for example defaults, nullability, or constraints)."
            )

        results.append(
            ObjectDrift(
                object_type=SINGULAR_CATEGORY[category],
                qualified_name=qualified_name,
                status="modified",
                details=details,
                column_differences=column_differences,
                left_relative_path=baseline.relative_path,
                right_relative_path=current.relative_path,
            )
        )

    return sorted(results, key=lambda item: (STATUS_ORDER[item.status], item.qualified_name.lower()))


def compare_table_columns(left: SnapshotObject, right: SnapshotObject) -> List[ColumnDrift]:
    """Compute column-level differences for a table present in both snapshots."""
    left_columns = left.columns
    right_columns = right.columns
    left_names = set(left_columns)
    right_names = set(right_columns)
    differences: List[ColumnDrift] = []

    for column_name in sorted(right_names - left_names, key=str.lower):
        differences.append(
            ColumnDrift(
                column_name=column_name,
                status="added",
                right_type=right_columns[column_name].type_sql,
            )
        )

    for column_name in sorted(left_names - right_names, key=str.lower):
        differences.append(
            ColumnDrift(
                column_name=column_name,
                status="removed",
                left_type=left_columns[column_name].type_sql,
            )
        )

    for column_name in sorted(left_names & right_names, key=str.lower):
        left_column = left_columns[column_name]
        right_column = right_columns[column_name]
        if left_column.normalized_type_sql != right_column.normalized_type_sql:
            differences.append(
                ColumnDrift(
                    column_name=column_name,
                    status="type_changed",
                    left_type=left_column.type_sql,
                    right_type=right_column.type_sql,
                )
            )

    return differences


def render_report(report: DriftReport, output_format: str = "text", use_color: Optional[bool] = None) -> str:
    """Render a drift report as text, JSON, or Markdown."""
    normalized_format = output_format.lower()
    if normalized_format == "json":
        return json.dumps(report.to_dict(), indent=2)
    if normalized_format == "markdown":
        return render_markdown_report(report)
    if normalized_format == "text":
        return render_text_report(report, use_color=use_color)
    raise ValueError(f"Unsupported output format: {output_format}")


def render_text_report(report: DriftReport, *, use_color: Optional[bool] = None) -> str:
    """Render a human-readable drift report with optional ANSI colors."""
    color_enabled = should_use_color(use_color)
    lines = [
        style("Schema Drift Report", "bold", enabled=color_enabled),
        f"Timestamp: {report.comparison_timestamp}",
        f"Git schema dir: {report.git_schema_dir}",
        f"Comparison source: {report.comparison_source}",
        f"Comparison schema dir: {report.comparison_schema_dir}",
        "",
        (
            f"Summary: {report.summary['tables']['drifted']} tables drifted, "
            f"{report.summary['views']['drifted']} views drifted, "
            f"{report.summary['procedures']['drifted']} procedures drifted"
        ),
    ]

    if report.ignore_patterns:
        ignored_total = sum(len(items) for items in report.ignored_objects.values())
        lines.append(f"Ignore patterns: {', '.join(report.ignore_patterns)}")
        lines.append(f"Ignored objects: {ignored_total}")

    lines.append("")

    if not report.has_drift:
        lines.append(style("No schema drift detected.", "green", enabled=color_enabled))
    else:
        for category in MANAGED_CATEGORIES:
            lines.extend(render_text_category(category, getattr(report, category), color_enabled=color_enabled))
            lines.append("")

    lines.append("Recommendations:")
    for recommendation in report.recommendations:
        lines.append(f"- {recommendation}")

    return "\n".join(line for line in lines if line is not None).rstrip() + "\n"


def render_text_category(category: str, objects: Sequence[ObjectDrift], *, color_enabled: bool) -> List[str]:
    """Render one category section for text output."""
    label = CATEGORY_LABELS[category].capitalize()
    lines = [style(label, "cyan", enabled=color_enabled)]
    drifted = [item for item in objects if item.status != "unchanged"]
    unchanged_count = len(objects) - len(drifted)

    if not drifted:
        lines.append(f"  {style('No drift', 'green', enabled=color_enabled)} ({unchanged_count} unchanged)")
        return lines

    for item in drifted:
        symbol, color = status_symbol_and_color(item.status)
        lines.append(f"  {style(symbol, color, enabled=color_enabled)} {item.qualified_name} [{item.status}]")
        for detail in item.details:
            lines.append(f"      - {detail}")
        for column_diff in item.column_differences:
            if column_diff.status == "added":
                lines.append(f"      - Column added: {column_diff.column_name} ({column_diff.right_type})")
            elif column_diff.status == "removed":
                lines.append(f"      - Column removed: {column_diff.column_name} ({column_diff.left_type})")
            else:
                lines.append(
                    f"      - Column type changed: {column_diff.column_name} ({column_diff.left_type} -> {column_diff.right_type})"
                )

    lines.append(f"  Unchanged: {unchanged_count}")
    return lines


def render_markdown_report(report: DriftReport) -> str:
    """Render the report as Markdown suitable for PR comments."""
    lines = [
        "# Schema Drift Report",
        "",
        f"- **Timestamp:** {report.comparison_timestamp}",
        f"- **Git schema dir:** `{report.git_schema_dir}`",
        f"- **Comparison source:** `{report.comparison_source}`",
        f"- **Comparison schema dir:** `{report.comparison_schema_dir}`",
        (
            f"- **Summary:** {report.summary['tables']['drifted']} tables drifted, "
            f"{report.summary['views']['drifted']} views drifted, "
            f"{report.summary['procedures']['drifted']} procedures drifted"
        ),
        "",
    ]

    if report.ignore_patterns:
        lines.append(f"- **Ignore patterns:** `{', '.join(report.ignore_patterns)}`")
        lines.append("")

    drifted_rows: List[str] = []
    for category in MANAGED_CATEGORIES:
        for item in getattr(report, category):
            if item.status == "unchanged":
                continue
            details = list(item.details)
            for column_diff in item.column_differences:
                if column_diff.status == "added":
                    details.append(f"column added `{column_diff.column_name}` ({column_diff.right_type})")
                elif column_diff.status == "removed":
                    details.append(f"column removed `{column_diff.column_name}` ({column_diff.left_type})")
                else:
                    details.append(
                        f"column type changed `{column_diff.column_name}` ({column_diff.left_type} -> {column_diff.right_type})"
                    )
            drifted_rows.append(
                f"| {item.object_type} | `{item.qualified_name}` | {item.status} | {'; '.join(details)} |"
            )

    if drifted_rows:
        lines.extend(
            [
                "| Object type | Object | Status | Details |",
                "| --- | --- | --- | --- |",
                *drifted_rows,
                "",
            ]
        )
    else:
        lines.append("No schema drift detected.")
        lines.append("")

    lines.append("## Recommendations")
    lines.extend(f"- {recommendation}" for recommendation in report.recommendations)
    lines.append("")
    return "\n".join(lines)


def load_manifest(export_dir: str | os.PathLike[str]) -> Dict[str, Any]:
    """Load only ``manifest.json`` from an export directory."""
    manifest_path = Path(export_dir) / "manifest.json"
    if not manifest_path.exists():
        raise DriftDetectionError(f"manifest.json was not found under {export_dir}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def normalize_ignore_patterns(patterns: Optional[Sequence[str]]) -> List[str]:
    """Normalize comma-separated and repeated ignore patterns into a flat list."""
    normalized: List[str] = []
    for value in patterns or []:
        for item in str(value).split(","):
            candidate = item.strip()
            if candidate:
                normalized.append(candidate)
    return normalized


def should_ignore(category: str, qualified_name: str, relative_path: str, patterns: Sequence[str]) -> bool:
    """Return ``True`` when an object matches any ignore pattern."""
    if not patterns:
        return False

    candidates = (
        qualified_name.lower(),
        f"{category}:{qualified_name}".lower(),
        f"{SINGULAR_CATEGORY[category]}:{qualified_name}".lower(),
        relative_path.replace("\\", "/").lower(),
    )
    for pattern in patterns:
        lowered_pattern = pattern.lower()
        if any(fnmatch.fnmatch(candidate, lowered_pattern) for candidate in candidates):
            return True
    return False


def resolve_relative_path(export_dir: Path, manifest: Mapping[str, Any], category: str, qualified_name: str) -> str:
    """Resolve the relative SQL file path for an exported object."""
    manifest_files = manifest.get("files") or {}
    for relative_path, metadata in manifest_files.items():
        if (
            str(metadata.get("qualified_name")) == qualified_name
            and str(metadata.get("object_type")) == SINGULAR_CATEGORY[category]
        ):
            return str(relative_path)

    expected = schema_extractor.build_relative_export_path(category, qualified_name)
    if (export_dir / Path(expected)).exists():
        return expected

    raise DriftDetectionError(
        f"Could not resolve SQL file for {qualified_name!r} in category {category!r} under {export_dir}"
    )


def merge_ignored_objects(
    left: Mapping[str, Sequence[str]],
    right: Mapping[str, Sequence[str]],
) -> Dict[str, List[str]]:
    """Merge ignored object lists from both compared snapshots."""
    return {
        category: sorted(set(left.get(category, [])) | set(right.get(category, [])))
        for category in MANAGED_CATEGORIES
    }


def normalize_ddl(ddl: str) -> str:
    """Strip comments and normalize whitespace for semantically stable comparisons."""
    without_block_comments = re.sub(r"/\*.*?\*/", " ", ddl, flags=re.DOTALL)
    without_line_comments = re.sub(r"(?m)--.*$", " ", without_block_comments)
    normalized_whitespace = re.sub(r"\s+", " ", without_line_comments).strip()
    return normalized_whitespace.upper()


def parse_table_columns(ddl: str) -> Dict[str, ColumnSignature]:
    """Parse column definitions from exported ``CREATE TABLE`` DDL.

    The extractor in this repository generates deterministic SQL in which each
    column or constraint is emitted as a top-level comma-separated item within the
    ``CREATE TABLE (...)`` body. This parser preserves that contract to provide
    column-level drift reporting.
    """
    body = extract_table_body(ddl)
    if body is None:
        raise DriftDetectionError("Unable to parse CREATE TABLE statement for column comparison")

    columns: Dict[str, ColumnSignature] = {}
    for ordinal, item in enumerate(split_sql_list(body), start=1):
        if not item or re.match(r"^\s*CONSTRAINT\b", item, flags=re.IGNORECASE):
            continue

        match = re.match(r"^\s*\[(?P<name>(?:[^\]]|\]\])+)\]\s+(?P<definition>.+?)\s*$", item, flags=re.DOTALL)
        if not match:
            continue

        name = match.group("name").replace("]]", "]")
        definition = match.group("definition").strip().rstrip(",")
        type_sql = extract_column_type(definition)
        columns[name] = ColumnSignature(
            name=name,
            ordinal=ordinal,
            type_sql=type_sql,
            normalized_type_sql=normalize_ddl(type_sql),
            normalized_definition=normalize_ddl(definition),
        )

    return columns


def extract_table_body(ddl: str) -> Optional[str]:
    """Extract the body of a ``CREATE TABLE (...)`` statement."""
    match = re.search(r"CREATE\s+TABLE\b", ddl, flags=re.IGNORECASE)
    if not match:
        return None

    start_index = ddl.find("(", match.end())
    if start_index == -1:
        return None

    depth = 0
    body_start = start_index + 1
    for index in range(start_index, len(ddl)):
        char = ddl[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return ddl[body_start:index]
    return None


def split_sql_list(body: str) -> List[str]:
    """Split a comma-separated SQL definition list while respecting nested syntax."""
    items: List[str] = []
    current: List[str] = []
    paren_depth = 0
    bracket_depth = 0
    in_string = False
    index = 0

    while index < len(body):
        char = body[index]
        next_char = body[index + 1] if index + 1 < len(body) else ""

        if char == "'":
            current.append(char)
            if in_string and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            in_string = not in_string
            index += 1
            continue

        if not in_string:
            if char == "[":
                bracket_depth += 1
            elif char == "]" and bracket_depth > 0:
                bracket_depth -= 1
            elif char == "(":
                paren_depth += 1
            elif char == ")" and paren_depth > 0:
                paren_depth -= 1
            elif char == "," and paren_depth == 0 and bracket_depth == 0:
                items.append("".join(current).strip())
                current = []
                index += 1
                continue

        current.append(char)
        index += 1

    remainder = "".join(current).strip()
    if remainder:
        items.append(remainder)
    return items


def extract_column_type(definition: str) -> str:
    """Extract the SQL type portion from a rendered column definition."""
    without_nullability = re.sub(r"\s+NOT\s+NULL\s*$", "", definition, flags=re.IGNORECASE)
    without_nullability = re.sub(r"\s+NULL\s*$", "", without_nullability, flags=re.IGNORECASE)
    without_default = re.sub(r"\s+DEFAULT\s+.+$", "", without_nullability, flags=re.IGNORECASE | re.DOTALL)
    return without_default.strip()


def build_recommendations(report: DriftReport) -> List[str]:
    """Generate actionable next steps based on detected drift."""
    if not report.has_drift:
        return ["No action required. Git and the compared Lakehouse schema are aligned."]

    recommendations = [
        "If the live Lakehouse changes are intentional, run schema_extractor.py and commit the refreshed schema-export files to Git.",
        "If Git is the source of truth, create a new migration script to reconcile the live Lakehouse with the version-controlled schema.",
    ]

    if report.summary["tables"]["modified"] > 0:
        recommendations.append(
            "Review modified table DDL carefully for column additions, removals, type changes, defaults, and constraints before promoting downstream."
        )

    if report.summary["tables"]["added"] > 0 or report.summary["tables"]["removed"] > 0:
        recommendations.append(
            "Check whether manually created or deleted Lakehouse tables, such as data_centers or power_consumption derivatives, need matching migration coverage."
        )

    return recommendations


def should_use_color(explicit_value: Optional[bool]) -> bool:
    """Determine whether ANSI colors should be emitted."""
    if explicit_value is not None:
        return explicit_value
    return os.getenv("NO_COLOR") is None


def style(text: str, color: str, *, enabled: bool) -> str:
    """Apply a simple ANSI color style to text when enabled."""
    if not enabled:
        return text
    return f"{ANSI_COLORS[color]}{text}{ANSI_COLORS['reset']}"


def status_symbol_and_color(status: str) -> tuple[str, str]:
    """Return the text symbol and color associated with a drift status."""
    if status == "modified":
        return "~", "yellow"
    if status in {"added", "removed"}:
        return "!", "red"
    return "=", "green"


def determine_exit_code(report: DriftReport, *, fail_on_drift: bool = False) -> int:
    """Determine the CLI exit code for a completed comparison.

    Drift always returns exit code 1 to support CI/CD usage. ``fail_on_drift`` is
    accepted for explicit pipeline intent and future compatibility.
    """
    if report.has_drift:
        return 1
    return 0


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Detect schema drift between Git-exported Lakehouse DDL and a live or offline schema export.",
    )
    parser.add_argument(
        "--git-schema-dir",
        required=True,
        help="Path to the baseline Git schema export directory containing manifest.json",
    )
    parser.add_argument("--endpoint", help="Fabric Lakehouse SQL Analytics Endpoint host or URL")
    parser.add_argument("--database", help="Fabric Lakehouse SQL database name")
    parser.add_argument(
        "--live-schema-dir",
        help="Path to a second export directory for offline comparison instead of querying a live endpoint",
    )
    parser.add_argument(
        "--auth-method",
        choices=list(schema_extractor.VALID_AUTH_METHODS),
        default="interactive",
        help="Authentication mode to use when extracting from a live endpoint",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format for the drift report",
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit with code 1 when drift is found (the script already does this by default for CI/CD safety)",
    )
    parser.add_argument(
        "--ignore-patterns",
        nargs="*",
        default=[],
        help="Glob patterns to exclude from comparison, for example staging.* tables:dbo.temp_*",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity for live extraction and comparison",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors for text output",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        report = detect_drift(
            git_schema_dir=args.git_schema_dir,
            endpoint=args.endpoint,
            database=args.database,
            live_schema_dir=args.live_schema_dir,
            auth_method=args.auth_method,
            ignore_patterns=args.ignore_patterns,
            logger=LOGGER,
        )
        rendered = render_report(
            report,
            output_format=args.format,
            use_color=False if args.no_color or args.format != "text" else None,
        )
        sys.stdout.write(rendered)
        return determine_exit_code(report, fail_on_drift=args.fail_on_drift)
    except Exception as exc:
        LOGGER.error("Schema drift detection failed: %s", exc)
        LOGGER.debug("Failure details", exc_info=True)
        return 2


if __name__ == "__main__":  # pragma: no cover - exercised via CLI execution.
    raise SystemExit(main())
