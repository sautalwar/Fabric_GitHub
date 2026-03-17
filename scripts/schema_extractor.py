"""Extract Microsoft Fabric Lakehouse SQL endpoint schema into version-controlled SQL files.

This module connects to a Microsoft Fabric Lakehouse SQL Analytics Endpoint with an
Azure AD / Entra ID access token, reads T-SQL compatible catalog metadata, and
reconstructs DDL files for tables, views, and stored procedures.

The extractor is intentionally opinionated for Git workflows:
- tables are written to ``tables/<schema>.<object>.sql``
- views are written to ``views/<schema>.<object>.sql``
- procedures are written to ``procedures/<schema>.<object>.sql``
- ``manifest.json`` captures counts, object names, and a drift-detection hash

The script works well for the data center operations demo domain used in this
repository, including objects such as ``data_centers``, ``power_consumption``,
``cooling_metrics``, ``capacity_utilization``, and ``sla_incidents``.

Examples
--------
Run an extraction:
    python schema_extractor.py --endpoint <sql-endpoint> --database <lakehouse-name> --output-dir schema-export

Compare two exported manifests from Python:
    from schema_extractor import compare_schemas
    report = compare_schemas("schema-export-dev", "schema-export-prod")
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import struct
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

try:
    import pyodbc
except ImportError:  # pragma: no cover - handled at runtime when extraction is invoked.
    pyodbc = None  # type: ignore[assignment]

try:
    from azure.identity import AzureCliCredential, ClientSecretCredential, InteractiveBrowserCredential, ManagedIdentityCredential
except ImportError:  # pragma: no cover - handled at runtime when extraction is invoked.
    AzureCliCredential = None  # type: ignore[assignment]
    ClientSecretCredential = None  # type: ignore[assignment]
    InteractiveBrowserCredential = None  # type: ignore[assignment]
    ManagedIdentityCredential = None  # type: ignore[assignment]


LOGGER = logging.getLogger("schema_extractor")
SQL_ACCESS_TOKEN_SCOPE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256
PREFERRED_ODBC_DRIVERS = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
)
MANAGED_EXPORT_FOLDERS = ("tables", "views", "procedures")
SQL_SYSTEM_SCHEMAS = {"INFORMATION_SCHEMA", "sys"}
VALID_AUTH_METHODS = ("service-principal", "interactive", "managed-identity", "azure-cli")
WINDOWS_FORBIDDEN_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*]')


class SchemaExtractionError(RuntimeError):
    """Raised when schema metadata cannot be extracted or reconstructed safely."""


@dataclass(frozen=True)
class ColumnMetadata:
    """Represents a single column from ``INFORMATION_SCHEMA.COLUMNS``."""

    schema_name: str
    table_name: str
    column_name: str
    ordinal_position: int
    data_type: str
    is_nullable: bool
    column_default: Optional[str]
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    datetime_precision: Optional[int]


@dataclass(frozen=True)
class ConstraintMetadata:
    """Represents PRIMARY KEY or UNIQUE metadata for a table."""

    schema_name: str
    table_name: str
    constraint_name: str
    constraint_type: str
    columns: Tuple[str, ...]


@dataclass
class TableMetadata:
    """Aggregates table definition pieces needed to reconstruct CREATE TABLE."""

    schema_name: str
    object_name: str
    object_type: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    constraints: List[ConstraintMetadata] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        """Return a stable ``schema.object`` identifier for manifests and filenames."""
        return f"{self.schema_name}.{self.object_name}"


@dataclass(frozen=True)
class ModuleMetadata:
    """Represents a stored module definition from ``sys.sql_modules``."""

    schema_name: str
    object_name: str
    object_type: str
    definition: str

    @property
    def qualified_name(self) -> str:
        """Return a stable ``schema.object`` identifier for manifests and filenames."""
        return f"{self.schema_name}.{self.object_name}"


@dataclass(frozen=True)
class ExportArtifact:
    """In-memory representation of a generated SQL file."""

    object_type: str
    qualified_name: str
    relative_path: str
    content: str

    @property
    def sha256(self) -> str:
        """Return the SHA256 hash of this artifact's SQL content."""
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()


class FabricSchemaExtractor:
    """Extract tables, views, and procedures from a Fabric SQL Analytics Endpoint."""

    def __init__(
        self,
        endpoint: str,
        database: str,
        output_dir: str | os.PathLike[str],
        auth_method: str = "interactive",
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.endpoint = endpoint.strip()
        self.database = database.strip()
        self.output_dir = Path(output_dir)
        self.auth_method = auth_method
        self.dry_run = dry_run
        self.logger = logger or LOGGER

        if not self.endpoint:
            raise ValueError("endpoint must not be empty")
        if not self.database:
            raise ValueError("database must not be empty")
        if self.auth_method not in VALID_AUTH_METHODS:
            raise ValueError(
                f"auth_method must be one of {', '.join(VALID_AUTH_METHODS)}; received {self.auth_method!r}"
            )

    def extract(self) -> Dict[str, Any]:
        """Run a full schema extraction and optionally persist SQL files to disk."""
        extraction_timestamp = utc_now_iso()
        server = normalize_endpoint(self.endpoint)
        self.logger.info(
            "Starting schema extraction from %s / %s using %s authentication",
            server,
            self.database,
            self.auth_method,
        )

        with self._connect() as connection:
            schemata = self._fetch_schemata(connection)
            table_rows = self._fetch_tables(connection)
            columns = self._fetch_columns(connection)
            constraints = self._fetch_constraints(connection)
            modules = self._fetch_modules(connection)

        tables, views, procedures = self._build_metadata(table_rows, columns, constraints, modules)
        artifacts = self._build_artifacts(
            extraction_timestamp=extraction_timestamp,
            tables=tables,
            views=views,
            procedures=procedures,
        )
        ddl_hash = compute_artifact_bundle_hash(artifacts)
        manifest = self._build_manifest(
            extraction_timestamp=extraction_timestamp,
            schemata=schemata,
            tables=tables,
            views=views,
            procedures=procedures,
            artifacts=artifacts,
            ddl_hash=ddl_hash,
        )

        if self.dry_run:
            self._log_dry_run_summary(tables=tables, views=views, procedures=procedures)
            return manifest

        self._write_export(artifacts=artifacts, manifest=manifest)
        self.logger.info(
            "Schema export complete: %s table(s), %s view(s), %s procedure(s)",
            len(tables),
            len(views),
            len(procedures),
        )
        return manifest

    def _connect(self):
        """Create a token-authenticated ODBC connection to the Fabric SQL endpoint."""
        if pyodbc is None:
            raise SchemaExtractionError(
                "pyodbc is required for extraction. Install dependencies with: pip install pyodbc azure-identity"
            )
        if any(dependency is None for dependency in (ClientSecretCredential, InteractiveBrowserCredential, ManagedIdentityCredential)):
            raise SchemaExtractionError(
                "azure-identity is required for token acquisition. Install dependencies with: pip install azure-identity"
            )

        driver = resolve_odbc_driver()
        server = normalize_endpoint(self.endpoint)
        credential = build_credential(self.auth_method)
        token = credential.get_token(SQL_ACCESS_TOKEN_SCOPE).token
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

        connection_string = (
            f"Driver={{{driver}}};"
            f"Server=tcp:{server};"
            f"Database={self.database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        self.logger.debug("Connecting with ODBC driver %s", driver)
        try:
            return pyodbc.connect(
                connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                autocommit=True,
            )
        except Exception as exc:  # pragma: no cover - depends on external infrastructure.
            raise SchemaExtractionError(
                f"Failed to connect to Fabric SQL endpoint {server!r} for database {self.database!r}: {exc}"
            ) from exc

    def _fetch_schemata(self, connection) -> List[str]:
        """Fetch non-system schema names from ``INFORMATION_SCHEMA.SCHEMATA``."""
        query = """
        SELECT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'sys')
        ORDER BY SCHEMA_NAME;
        """
        rows = connection.cursor().execute(query).fetchall()
        schemata = [str(row[0]) for row in rows]
        self.logger.info("Discovered %s schema(s)", len(schemata))
        return schemata

    def _fetch_tables(self, connection) -> List[Tuple[str, str, str]]:
        """Fetch table and view names from ``INFORMATION_SCHEMA.TABLES``."""
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
        ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME;
        """
        rows = connection.cursor().execute(query).fetchall()
        return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]

    def _fetch_columns(self, connection) -> List[ColumnMetadata]:
        """Fetch ordered column definitions from ``INFORMATION_SCHEMA.COLUMNS``."""
        query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            DATETIME_PRECISION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        """
        rows = connection.cursor().execute(query).fetchall()
        return [
            ColumnMetadata(
                schema_name=str(row[0]),
                table_name=str(row[1]),
                column_name=str(row[2]),
                ordinal_position=int(row[3]),
                data_type=str(row[4]),
                is_nullable=str(row[5]).upper() == "YES",
                column_default=None if row[6] is None else str(row[6]),
                character_maximum_length=None if row[7] is None else int(row[7]),
                numeric_precision=None if row[8] is None else int(row[8]),
                numeric_scale=None if row[9] is None else int(row[9]),
                datetime_precision=None if row[10] is None else int(row[10]),
            )
            for row in rows
        ]

    def _fetch_constraints(self, connection) -> List[ConstraintMetadata]:
        """Fetch primary key and unique key metadata for table reconstruction."""
        query = """
        SELECT
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE,
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG
            AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;
        """
        rows = connection.cursor().execute(query).fetchall()
        grouped: Dict[Tuple[str, str, str, str], List[Tuple[int, str]]] = {}
        for row in rows:
            key = (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            grouped.setdefault(key, []).append((int(row[5]), str(row[4])))

        constraints: List[ConstraintMetadata] = []
        for (schema_name, table_name, constraint_name, constraint_type), column_rows in grouped.items():
            ordered_columns = tuple(column_name for _, column_name in sorted(column_rows, key=lambda item: item[0]))
            constraints.append(
                ConstraintMetadata(
                    schema_name=schema_name,
                    table_name=table_name,
                    constraint_name=constraint_name,
                    constraint_type=constraint_type,
                    columns=ordered_columns,
                )
            )
        return constraints

    def _fetch_modules(self, connection) -> List[ModuleMetadata]:
        """Fetch view and stored procedure definitions from ``sys.sql_modules``."""
        query = """
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            o.type AS object_type,
            m.definition
        FROM sys.sql_modules AS m
        INNER JOIN sys.objects AS o
            ON m.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE o.is_ms_shipped = 0
          AND s.name NOT IN ('INFORMATION_SCHEMA', 'sys')
          AND o.type IN ('V', 'P', 'PC')
        ORDER BY s.name, o.type, o.name;
        """
        rows = connection.cursor().execute(query).fetchall()
        modules: List[ModuleMetadata] = []
        for row in rows:
            definition = "" if row[3] is None else str(row[3])
            modules.append(
                ModuleMetadata(
                    schema_name=str(row[0]),
                    object_name=str(row[1]),
                    object_type=str(row[2]),
                    definition=definition,
                )
            )
        return modules

    def _build_metadata(
        self,
        table_rows: Sequence[Tuple[str, str, str]],
        columns: Sequence[ColumnMetadata],
        constraints: Sequence[ConstraintMetadata],
        modules: Sequence[ModuleMetadata],
    ) -> Tuple[List[TableMetadata], List[ModuleMetadata], List[ModuleMetadata]]:
        """Assemble table, view, and procedure objects from raw catalog result sets."""
        table_map: Dict[Tuple[str, str], TableMetadata] = {}
        view_names_from_information_schema: set[Tuple[str, str]] = set()

        for schema_name, object_name, table_type in table_rows:
            normalized_type = table_type.upper()
            key = (schema_name, object_name)
            if normalized_type == "BASE TABLE":
                table_map[key] = TableMetadata(
                    schema_name=schema_name,
                    object_name=object_name,
                    object_type=normalized_type,
                )
            elif normalized_type == "VIEW":
                view_names_from_information_schema.add(key)

        for column in columns:
            key = (column.schema_name, column.table_name)
            if key in table_map:
                table_map[key].columns.append(column)

        for constraint in constraints:
            key = (constraint.schema_name, constraint.table_name)
            if key in table_map:
                table_map[key].constraints.append(constraint)

        tables = sorted(table_map.values(), key=lambda item: item.qualified_name)
        modules_by_key = {(module.schema_name, module.object_name, module.object_type): module for module in modules}
        views = sorted(
            [module for module in modules if module.object_type == "V"],
            key=lambda item: item.qualified_name,
        )
        procedures = sorted(
            [module for module in modules if module.object_type in {"P", "PC"}],
            key=lambda item: item.qualified_name,
        )

        missing_view_definitions = [
            f"{schema_name}.{object_name}"
            for schema_name, object_name in sorted(view_names_from_information_schema)
            if (schema_name, object_name, "V") not in modules_by_key
        ]
        if missing_view_definitions:
            raise SchemaExtractionError(
                "Missing view definitions from sys.sql_modules for: " + ", ".join(missing_view_definitions)
            )

        tables_without_columns = [table.qualified_name for table in tables if not table.columns]
        if tables_without_columns:
            raise SchemaExtractionError(
                "The following tables were found without column metadata: " + ", ".join(tables_without_columns)
            )

        self.logger.info(
            "Prepared metadata for %s table(s), %s view(s), and %s procedure(s)",
            len(tables),
            len(views),
            len(procedures),
        )
        return tables, views, procedures

    def _build_artifacts(
        self,
        extraction_timestamp: str,
        tables: Sequence[TableMetadata],
        views: Sequence[ModuleMetadata],
        procedures: Sequence[ModuleMetadata],
    ) -> List[ExportArtifact]:
        """Render metadata into SQL file artifacts for later writing and hashing."""
        artifacts: List[ExportArtifact] = []

        for table in tables:
            relative_path = build_relative_export_path("tables", table.qualified_name)
            artifacts.append(
                ExportArtifact(
                    object_type="table",
                    qualified_name=table.qualified_name,
                    relative_path=relative_path,
                    content=render_table_ddl(
                        table=table,
                        endpoint=self.endpoint,
                        database=self.database,
                        extraction_timestamp=extraction_timestamp,
                    ),
                )
            )

        for view in views:
            relative_path = build_relative_export_path("views", view.qualified_name)
            artifacts.append(
                ExportArtifact(
                    object_type="view",
                    qualified_name=view.qualified_name,
                    relative_path=relative_path,
                    content=render_module_ddl(
                        module=view,
                        endpoint=self.endpoint,
                        database=self.database,
                        extraction_timestamp=extraction_timestamp,
                    ),
                )
            )

        for procedure in procedures:
            relative_path = build_relative_export_path("procedures", procedure.qualified_name)
            artifacts.append(
                ExportArtifact(
                    object_type="procedure",
                    qualified_name=procedure.qualified_name,
                    relative_path=relative_path,
                    content=render_module_ddl(
                        module=procedure,
                        endpoint=self.endpoint,
                        database=self.database,
                        extraction_timestamp=extraction_timestamp,
                    ),
                )
            )

        return sorted(artifacts, key=lambda artifact: artifact.relative_path)

    def _build_manifest(
        self,
        extraction_timestamp: str,
        schemata: Sequence[str],
        tables: Sequence[TableMetadata],
        views: Sequence[ModuleMetadata],
        procedures: Sequence[ModuleMetadata],
        artifacts: Sequence[ExportArtifact],
        ddl_hash: str,
    ) -> Dict[str, Any]:
        """Build manifest metadata for Git drift detection and schema comparisons."""
        files = {
            artifact.relative_path: {
                "object_type": artifact.object_type,
                "qualified_name": artifact.qualified_name,
                "sha256": artifact.sha256,
            }
            for artifact in artifacts
        }
        return {
            "extraction_timestamp": extraction_timestamp,
            "endpoint": normalize_endpoint(self.endpoint),
            "database": self.database,
            "auth_method": self.auth_method,
            "schemas": list(schemata),
            "table_count": len(tables),
            "view_count": len(views),
            "procedure_count": len(procedures),
            "tables": [table.qualified_name for table in tables],
            "views": [view.qualified_name for view in views],
            "procedures": [procedure.qualified_name for procedure in procedures],
            "files": files,
            "ddl_sha256": ddl_hash,
            "hash": ddl_hash,
        }

    def _write_export(self, artifacts: Sequence[ExportArtifact], manifest: Mapping[str, Any]) -> None:
        """Write the generated SQL files and manifest into the target output directory."""
        prepare_output_directory(self.output_dir, logger=self.logger)
        for artifact in artifacts:
            target_path = self.output_dir / Path(artifact.relative_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(artifact.content, encoding="utf-8", newline="\n")
            self.logger.debug("Wrote %s", target_path)

        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8", newline="\n")
        self.logger.info("Wrote manifest to %s", manifest_path)

    def _log_dry_run_summary(
        self,
        tables: Sequence[TableMetadata],
        views: Sequence[ModuleMetadata],
        procedures: Sequence[ModuleMetadata],
    ) -> None:
        """Emit a dry-run summary without creating files on disk."""
        self.logger.info("Dry run enabled - no files were written.")
        if tables:
            self.logger.info("Tables that would be exported: %s", ", ".join(table.qualified_name for table in tables))
        if views:
            self.logger.info("Views that would be exported: %s", ", ".join(view.qualified_name for view in views))
        if procedures:
            self.logger.info(
                "Procedures that would be exported: %s",
                ", ".join(procedure.qualified_name for procedure in procedures),
            )


def utc_now_iso() -> str:
    """Return a UTC ISO-8601 timestamp suitable for comments and manifest metadata."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()



def normalize_endpoint(endpoint: str) -> str:
    """Normalize a Fabric SQL endpoint or URL into an ODBC-compatible ``server[,port]`` string."""
    raw = endpoint.strip()
    if not raw:
        raise ValueError("endpoint must not be empty")

    if "://" in raw:
        parsed = urlparse(raw)
        host = parsed.hostname or ""
        port = parsed.port
    else:
        normalized = raw
        if normalized.lower().startswith("tcp:"):
            normalized = normalized[4:]
        normalized = normalized.split("/", 1)[0]
        if "," in normalized:
            host, port_text = normalized.split(",", 1)
            port = int(port_text)
        elif normalized.count(":") == 1:
            host, port_text = normalized.rsplit(":", 1)
            port = int(port_text)
        else:
            host = normalized
            port = None

    if not host:
        raise ValueError(f"Unable to determine SQL server host from endpoint {endpoint!r}")
    return f"{host},{port or 1433}"



def resolve_odbc_driver() -> str:
    """Pick the best installed SQL Server ODBC driver, preferring Driver 18."""
    if pyodbc is None:
        raise SchemaExtractionError("pyodbc is not installed.")

    available = set(pyodbc.drivers())
    override = os.getenv("ODBC_DRIVER")
    if override:
        if override in available:
            return override
        raise SchemaExtractionError(
            f"ODBC_DRIVER is set to {override!r}, but that driver is not installed. Available drivers: {sorted(available)}"
        )

    for driver in PREFERRED_ODBC_DRIVERS:
        if driver in available:
            return driver

    raise SchemaExtractionError(
        "No supported SQL Server ODBC driver was found. Install ODBC Driver 18 or 17 for SQL Server. "
        f"Available drivers: {sorted(available)}"
    )



def build_credential(auth_method: str):
    """Create an ``azure-identity`` credential matching the selected auth method."""
    if auth_method == "interactive":
        if InteractiveBrowserCredential is None:
            raise SchemaExtractionError("InteractiveBrowserCredential is unavailable because azure-identity is not installed.")
        return InteractiveBrowserCredential()

    if auth_method == "managed-identity":
        if ManagedIdentityCredential is None:
            raise SchemaExtractionError("ManagedIdentityCredential is unavailable because azure-identity is not installed.")
        client_id = first_non_empty(
            os.getenv("AZURE_CLIENT_ID"),
            os.getenv("FABRIC_MANAGED_IDENTITY_CLIENT_ID"),
        )
        return ManagedIdentityCredential(client_id=client_id)

    if auth_method == "service-principal":
        if ClientSecretCredential is None:
            raise SchemaExtractionError("ClientSecretCredential is unavailable because azure-identity is not installed.")
        tenant_id = first_non_empty(os.getenv("AZURE_TENANT_ID"), os.getenv("FABRIC_TENANT_ID"))
        client_id = first_non_empty(os.getenv("AZURE_CLIENT_ID"), os.getenv("FABRIC_CLIENT_ID"))
        client_secret = first_non_empty(os.getenv("AZURE_CLIENT_SECRET"), os.getenv("FABRIC_CLIENT_SECRET"))
        missing = [
            name
            for name, value in {
                "AZURE_TENANT_ID/FABRIC_TENANT_ID": tenant_id,
                "AZURE_CLIENT_ID/FABRIC_CLIENT_ID": client_id,
                "AZURE_CLIENT_SECRET/FABRIC_CLIENT_SECRET": client_secret,
            }.items()
            if not value
        ]
        if missing:
            raise SchemaExtractionError(
                "Service principal authentication requires environment variables: " + ", ".join(missing)
            )
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    if auth_method == "azure-cli":
        if AzureCliCredential is None:
            raise SchemaExtractionError("AzureCliCredential is unavailable because azure-identity is not installed.")
        return AzureCliCredential()

    raise ValueError(f"Unsupported auth method: {auth_method}")



def first_non_empty(*values: Optional[str]) -> Optional[str]:
    """Return the first non-empty string from a list of optional environment values."""
    for value in values:
        if value and value.strip():
            return value.strip()
    return None



def quote_identifier(identifier: str) -> str:
    """Escape a SQL Server identifier using square brackets."""
    return f"[{identifier.replace(']', ']]')}]"



def render_table_ddl(
    table: TableMetadata,
    endpoint: str,
    database: str,
    extraction_timestamp: str,
) -> str:
    """Render a CREATE TABLE statement using catalog metadata.

    The Lakehouse SQL endpoint does not expose a built-in "show create table" command,
    so this function reconstructs the statement from ``INFORMATION_SCHEMA`` metadata.
    """
    ordered_columns = sorted(table.columns, key=lambda column: column.ordinal_position)
    ordered_constraints = sorted(
        table.constraints,
        key=lambda item: (0 if item.constraint_type == "PRIMARY KEY" else 1, item.constraint_name),
    )

    lines: List[str] = []
    for column in ordered_columns:
        data_type_sql = format_data_type(column)
        default_sql = f" DEFAULT {column.column_default.strip()}" if column.column_default else ""
        nullability_sql = "NULL" if column.is_nullable else "NOT NULL"
        lines.append(
            f"    {quote_identifier(column.column_name)} {data_type_sql}{default_sql} {nullability_sql}".rstrip()
        )

    for constraint in ordered_constraints:
        constraint_columns = ", ".join(quote_identifier(column_name) for column_name in constraint.columns)
        lines.append(
            f"    CONSTRAINT {quote_identifier(constraint.constraint_name)} {constraint.constraint_type} ({constraint_columns})"
        )

    body = ",\n".join(lines)
    return (
        f"-- Extracted on {extraction_timestamp}\n"
        f"-- Source endpoint: {normalize_endpoint(endpoint)}\n"
        f"-- Source database: {database}\n"
        f"CREATE TABLE {quote_identifier(table.schema_name)}.{quote_identifier(table.object_name)} (\n"
        f"{body}\n"
        f");\n"
    )



def format_data_type(column: ColumnMetadata) -> str:
    """Render the column data type with length, precision, or scale when applicable."""
    data_type = column.data_type.upper()

    if data_type in {"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "BINARY", "VARBINARY"}:
        length = column.character_maximum_length
        if length is None:
            return data_type
        return f"{data_type}(MAX)" if length == -1 else f"{data_type}({length})"

    if data_type in {"DECIMAL", "NUMERIC"}:
        if column.numeric_precision is None:
            return data_type
        scale = column.numeric_scale or 0
        return f"{data_type}({column.numeric_precision},{scale})"

    if data_type in {"TIME", "DATETIME2", "DATETIMEOFFSET"} and column.datetime_precision is not None:
        return f"{data_type}({column.datetime_precision})"

    if data_type == "FLOAT" and column.numeric_precision is not None:
        return f"{data_type}({column.numeric_precision})"

    return data_type



def render_module_ddl(
    module: ModuleMetadata,
    endpoint: str,
    database: str,
    extraction_timestamp: str,
) -> str:
    """Render a CREATE VIEW or CREATE PROCEDURE script from ``sys.sql_modules`` text."""
    definition = normalize_module_definition(module)
    return (
        f"-- Extracted on {extraction_timestamp}\n"
        f"-- Source endpoint: {normalize_endpoint(endpoint)}\n"
        f"-- Source database: {database}\n"
        f"{definition.rstrip()}\n"
    )



def normalize_module_definition(module: ModuleMetadata) -> str:
    """Normalize module text so exported files remain executable and Git-friendly."""
    definition = module.definition.strip()
    if not definition:
        raise SchemaExtractionError(f"Definition for {module.qualified_name} is empty and cannot be exported.")

    # Many SQL endpoints store CREATE text directly. When they return ALTER text,
    # convert the leading keyword so version-controlled files remain re-runnable.
    if re.match(r"^ALTER\s+VIEW\b", definition, flags=re.IGNORECASE):
        return re.sub(r"^ALTER", "CREATE OR ALTER", definition, count=1, flags=re.IGNORECASE)
    if re.match(r"^ALTER\s+PROC(?:EDURE)?\b", definition, flags=re.IGNORECASE):
        return re.sub(r"^ALTER", "CREATE OR ALTER", definition, count=1, flags=re.IGNORECASE)
    if re.match(r"^CREATE\s+", definition, flags=re.IGNORECASE):
        return definition

    qualified_name = f"{quote_identifier(module.schema_name)}.{quote_identifier(module.object_name)}"
    if module.object_type == "V":
        return f"CREATE VIEW {qualified_name} AS\n{definition}"
    if module.object_type in {"P", "PC"}:
        return f"CREATE PROCEDURE {qualified_name}\nAS\n{definition}"
    return definition



def sanitize_filename(name: str) -> str:
    """Make a stable filename while preserving ``schema.object`` naming."""
    sanitized = WINDOWS_FORBIDDEN_FILENAME_CHARS.sub("_", name)
    return sanitized.rstrip(". ")



def build_relative_export_path(folder: str, qualified_name: str) -> str:
    """Return the relative SQL path for an exported object."""
    file_name = f"{sanitize_filename(qualified_name)}.sql"
    return Path(folder, file_name).as_posix()



def prepare_output_directory(output_dir: Path, logger: Optional[logging.Logger] = None) -> None:
    """Create export folders and remove prior generated SQL files.

    The exporter owns ``tables/``, ``views/``, ``procedures/``, and ``manifest.json``
    beneath the chosen output directory. Clearing old SQL files ensures removed source
    objects appear as deleted files in Git after a fresh extraction.
    """
    active_logger = logger or LOGGER
    output_dir.mkdir(parents=True, exist_ok=True)

    for folder_name in MANAGED_EXPORT_FOLDERS:
        folder_path = output_dir / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        for sql_file in folder_path.glob("*.sql"):
            sql_file.unlink()
            active_logger.debug("Removed stale export file %s", sql_file)

    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        manifest_path.unlink()
        active_logger.debug("Removed prior manifest %s", manifest_path)



def compute_artifact_bundle_hash(artifacts: Sequence[ExportArtifact]) -> str:
    """Hash all DDL file contents concatenated in deterministic relative-path order."""
    digest = hashlib.sha256()
    for artifact in sorted(artifacts, key=lambda item: item.relative_path):
        digest.update(artifact.content.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()



def export_schema(
    endpoint: str,
    database: str,
    output_dir: str | os.PathLike[str],
    auth_method: str = "interactive",
    dry_run: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Convenience wrapper for programmatic schema extraction."""
    extractor = FabricSchemaExtractor(
        endpoint=endpoint,
        database=database,
        output_dir=output_dir,
        auth_method=auth_method,
        dry_run=dry_run,
        logger=logger,
    )
    return extractor.extract()



def compare_schemas(left_dir: str | os.PathLike[str], right_dir: str | os.PathLike[str]) -> Dict[str, Any]:
    """Compare two export directories and report added, removed, and modified objects.

    Parameters
    ----------
    left_dir:
        Path to the baseline export directory containing ``manifest.json``.
    right_dir:
        Path to the comparison export directory containing ``manifest.json``.

    Returns
    -------
    dict
        A structured report grouping added, removed, and modified tables, views,
        and stored procedures by qualified object name.
    """
    left_snapshot = load_export_snapshot(Path(left_dir))
    right_snapshot = load_export_snapshot(Path(right_dir))

    left_objects = left_snapshot["objects"]
    right_objects = right_snapshot["objects"]
    left_keys = set(left_objects)
    right_keys = set(right_objects)

    added_keys = sorted(right_keys - left_keys)
    removed_keys = sorted(left_keys - right_keys)
    modified_keys = sorted(
        key for key in (left_keys & right_keys) if left_objects[key]["sha256"] != right_objects[key]["sha256"]
    )

    return {
        "left": {
            "path": str(Path(left_dir)),
            "extraction_timestamp": left_snapshot["manifest"].get("extraction_timestamp"),
            "ddl_sha256": left_snapshot["manifest"].get("ddl_sha256") or left_snapshot["manifest"].get("hash"),
        },
        "right": {
            "path": str(Path(right_dir)),
            "extraction_timestamp": right_snapshot["manifest"].get("extraction_timestamp"),
            "ddl_sha256": right_snapshot["manifest"].get("ddl_sha256") or right_snapshot["manifest"].get("hash"),
        },
        "added": group_snapshot_keys(added_keys, right_objects),
        "removed": group_snapshot_keys(removed_keys, left_objects),
        "modified": group_snapshot_keys(modified_keys, right_objects),
    }



def load_export_snapshot(export_dir: Path) -> Dict[str, Any]:
    """Load manifest metadata plus per-file hashes from an export directory."""
    manifest_path = export_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json was not found under {export_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    objects: Dict[str, Dict[str, str]] = {}
    folder_to_type = {"tables": "tables", "views": "views", "procedures": "procedures"}

    for folder_name, category in folder_to_type.items():
        folder_path = export_dir / folder_name
        if not folder_path.exists():
            continue
        for sql_file in sorted(folder_path.glob("*.sql")):
            relative_path = sql_file.relative_to(export_dir).as_posix()
            qualified_name = sql_file.stem
            sha256 = hashlib.sha256(sql_file.read_bytes()).hexdigest()
            key = f"{category}:{qualified_name}"
            objects[key] = {
                "category": category,
                "qualified_name": qualified_name,
                "relative_path": relative_path,
                "sha256": sha256,
            }

    return {"manifest": manifest, "objects": objects}



def group_snapshot_keys(keys: Iterable[str], object_map: Mapping[str, Mapping[str, str]]) -> Dict[str, List[str]]:
    """Group diff keys into tables, views, and procedures for human-readable reports."""
    grouped = {"tables": [], "views": [], "procedures": []}
    for key in keys:
        item = object_map[key]
        grouped[item["category"]].append(item["qualified_name"])
    for category in grouped:
        grouped[category].sort()
    return grouped



def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for extraction runs."""
    parser = argparse.ArgumentParser(
        description="Extract Microsoft Fabric Lakehouse SQL schema into version-controlled SQL files.",
    )
    parser.add_argument("--endpoint", required=True, help="Fabric Lakehouse SQL Analytics Endpoint host or URL")
    parser.add_argument("--database", required=True, help="Lakehouse SQL database name")
    parser.add_argument("--output-dir", required=True, help="Directory where DDL files and manifest.json will be written")
    parser.add_argument(
        "--auth-method",
        choices=list(VALID_AUTH_METHODS),
        default="interactive",
        help="Azure AD authentication mode to use when connecting to the SQL endpoint",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the objects that would be exported without writing any files",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity for the extraction run",
    )
    return parser



def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for direct execution."""
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        manifest = export_schema(
            endpoint=args.endpoint,
            database=args.database,
            output_dir=args.output_dir,
            auth_method=args.auth_method,
            dry_run=args.dry_run,
            logger=LOGGER,
        )
    except Exception as exc:
        LOGGER.error("Schema extraction failed: %s", exc)
        LOGGER.debug("Failure details", exc_info=True)
        return 1

    if args.dry_run:
        LOGGER.info(
            "Dry run summary: %s table(s), %s view(s), %s procedure(s)",
            manifest["table_count"],
            manifest["view_count"],
            manifest["procedure_count"],
        )
    else:
        LOGGER.info("Manifest hash: %s", manifest["ddl_sha256"])
    return 0


__all__ = [
    "FabricSchemaExtractor",
    "SchemaExtractionError",
    "compare_schemas",
    "export_schema",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
