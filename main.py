from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv


LOGGER = logging.getLogger("airtable_baserow_migrator")


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _sanitize_name(name: str, fallback: str) -> str:
    cleaned = re.sub(r"\s+", " ", (name or "").strip())
    cleaned = re.sub(r"[^\w \-.:/]", "_", cleaned)
    return cleaned[:255] or fallback


def _unique_name(candidate: str, used: set[str]) -> str:
    if candidate not in used:
        used.add(candidate)
        return candidate
    idx = 2
    while True:
        alt = f"{candidate} ({idx})"
        if alt not in used:
            used.add(alt)
            return alt
        idx += 1


@dataclass(frozen=True)
class Config:
    airtable_pat: str
    airtable_workspace_id: str
    baserow_url: str
    baserow_workspace_id: int
    baserow_management_token: str
    baserow_management_token_type: str
    baserow_database_token: Optional[str]
    sqlite_path: Path
    attachments_dir: Path
    request_timeout: int
    batch_size: int
    dry_run: bool
    airtable_base_ids: Optional[set[str]]
    report_path: Path

    @staticmethod
    def from_env(dry_run_override: Optional[bool] = None) -> "Config":
        load_dotenv()
        required = {
            "AIRTABLE_PAT": os.getenv("AIRTABLE_PAT", ""),
            "AIRTABLE_WORKSPACE_ID": os.getenv("AIRTABLE_WORKSPACE_ID", ""),
            "BASEROW_URL": os.getenv("BASEROW_URL", ""),
            "BASEROW_WORKSPACE_ID": os.getenv("BASEROW_WORKSPACE_ID", ""),
            "BASEROW_MANAGEMENT_TOKEN": os.getenv("BASEROW_MANAGEMENT_TOKEN", ""),
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError(
                "Missing required environment variables: " + ", ".join(sorted(missing))
            )
        token_type = os.getenv("BASEROW_MANAGEMENT_TOKEN_TYPE", "Token").strip()
        if token_type not in {"Token", "JWT"}:
            raise ValueError("BASEROW_MANAGEMENT_TOKEN_TYPE must be either Token or JWT")

        base_ids_raw = os.getenv("AIRTABLE_BASE_IDS", "").strip()
        base_ids = None
        if base_ids_raw:
            base_ids = {item.strip() for item in base_ids_raw.split(",") if item.strip()}

        dry_run_env = _to_bool(os.getenv("DRY_RUN", "false"))
        dry_run = dry_run_override if dry_run_override is not None else dry_run_env

        return Config(
            airtable_pat=required["AIRTABLE_PAT"],
            airtable_workspace_id=required["AIRTABLE_WORKSPACE_ID"],
            baserow_url=required["BASEROW_URL"].rstrip("/"),
            baserow_workspace_id=int(required["BASEROW_WORKSPACE_ID"]),
            baserow_management_token=required["BASEROW_MANAGEMENT_TOKEN"],
            baserow_management_token_type=token_type,
            baserow_database_token=os.getenv("BASEROW_DATABASE_TOKEN", "").strip() or None,
            sqlite_path=Path(os.getenv("SQLITE_PATH", "id_mapping.db")),
            attachments_dir=Path(os.getenv("ATTACHMENTS_DIR", "attachments_cache")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "60")),
            batch_size=int(os.getenv("BATCH_SIZE", "50")),
            dry_run=dry_run,
            airtable_base_ids=base_ids,
            report_path=Path(os.getenv("REPORT_PATH", "migration_report.json")),
        )


class MappingStore:
    def __init__(self, db_path: Path) -> None:
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS base_map (
                airtable_base_id TEXT PRIMARY KEY,
                airtable_base_name TEXT NOT NULL,
                baserow_database_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS table_map (
                airtable_base_id TEXT NOT NULL,
                airtable_table_id TEXT PRIMARY KEY,
                airtable_table_name TEXT NOT NULL,
                baserow_table_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS field_map (
                airtable_table_id TEXT NOT NULL,
                airtable_field_id TEXT NOT NULL,
                airtable_field_name TEXT NOT NULL,
                baserow_field_id INTEGER,
                baserow_field_name TEXT NOT NULL,
                baserow_field_type TEXT NOT NULL,
                linked_target_airtable_table_id TEXT,
                PRIMARY KEY (airtable_table_id, airtable_field_id)
            );
            CREATE TABLE IF NOT EXISTS record_map (
                airtable_table_id TEXT NOT NULL,
                airtable_record_id TEXT NOT NULL,
                baserow_row_id INTEGER NOT NULL,
                PRIMARY KEY (airtable_table_id, airtable_record_id)
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def get_base(self, airtable_base_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT baserow_database_id FROM base_map WHERE airtable_base_id = ?",
            (airtable_base_id,),
        ).fetchone()
        return row[0] if row else None

    def set_base(self, airtable_base_id: str, airtable_base_name: str, baserow_database_id: int) -> None:
        self.conn.execute(
            """
            INSERT INTO base_map (airtable_base_id, airtable_base_name, baserow_database_id)
            VALUES (?, ?, ?)
            ON CONFLICT(airtable_base_id) DO UPDATE SET
                airtable_base_name=excluded.airtable_base_name,
                baserow_database_id=excluded.baserow_database_id
            """,
            (airtable_base_id, airtable_base_name, baserow_database_id),
        )
        self.conn.commit()

    def get_table(self, airtable_table_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT baserow_table_id FROM table_map WHERE airtable_table_id = ?",
            (airtable_table_id,),
        ).fetchone()
        return row[0] if row else None

    def get_base_id_for_table(self, airtable_table_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT airtable_base_id FROM table_map WHERE airtable_table_id = ?",
            (airtable_table_id,),
        ).fetchone()
        return row[0] if row else None

    def set_table(
        self,
        airtable_base_id: str,
        airtable_table_id: str,
        airtable_table_name: str,
        baserow_table_id: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO table_map (airtable_base_id, airtable_table_id, airtable_table_name, baserow_table_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(airtable_table_id) DO UPDATE SET
                airtable_base_id=excluded.airtable_base_id,
                airtable_table_name=excluded.airtable_table_name,
                baserow_table_id=excluded.baserow_table_id
            """,
            (airtable_base_id, airtable_table_id, airtable_table_name, baserow_table_id),
        )
        self.conn.commit()

    def set_field(
        self,
        airtable_table_id: str,
        airtable_field_id: str,
        airtable_field_name: str,
        baserow_field_id: Optional[int],
        baserow_field_name: str,
        baserow_field_type: str,
        linked_target_airtable_table_id: Optional[str],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO field_map
            (airtable_table_id, airtable_field_id, airtable_field_name, baserow_field_id,
             baserow_field_name, baserow_field_type, linked_target_airtable_table_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airtable_table_id, airtable_field_id) DO UPDATE SET
                airtable_field_name=excluded.airtable_field_name,
                baserow_field_id=excluded.baserow_field_id,
                baserow_field_name=excluded.baserow_field_name,
                baserow_field_type=excluded.baserow_field_type,
                linked_target_airtable_table_id=excluded.linked_target_airtable_table_id
            """,
            (
                airtable_table_id,
                airtable_field_id,
                airtable_field_name,
                baserow_field_id,
                baserow_field_name,
                baserow_field_type,
                linked_target_airtable_table_id,
            ),
        )
        self.conn.commit()

    def get_fields_for_table(self, airtable_table_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT airtable_field_id, airtable_field_name, baserow_field_id, baserow_field_name,
                   baserow_field_type, linked_target_airtable_table_id
            FROM field_map
            WHERE airtable_table_id = ?
            """,
            (airtable_table_id,),
        ).fetchall()
        output = []
        for row in rows:
            output.append(
                {
                    "airtable_field_id": row[0],
                    "airtable_field_name": row[1],
                    "baserow_field_id": row[2],
                    "baserow_field_name": row[3],
                    "baserow_field_type": row[4],
                    "linked_target_airtable_table_id": row[5],
                }
            )
        return output

    def get_record(self, airtable_table_id: str, airtable_record_id: str) -> Optional[int]:
        row = self.conn.execute(
            """
            SELECT baserow_row_id
            FROM record_map
            WHERE airtable_table_id = ? AND airtable_record_id = ?
            """,
            (airtable_table_id, airtable_record_id),
        ).fetchone()
        return row[0] if row else None

    def set_record(self, airtable_table_id: str, airtable_record_id: str, baserow_row_id: int) -> None:
        self.conn.execute(
            """
            INSERT INTO record_map (airtable_table_id, airtable_record_id, baserow_row_id)
            VALUES (?, ?, ?)
            ON CONFLICT(airtable_table_id, airtable_record_id) DO UPDATE SET
                baserow_row_id=excluded.baserow_row_id
            """,
            (airtable_table_id, airtable_record_id, baserow_row_id),
        )
        self.conn.commit()


class Migrator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.mapping = MappingStore(config.sqlite_path)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "airtable-baserow-migrator/1.0"})
        self.attachments_dir = config.attachments_dir
        self.attachments_dir.mkdir(parents=True, exist_ok=True)
        self._fake_id_counter = 10_000
        self.base_names: Dict[str, str] = {}
        self.table_names: Dict[str, str] = {}
        self.report: Dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "dry_run": config.dry_run,
            "airtable_workspace_id": config.airtable_workspace_id,
            "bases": {},
            "errors": [],
            "totals": {
                "bases_discovered": 0,
                "bases_processed": 0,
                "tables_discovered": 0,
                "tables_processed": 0,
                "rows_created": 0,
                "rows_skipped_existing": 0,
                "rows_patch_links": 0,
                "attachments_downloaded": 0,
                "attachments_reused_from_cache": 0,
                "files_uploaded_to_baserow": 0,
                "files_failed": 0,
            },
        }

    def close(self) -> None:
        self.mapping.close()
        self.session.close()

    def _add_error(self, stage: str, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        item: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "message": message,
        }
        if context:
            item["context"] = context
        self.report["errors"].append(item)
        LOGGER.error("%s | %s | context=%s", stage, message, context or {})

    def _base_report(self, base_id: str) -> Dict[str, Any]:
        base_name = self.base_names.get(base_id, base_id)
        bases = self.report["bases"]
        if base_id not in bases:
            bases[base_id] = {
                "base_id": base_id,
                "base_name": base_name,
                "baserow_database_id": None,
                "tables": {},
            }
        return bases[base_id]

    def _table_report(self, base_id: str, table_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        base_report = self._base_report(base_id)
        tables = base_report["tables"]
        resolved_table_name = table_name or self.table_names.get(table_id, table_id)
        if table_id not in tables:
            tables[table_id] = {
                "table_id": table_id,
                "table_name": resolved_table_name,
                "baserow_table_id": None,
                "fields_created": 0,
                "link_fields_created": 0,
                "rows_created": 0,
                "rows_skipped_existing": 0,
                "rows_patched_links": 0,
                "attachments_downloaded": 0,
                "attachments_reused_from_cache": 0,
                "files_uploaded_to_baserow": 0,
                "files_failed": 0,
                "errors": [],
            }
        self.table_names[table_id] = resolved_table_name
        return tables[table_id]

    def _write_report(self) -> None:
        self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        output = json.dumps(self.report, indent=2, ensure_ascii=True)
        self.config.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.report_path.write_text(output, encoding="utf-8")
        LOGGER.info("Migration report written to %s", self.config.report_path)

    def _airtable_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.config.airtable_pat}"}

    def _baserow_management_headers(self, content_type_json: bool = True) -> Dict[str, str]:
        headers = {
            "Authorization": f"{self.config.baserow_management_token_type} {self.config.baserow_management_token}"
        }
        if content_type_json:
            headers["Content-Type"] = "application/json"
        return headers

    def _baserow_row_headers(self) -> Dict[str, str]:
        if self.config.baserow_database_token:
            return {"Authorization": f"Token {self.config.baserow_database_token}"}
        return self._baserow_management_headers(content_type_json=False)

    def _request_with_retries(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        expected_statuses: Iterable[int],
        **kwargs: Any,
    ) -> requests.Response:
        for attempt in range(1, 6):
            resp = self.session.request(
                method=method,
                url=url,
                headers=headers,
                timeout=self.config.request_timeout,
                **kwargs,
            )
            if resp.status_code in expected_statuses:
                return resp
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < 5:
                sleep_seconds = 2 ** attempt
                LOGGER.warning("Retrying %s after HTTP %s in %ss", url, resp.status_code, sleep_seconds)
                time.sleep(sleep_seconds)
                continue
            raise RuntimeError(
                f"Request failed: {method} {url} -> {resp.status_code}: {resp.text[:500]}"
            )
        raise RuntimeError(f"Request retries exhausted for {method} {url}")

    def airtable_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"https://api.airtable.com{path}"
        resp = self._request_with_retries(
            "GET",
            url,
            headers=self._airtable_headers(),
            expected_statuses={200},
            params=params,
        )
        return resp.json()

    def baserow_management_request(
        self, method: str, path: str, expected_statuses: Iterable[int], json_body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = f"{self.config.baserow_url}{path}"
        resp = self._request_with_retries(
            method,
            url,
            headers=self._baserow_management_headers(),
            expected_statuses=expected_statuses,
            data=json.dumps(json_body) if json_body is not None else None,
        )
        return resp.json() if resp.text.strip() else {}

    def baserow_row_request(
        self,
        method: str,
        path: str,
        expected_statuses: Iterable[int],
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.config.baserow_url}{path}"
        headers = self._baserow_row_headers()
        headers["Content-Type"] = "application/json"
        resp = self._request_with_retries(
            method,
            url,
            headers=headers,
            expected_statuses=expected_statuses,
            data=json.dumps(json_body) if json_body is not None else None,
        )
        return resp.json() if resp.text.strip() else {}

    def discover_workspace_bases(self) -> List[Dict[str, Any]]:
        payload = self.airtable_get("/v0/meta/bases")
        bases = payload.get("bases", [])
        filtered = [
            b
            for b in bases
            if b.get("workspaceId") == self.config.airtable_workspace_id
            and (self.config.airtable_base_ids is None or b.get("id") in self.config.airtable_base_ids)
        ]
        LOGGER.info("Discovered %s base(s) in Airtable workspace", len(filtered))
        return filtered

    def get_base_schema(self, base_id: str) -> List[Dict[str, Any]]:
        payload = self.airtable_get(f"/v0/meta/bases/{base_id}/tables")
        return payload.get("tables", [])

    def map_field_definition(self, field: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
        ftype = field.get("type", "")
        options = field.get("options", {}) or {}

        if ftype in {"singleLineText", "email", "url", "phoneNumber", "createdBy", "lastModifiedBy"}:
            return "text", {}, False
        if ftype in {"multilineText", "richText"}:
            return "long_text", {}, False
        if ftype in {"number", "currency", "percent", "rating", "duration", "autoNumber", "count"}:
            return "number", {"number_decimal_places": 5}, False
        if ftype in {"checkbox"}:
            return "boolean", {}, False
        if ftype in {"date", "dateTime", "createdTime", "lastModifiedTime"}:
            include_time = "timeFormat" in options or ftype in {"dateTime", "createdTime", "lastModifiedTime"}
            return "date", {"date_include_time": include_time}, False
        if ftype == "singleSelect":
            choices = options.get("choices", [])
            return (
                "single_select",
                {"select_options": [{"value": c.get("name", "Option")} for c in choices]},
                False,
            )
        if ftype == "multipleSelects":
            choices = options.get("choices", [])
            return (
                "multiple_select",
                {"select_options": [{"value": c.get("name", "Option")} for c in choices]},
                False,
            )
        if ftype == "multipleAttachments":
            return "file", {}, False
        if ftype == "multipleRecordLinks":
            linked_table_id = options.get("linkedTableId")
            return "link_row", {"linked_target_airtable_table_id": linked_table_id}, True
        # Formulas/lookups/rollups and unsupported types fallback to text.
        return "long_text", {}, False

    def create_baserow_database_if_needed(self, base: Dict[str, Any]) -> int:
        base_id = base["id"]
        base_name = base.get("name", base_id)
        self.base_names[base_id] = base_name
        base_report = self._base_report(base_id)
        existing = self.mapping.get_base(base_id)
        if existing:
            base_report["baserow_database_id"] = existing
            return existing
        if self.config.dry_run:
            fake_id = self._next_fake_id()
            LOGGER.info("[dry-run] Would create Baserow database for base '%s'", base.get("name"))
            self.mapping.set_base(base_id, base.get("name", base_id), fake_id)
            base_report["baserow_database_id"] = fake_id
            return fake_id
        payload = self.baserow_management_request(
            "POST",
            "/api/applications/",
            {200},
            {
                "name": _sanitize_name(base.get("name", base_id), base_id),
                "workspace": self.config.baserow_workspace_id,
                "type": "database",
            },
        )
        db_id = int(payload["id"])
        self.mapping.set_base(base_id, base.get("name", base_id), db_id)
        base_report["baserow_database_id"] = db_id
        LOGGER.info("Created Baserow database %s for Airtable base %s", db_id, base_id)
        return db_id

    def create_table_skeleton(
        self,
        airtable_base_id: str,
        db_id: int,
        table: Dict[str, Any],
        first_field_name: str,
    ) -> int:
        table_report = self._table_report(
            airtable_base_id,
            table["id"],
            table.get("name", table["id"]),
        )
        existing = self.mapping.get_table(table["id"])
        if existing:
            table_report["baserow_table_id"] = existing
            return existing
        if self.config.dry_run:
            fake_id = self._next_fake_id()
            LOGGER.info("[dry-run] Would create table '%s'", table.get("name"))
            self.mapping.set_table(airtable_base_id, table["id"], table.get("name", table["id"]), fake_id)
            table_report["baserow_table_id"] = fake_id
            return fake_id
        payload = self.baserow_management_request(
            "POST",
            f"/api/database/tables/database/{db_id}/",
            {200},
            {
                "name": _sanitize_name(table.get("name", table["id"]), table["id"]),
                "data": [{"name": first_field_name, "type": "text"}],
            },
        )
        table_id = int(payload["id"])
        self.mapping.set_table(airtable_base_id, table["id"], table.get("name", table["id"]), table_id)
        table_report["baserow_table_id"] = table_id
        LOGGER.info("Created Baserow table %s for Airtable table %s", table_id, table["id"])
        return table_id

    def get_baserow_fields(self, table_id: int) -> List[Dict[str, Any]]:
        payload = self.baserow_management_request("GET", f"/api/database/fields/table/{table_id}/", {200})
        if isinstance(payload, list):
            return payload
        return payload.get("results", [])

    def create_field_if_needed(
        self,
        airtable_table_id: str,
        baserow_table_id: int,
        airtable_field: Dict[str, Any],
        baserow_field_name: str,
        baserow_type: str,
        extra: Dict[str, Any],
    ) -> Optional[int]:
        known = {f["airtable_field_id"]: f for f in self.mapping.get_fields_for_table(airtable_table_id)}
        if airtable_field["id"] in known and known[airtable_field["id"]]["baserow_field_id"]:
            return int(known[airtable_field["id"]]["baserow_field_id"])

        if self.config.dry_run:
            LOGGER.info(
                "[dry-run] Would create field '%s' (%s) in table %s",
                baserow_field_name,
                baserow_type,
                baserow_table_id,
            )
            fake_id = self._next_fake_id()
            self.mapping.set_field(
                airtable_table_id,
                airtable_field["id"],
                airtable_field.get("name", airtable_field["id"]),
                fake_id,
                baserow_field_name,
                baserow_type,
                extra.get("linked_target_airtable_table_id"),
            )
            return fake_id

        body = {"name": baserow_field_name, "type": baserow_type}
        if baserow_type in {"single_select", "multiple_select"}:
            body["select_options"] = extra.get("select_options", [])
        if baserow_type == "number":
            body["number_decimal_places"] = extra.get("number_decimal_places", 0)
        if baserow_type == "date":
            body["date_include_time"] = extra.get("date_include_time", False)

        payload = self.baserow_management_request(
            "POST",
            f"/api/database/fields/table/{baserow_table_id}/",
            {200},
            body,
        )
        field_id = int(payload["id"])
        self.mapping.set_field(
            airtable_table_id,
            airtable_field["id"],
            airtable_field.get("name", airtable_field["id"]),
            field_id,
            baserow_field_name,
            baserow_type,
            extra.get("linked_target_airtable_table_id"),
        )
        base_id = self.mapping.get_base_id_for_table(airtable_table_id) or airtable_table_id
        table_report = self._table_report(base_id, airtable_table_id)
        table_report["fields_created"] += 1
        LOGGER.info("Created field '%s' in Baserow table %s", baserow_field_name, baserow_table_id)
        return field_id

    def create_link_field_if_needed(
        self,
        airtable_table_id: str,
        baserow_table_id: int,
        airtable_field: Dict[str, Any],
        baserow_field_name: str,
        target_baserow_table_id: int,
        linked_target_airtable_table_id: str,
    ) -> Optional[int]:
        known = {f["airtable_field_id"]: f for f in self.mapping.get_fields_for_table(airtable_table_id)}
        if (
            airtable_field["id"] in known
            and known[airtable_field["id"]]["baserow_field_type"] == "link_row"
            and known[airtable_field["id"]]["baserow_field_id"] is not None
        ):
            return known[airtable_field["id"]]["baserow_field_id"]

        if self.config.dry_run:
            LOGGER.info(
                "[dry-run] Would create link field '%s' in table %s -> table %s",
                baserow_field_name,
                baserow_table_id,
                target_baserow_table_id,
            )
            fake_id = self._next_fake_id()
            self.mapping.set_field(
                airtable_table_id,
                airtable_field["id"],
                airtable_field.get("name", airtable_field["id"]),
                fake_id,
                baserow_field_name,
                "link_row",
                linked_target_airtable_table_id,
            )
            return fake_id

        payload = self.baserow_management_request(
            "POST",
            f"/api/database/fields/table/{baserow_table_id}/",
            {200},
            {
                "name": baserow_field_name,
                "type": "link_row",
                "link_row_table": target_baserow_table_id,
                "has_related_field": True,
            },
        )
        field_id = int(payload["id"])
        self.mapping.set_field(
            airtable_table_id,
            airtable_field["id"],
            airtable_field.get("name", airtable_field["id"]),
            field_id,
            baserow_field_name,
            "link_row",
            linked_target_airtable_table_id,
        )
        base_id = self.mapping.get_base_id_for_table(airtable_table_id) or airtable_table_id
        table_report = self._table_report(base_id, airtable_table_id)
        table_report["link_fields_created"] += 1
        LOGGER.info("Created link-row field '%s' in table %s", baserow_field_name, baserow_table_id)
        return field_id

    def iter_airtable_records(self, base_id: str, table_id: str) -> Iterable[Dict[str, Any]]:
        offset = None
        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            payload = self.airtable_get(f"/v0/{base_id}/{quote(table_id, safe='')}", params=params)
            for record in payload.get("records", []):
                yield record
            offset = payload.get("offset")
            if not offset:
                break

    def _next_fake_id(self) -> int:
        self._fake_id_counter += 1
        return self._fake_id_counter

    def _download_attachment(self, attachment: Dict[str, Any], record_id: str, table_report: Dict[str, Any]) -> Optional[Path]:
        url = attachment.get("url")
        if not url:
            return None
        attachment_id = attachment.get("id", "attachment")
        filename = _sanitize_name(attachment.get("filename", "file.bin"), "file.bin")
        destination = self.attachments_dir / f"{record_id}_{attachment_id}_{filename}"
        if destination.exists():
            table_report["attachments_reused_from_cache"] += 1
            self.report["totals"]["attachments_reused_from_cache"] += 1
            return destination
        resp = self._request_with_retries("GET", url, headers={}, expected_statuses={200}, stream=True)
        with destination.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 128):
                if chunk:
                    fh.write(chunk)
        table_report["attachments_downloaded"] += 1
        self.report["totals"]["attachments_downloaded"] += 1
        return destination

    def _upload_file_to_baserow(self, file_path: Path, table_report: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.dry_run:
            return {"name": file_path.name}
        url = f"{self.config.baserow_url}/api/user-files/upload-file/"
        headers = self._baserow_management_headers(content_type_json=False)
        with file_path.open("rb") as fh:
            resp = self._request_with_retries(
                "POST",
                url,
                headers=headers,
                expected_statuses={200},
                files={"file": (file_path.name, fh)},
            )
        table_report["files_uploaded_to_baserow"] += 1
        self.report["totals"]["files_uploaded_to_baserow"] += 1
        return resp.json()

    def _transform_value(
        self,
        baserow_type: str,
        value: Any,
        record_id: str,
        table_report: Dict[str, Any],
    ) -> Any:
        if value is None:
            return None
        if baserow_type == "file":
            uploaded_files = []
            for attachment in value or []:
                local_path = self._download_attachment(attachment, record_id, table_report)
                if not local_path:
                    continue
                try:
                    uploaded_files.append(self._upload_file_to_baserow(local_path, table_report))
                except Exception as exc:
                    table_report["files_failed"] += 1
                    self.report["totals"]["files_failed"] += 1
                    self._add_error(
                        "upload_attachment",
                        str(exc),
                        {"record_id": record_id, "file_path": str(local_path)},
                    )
            return uploaded_files
        if baserow_type in {"single_select"}:
            return str(value) if value is not None else None
        if baserow_type in {"multiple_select"}:
            return [str(v) for v in (value or [])]
        if baserow_type == "number":
            return value
        if baserow_type == "boolean":
            return bool(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True)
        return value

    def migrate_rows_phase_a(self, base_id: str, table: Dict[str, Any]) -> None:
        airtable_table_id = table["id"]
        table_report = self._table_report(
            base_id,
            airtable_table_id,
            table.get("name", airtable_table_id),
        )
        baserow_table_id = self.mapping.get_table(airtable_table_id)
        if not baserow_table_id:
            raise RuntimeError(f"Missing Baserow table mapping for Airtable table {airtable_table_id}")

        fields_meta = self.mapping.get_fields_for_table(airtable_table_id)
        link_field_names = {
            item["airtable_field_name"]
            for item in fields_meta
            if item["baserow_field_type"] == "link_row"
        }
        fields_by_airtable_name = {item["airtable_field_name"]: item for item in fields_meta}

        created = 0
        skipped = 0
        for record in self.iter_airtable_records(base_id, airtable_table_id):
            airtable_record_id = record["id"]
            if self.mapping.get_record(airtable_table_id, airtable_record_id):
                skipped += 1
                continue
            source_fields = record.get("fields", {})
            row_payload: Dict[str, Any] = {}

            for source_name, source_value in source_fields.items():
                if source_name in link_field_names:
                    continue
                mapped = fields_by_airtable_name.get(source_name)
                if not mapped:
                    continue
                row_payload[mapped["baserow_field_name"]] = self._transform_value(
                    mapped["baserow_field_type"], source_value, airtable_record_id, table_report
                )

            if self.config.dry_run:
                LOGGER.info("[dry-run] Would create row in table %s with fields: %s", baserow_table_id, list(row_payload))
                continue

            row = self.baserow_row_request(
                "POST",
                f"/api/database/rows/table/{baserow_table_id}/?user_field_names=true",
                {200},
                row_payload,
            )
            self.mapping.set_record(airtable_table_id, airtable_record_id, int(row["id"]))
            created += 1
            if created % self.config.batch_size == 0:
                LOGGER.info("Table %s: created %s rows", airtable_table_id, created)

        table_report["rows_created"] += created
        table_report["rows_skipped_existing"] += skipped
        self.report["totals"]["rows_created"] += created
        self.report["totals"]["rows_skipped_existing"] += skipped
        LOGGER.info(
            "Phase A rows done for table %s (created=%s, skipped_existing=%s)",
            airtable_table_id,
            created,
            skipped,
        )

    def migrate_links_phase_b(self, base_id: str, table: Dict[str, Any]) -> None:
        airtable_table_id = table["id"]
        table_report = self._table_report(
            base_id,
            airtable_table_id,
            table.get("name", airtable_table_id),
        )
        baserow_table_id = self.mapping.get_table(airtable_table_id)
        if not baserow_table_id:
            return
        fields_meta = self.mapping.get_fields_for_table(airtable_table_id)
        link_fields = [item for item in fields_meta if item["baserow_field_type"] == "link_row"]
        if not link_fields:
            return
        link_fields_by_name = {item["airtable_field_name"]: item for item in link_fields}
        patched = 0

        for record in self.iter_airtable_records(base_id, airtable_table_id):
            baserow_row_id = self.mapping.get_record(airtable_table_id, record["id"])
            if not baserow_row_id:
                continue
            patch_payload: Dict[str, Any] = {}
            source_fields = record.get("fields", {})
            for source_name, mapped in link_fields_by_name.items():
                linked_ids = source_fields.get(source_name, [])
                if not isinstance(linked_ids, list):
                    continue
                target_airtable_table_id = mapped["linked_target_airtable_table_id"]
                if not target_airtable_table_id:
                    continue
                target_row_ids = []
                for linked_airtable_record_id in linked_ids:
                    target_row_id = self.mapping.get_record(target_airtable_table_id, linked_airtable_record_id)
                    if target_row_id:
                        target_row_ids.append(target_row_id)
                patch_payload[mapped["baserow_field_name"]] = target_row_ids

            if not patch_payload:
                continue
            if self.config.dry_run:
                LOGGER.info(
                    "[dry-run] Would patch row %s in table %s with link fields %s",
                    baserow_row_id,
                    baserow_table_id,
                    list(patch_payload),
                )
                continue

            self.baserow_row_request(
                "PATCH",
                f"/api/database/rows/table/{baserow_table_id}/{baserow_row_id}/?user_field_names=true",
                {200},
                patch_payload,
            )
            patched += 1
            if patched % self.config.batch_size == 0:
                LOGGER.info("Table %s: patched %s rows with link values", airtable_table_id, patched)
        table_report["rows_patched_links"] += patched
        self.report["totals"]["rows_patch_links"] += patched
        LOGGER.info("Phase B links done for table %s (patched=%s)", airtable_table_id, patched)

    def create_schema(self) -> List[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
        discovered = self.discover_workspace_bases()
        self.report["totals"]["bases_discovered"] = len(discovered)
        if not discovered:
            LOGGER.warning("No bases found for workspace '%s'", self.config.airtable_workspace_id)
            return []

        plan: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
        deferred_links: List[Tuple[str, int, Dict[str, Any], str, str]] = []

        for base in discovered:
            base_id = base["id"]
            base_name = base.get("name", base_id)
            self.base_names[base_id] = base_name
            tables = self.get_base_schema(base_id)
            self.report["totals"]["tables_discovered"] += len(tables)
            plan.append((base, tables))
            db_id = self.create_baserow_database_if_needed(base)
            self.report["totals"]["bases_processed"] += 1

            for table in tables:
                self.table_names[table["id"]] = table.get("name", table["id"])
                self.report["totals"]["tables_processed"] += 1
                fields = table.get("fields", [])
                fields_by_id = {field["id"]: field for field in fields}
                primary_field = fields_by_id.get(table.get("primaryFieldId"), fields[0] if fields else None)
                first_field_name = _sanitize_name(
                    (primary_field or {}).get("name", "Primary"), "Primary"
                )
                baserow_table_id = self.create_table_skeleton(base_id, db_id, table, first_field_name)
                existing_fields = self.get_baserow_fields(baserow_table_id) if not self.config.dry_run else []
                primary_baserow_id = None
                if existing_fields:
                    for bf in existing_fields:
                        if bf.get("name") == first_field_name:
                            primary_baserow_id = int(bf["id"])
                            break

                used_names = {first_field_name}
                for field in fields:
                    desired_name = _sanitize_name(field.get("name", "Field"), "Field")
                    if desired_name == first_field_name:
                        baserow_field_name = first_field_name
                    else:
                        baserow_field_name = _unique_name(desired_name, used_names)
                    baserow_type, extra, defer = self.map_field_definition(field)
                    if field.get("id") == table.get("primaryFieldId"):
                        self.mapping.set_field(
                            table["id"],
                            field["id"],
                            field.get("name", field["id"]),
                            primary_baserow_id,
                            baserow_field_name,
                            "text",
                            None,
                        )
                        continue
                    if defer:
                        linked_target = extra.get("linked_target_airtable_table_id")
                        if not linked_target:
                            LOGGER.warning("Skipping link field %s: missing linked table metadata", field.get("name"))
                            table_report = self._table_report(
                                base_id,
                                table["id"],
                                table.get("name", table["id"]),
                            )
                            table_report["errors"].append("Missing linked table metadata for a link field.")
                            continue
                        deferred_links.append(
                            (
                                table["id"],
                                baserow_table_id,
                                field,
                                baserow_field_name,
                                linked_target,
                            )
                        )
                        self.mapping.set_field(
                            table["id"],
                            field["id"],
                            field.get("name", field["id"]),
                            None,
                            baserow_field_name,
                            "link_row",
                            linked_target,
                        )
                        continue
                    self.create_field_if_needed(
                        table["id"],
                        baserow_table_id,
                        field,
                        baserow_field_name,
                        baserow_type,
                        extra,
                    )

        # Second pass for relation fields.
        for airtable_table_id, baserow_table_id, field, baserow_field_name, linked_target in deferred_links:
            target_baserow_table_id = self.mapping.get_table(linked_target)
            if not target_baserow_table_id:
                LOGGER.warning(
                    "Skipping relation field %s on table %s; target table %s not created",
                    field.get("name"),
                    airtable_table_id,
                    linked_target,
                )
                self._add_error(
                    "create_link_field",
                    "Target table missing for relation field",
                    {
                        "airtable_table_id": airtable_table_id,
                        "linked_target_airtable_table_id": linked_target,
                        "field_name": field.get("name"),
                    },
                )
                continue
            self.create_link_field_if_needed(
                airtable_table_id,
                baserow_table_id,
                field,
                baserow_field_name,
                target_baserow_table_id,
                linked_target,
            )

        return plan

    def migrate_data(self, plan: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]]) -> None:
        for base, tables in plan:
            base_id = base["id"]
            LOGGER.info("Migrating rows for base %s (%s)", base.get("name", base_id), base_id)
            for table in tables:
                self.migrate_rows_phase_a(base_id, table)
            for table in tables:
                self.migrate_links_phase_b(base_id, table)

    def run(self) -> None:
        try:
            plan = self.create_schema()
            self.migrate_data(plan)
            LOGGER.info("Migration completed successfully.")
        finally:
            self._write_report()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate Airtable workspace structure + data to self-hosted Baserow."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read Airtable metadata and records, but do not create/modify Baserow data.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level.",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional path for migration report JSON (defaults to REPORT_PATH env or migration_report.json).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    try:
        config = Config.from_env(dry_run_override=args.dry_run)
    except Exception as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2
    if args.report_path:
        config = replace(config, report_path=Path(args.report_path))

    migrator = Migrator(config)
    try:
        migrator.run()
    except Exception as exc:
        LOGGER.exception("Migration failed: %s", exc)
        return 1
    finally:
        migrator.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
