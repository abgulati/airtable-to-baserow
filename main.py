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

BASEROW_SELECT_COLORS = [
    "light-blue", "light-green", "light-orange", "light-red",
    "light-yellow", "light-gray", "light-cyan", "light-pink",
    "blue", "green", "orange", "red", "yellow", "gray",
]

BASEROW_VIEW_TYPES = {
    "grid": "grid",           # Direct mapping
    "kanban": "kanban",       # Direct mapping
    "gallery": "gallery",     # Direct mapping
    "calendar": "calendar",   # Direct mapping
    "timeline": "timeline",   # Direct mapping (NOT gantt!)
    "form": "form",           # Direct mapping
}

UNSUPPORTED_AIRTABLE_VIEW_TYPES = {
    "map",      # Baserow doesn't support map views
    "list",     # Baserow doesn't support list views
    "gantt",    # Baserow doesn't support gantt views
}


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_link_row_table_id(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _sanitize_name(name: str, fallback: str) -> str:
    cleaned = re.sub(r"\s+", " ", (name or "").strip())
    cleaned = re.sub(r"[^\w \-.:/]", "_", cleaned)
    return cleaned[:255] or fallback


def _normalize_view_name(name: str) -> str:
    return _sanitize_name(name, "View").casefold()


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
    baserow_refresh_token: Optional[str]
    max_token_refreshes: int
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
            baserow_refresh_token=os.getenv("BASEROW_REFRESH_TOKEN", "").strip() or None,
            max_token_refreshes=int(os.getenv("MAX_TOKEN_REFRESHES", "3")),
            sqlite_path=Path(os.getenv("SQLITE_PATH", "id_mapping.db")),
            attachments_dir=Path(os.getenv("ATTACHMENTS_DIR", "attachments_cache")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "60")),
            batch_size=int(os.getenv("BATCH_SIZE", "50")),
            dry_run=dry_run,
            airtable_base_ids=base_ids,
            report_path=Path(os.getenv("REPORT_PATH", "migration_report.json")),
        )


@dataclass(frozen=True)
class LinkFieldResult:
    field_id: int
    created_new_field: bool
    reverse_field_id: Optional[int] = None


@dataclass(frozen=True)
class PendingReverseLinkClaim:
    source_airtable_table_id: str
    source_airtable_field_id: str
    source_field_name: str
    source_baserow_table_id: int
    source_baserow_field_id: int
    reverse_field_id: int


class MappingStore:
    def __init__(self, db_path: Path, in_memory: bool = False) -> None:
        if in_memory:
            self.conn = sqlite3.connect(":memory:")
        else:
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
                reverse_baserow_field_id INTEGER,
                linked_target_airtable_table_id TEXT,
                PRIMARY KEY (airtable_table_id, airtable_field_id)
            );
            CREATE TABLE IF NOT EXISTS record_map (
                airtable_table_id TEXT NOT NULL,
                airtable_record_id TEXT NOT NULL,
                baserow_row_id INTEGER NOT NULL,
                PRIMARY KEY (airtable_table_id, airtable_record_id)
            );
            CREATE TABLE IF NOT EXISTS view_map (
                airtable_table_id TEXT NOT NULL,
                airtable_view_id TEXT NOT NULL,
                airtable_view_name TEXT NOT NULL,
                airtable_view_type TEXT NOT NULL,
                baserow_view_id INTEGER,
                baserow_view_name TEXT NOT NULL,
                baserow_view_type TEXT NOT NULL,
                is_supported INTEGER NOT NULL DEFAULT 1,
                skip_reason TEXT,
                PRIMARY KEY (airtable_table_id, airtable_view_id)
            );
            """
        )
        field_map_columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(field_map)").fetchall()
        }
        if "reverse_baserow_field_id" not in field_map_columns:
            self.conn.execute("ALTER TABLE field_map ADD COLUMN reverse_baserow_field_id INTEGER")
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
        reverse_baserow_field_id: Optional[int] = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO field_map
            (airtable_table_id, airtable_field_id, airtable_field_name, baserow_field_id,
             baserow_field_name, baserow_field_type, linked_target_airtable_table_id,
             reverse_baserow_field_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airtable_table_id, airtable_field_id) DO UPDATE SET
                airtable_field_name=excluded.airtable_field_name,
                baserow_field_id=excluded.baserow_field_id,
                baserow_field_name=excluded.baserow_field_name,
                baserow_field_type=excluded.baserow_field_type,
                linked_target_airtable_table_id=excluded.linked_target_airtable_table_id,
                reverse_baserow_field_id=excluded.reverse_baserow_field_id
            """,
            (
                airtable_table_id,
                airtable_field_id,
                airtable_field_name,
                baserow_field_id,
                baserow_field_name,
                baserow_field_type,
                linked_target_airtable_table_id,
                reverse_baserow_field_id,
            ),
        )
        self.conn.commit()

    def get_fields_for_table(self, airtable_table_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT airtable_field_id, airtable_field_name, baserow_field_id, baserow_field_name,
                 baserow_field_type, linked_target_airtable_table_id, reverse_baserow_field_id
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
                    "reverse_baserow_field_id": row[6],
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

    def set_view(
        self,
        airtable_table_id: str,
        airtable_view_id: str,
        airtable_view_name: str,
        airtable_view_type: str,
        baserow_view_id: Optional[int],
        baserow_view_name: str,
        baserow_view_type: str,
        is_supported: int,
        skip_reason: Optional[str],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO view_map
            (airtable_table_id, airtable_view_id, airtable_view_name, airtable_view_type,
            baserow_view_id, baserow_view_name, baserow_view_type, is_supported, skip_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airtable_table_id, airtable_view_id) DO UPDATE SET
                airtable_view_name=excluded.airtable_view_name,
                airtable_view_type=excluded.airtable_view_type,
                baserow_view_id=excluded.baserow_view_id,
                baserow_view_name=excluded.baserow_view_name,
                baserow_view_type=excluded.baserow_view_type,
                is_supported=excluded.is_supported,
                skip_reason=excluded.skip_reason
            """,
            (
                airtable_table_id,
                airtable_view_id,
                airtable_view_name,
                airtable_view_type,
                baserow_view_id,
                baserow_view_name,
                baserow_view_type,
                is_supported,
                skip_reason,
            ),
        )
        self.conn.commit()

    def get_view(self, airtable_table_id: str, airtable_view_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT baserow_view_id, baserow_view_name, baserow_view_type, is_supported, skip_reason
            FROM view_map
            WHERE airtable_table_id = ? AND airtable_view_id = ?
            """,
            (airtable_table_id, airtable_view_id),
        ).fetchone()
        if row:
            return {
                "baserow_view_id": row[0],
                "baserow_view_name": row[1],
                "baserow_view_type": row[2],
                "is_supported": row[3],
                "skip_reason": row[4],
            }
        return None


class Migrator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.mapping = MappingStore(config.sqlite_path, in_memory=config.dry_run)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "airtable-baserow-migrator/1.0"})
        self.attachments_dir = config.attachments_dir
        self.attachments_dir.mkdir(parents=True, exist_ok=True)
        self._fake_id_counter = 10_000
        self.base_names: Dict[str, str] = {}
        self.table_names: Dict[str, str] = {}
        self._management_token = config.baserow_management_token
        self._refresh_token = config.baserow_refresh_token
        self._token_type = config.baserow_management_token_type
        self._token_refreshes_remaining = config.max_token_refreshes
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
                "rows_failed": 0,
                "fields_failed": 0,
                "link_patches_failed": 0,
                "views_total": 0,
                "views_migrated": 0,
                "views_created": 0,
                "views_adopted_existing": 0,
                "views_skipped_existing": 0,
                "views_failed": 0,
                "views_unsupported_skipped": 0,
                "views_total_skipped": 0,
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
                "rows_failed": 0,
                "views_total": 0,
                "views_migrated": 0,
                "views_created": 0,
                "views_adopted_existing": 0,
                "views_skipped_existing": 0,
                "views_failed": 0,
                "views_unsupported_skipped": 0,
                "errors": [],
            }
        self.table_names[table_id] = resolved_table_name
        return tables[table_id]

    def _write_report(self) -> None:
        self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        totals = self.report["totals"]
        output = json.dumps(self.report, indent=2, ensure_ascii=True)
        self.config.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.report_path.write_text(output, encoding="utf-8")
        LOGGER.info("Migration report written to %s", self.config.report_path)
        LOGGER.info(
            "View Migration Summary: Total=%s, Migrated=%s, Created=%s, Adopted Existing=%s, "
            "Skipped Existing=%s, Failed=%s, Unsupported Skipped=%s, Total Skipped=%s",
            totals["views_total"],
            totals["views_migrated"],
            totals["views_created"],
            totals["views_adopted_existing"],
            totals["views_skipped_existing"],
            totals["views_failed"],
            totals["views_unsupported_skipped"],
            totals["views_total_skipped"],
        )

    def _airtable_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.config.airtable_pat}"}

    def _baserow_management_headers(self, content_type_json: bool = True) -> Dict[str, str]:
        headers = {
            "Authorization": f"{self._token_type} {self._management_token}"
        }
        if content_type_json:
            headers["Content-Type"] = "application/json"
        return headers

    def _baserow_row_headers(self) -> Dict[str, str]:
        if self.config.baserow_database_token:
            return {"Authorization": f"Token {self.config.baserow_database_token}"}
        return self._baserow_management_headers(content_type_json=False)

    def _refresh_access_token(self) -> bool:
        """Use the refresh token to obtain a new JWT access token. Returns True on success."""
        if not self._refresh_token or self._token_type != "JWT":
            return False
        if self._token_refreshes_remaining <= 0:
            LOGGER.warning("Token refresh limit reached, no more refreshes allowed.")
            return False
        LOGGER.info("Attempting to refresh JWT access token (%s refresh(es) remaining)...", self._token_refreshes_remaining)
        try:
            resp = self.session.post(
                f"{self.config.baserow_url}/api/user/token-refresh/",
                headers={"Content-Type": "application/json"},
                data=json.dumps({"refresh_token": self._refresh_token}),
                timeout=self.config.request_timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._management_token = data["access_token"]
                self._token_refreshes_remaining -= 1
                LOGGER.info("Successfully refreshed JWT access token. %s refresh(es) remaining.", self._token_refreshes_remaining)
                return True
            LOGGER.warning("Token refresh failed with status %s: %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            LOGGER.warning("Token refresh request failed: %s", exc)
        return False

    def _request_with_retries(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        expected_statuses: Iterable[int],
        **kwargs: Any,
    ) -> requests.Response:
        refreshed_this_call = False
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
            if resp.status_code == 401 and not refreshed_this_call:
                if self._refresh_access_token():
                    refreshed_this_call = True
                    headers = {k: v for k, v in headers.items()}
                    headers["Authorization"] = f"{self._token_type} {self._management_token}"
                    continue
            if resp.status_code in {409, 429, 500, 502, 503, 504} and attempt < 5:
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
            if (b.get("workspaceId") is None or b.get("workspaceId") == self.config.airtable_workspace_id)
            and (self.config.airtable_base_ids is None or b.get("id") in self.config.airtable_base_ids)
        ]
        LOGGER.info("Discovered %s base(s) in Airtable workspace", len(filtered))
        return filtered

    def get_base_schema(self, base_id: str) -> List[Dict[str, Any]]:
        payload = self.airtable_get(f"/v0/meta/bases/{base_id}/tables")
        return payload.get("tables", [])
    
    def get_airtable_views(self, base_id: str, table_id: str) -> List[Dict[str, Any]]:
        """Fetch all views for an Airtable table."""
        payload = self.airtable_get(f"/v0/meta/bases/{base_id}/tables/{quote(table_id, safe='')}/views")
        return payload.get("views", [])

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
            choices = [c for c in options.get("choices", []) if (c.get("name") or "").strip()]
            return (
                "single_select",
                {"select_options": [
                    {"value": c["name"].strip(), "color": BASEROW_SELECT_COLORS[i % len(BASEROW_SELECT_COLORS)]}
                    for i, c in enumerate(choices)
                ]},
                False,
            )
        if ftype == "multipleSelects":
            choices = [c for c in options.get("choices", []) if (c.get("name") or "").strip()]
            return (
                "multiple_select",
                {"select_options": [
                    {"value": c["name"].strip(), "color": BASEROW_SELECT_COLORS[i % len(BASEROW_SELECT_COLORS)]}
                    for i, c in enumerate(choices)
                ]},
                False,
            )
        if ftype == "multipleAttachments":
            return "file", {}, False
        if ftype == "multipleRecordLinks":
            linked_table_id = options.get("linkedTableId")
            return (
                "link_row",
                {
                    "linked_target_airtable_table_id": linked_table_id,
                    "inverse_link_airtable_field_id": options.get("inverseLinkFieldId"),
                },
                True,
            )
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
            f"/api/applications/workspace/{self.config.baserow_workspace_id}/",
            {200, 201},
            {
                "name": _sanitize_name(base.get("name", base_id), base_id),
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
            {200, 201},
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

    def get_baserow_views(self, table_id: int) -> List[Dict[str, Any]]:
        payload = self.baserow_management_request("GET", f"/api/database/views/table/{table_id}/", {200})
        if isinstance(payload, list):
            return payload
        return payload.get("results", [])

    def map_view_type(self, airtable_view_type: str) -> Tuple[Optional[str], bool]:
        """Map Airtable view type to Baserow view type.

        Returns:
            Tuple of (baserow_type, is_supported)
            is_supported = True means we'll attempt migration
            is_supported = False means we'll skip and log
        """
        normalized = airtable_view_type.lower()

        if normalized in BASEROW_VIEW_TYPES:
            return BASEROW_VIEW_TYPES[normalized], True
        if normalized not in UNSUPPORTED_AIRTABLE_VIEW_TYPES:
            LOGGER.warning("Encountered unknown Airtable view type '%s'; skipping it.", airtable_view_type)
        return None, False

    def _find_matching_baserow_view(
        self,
        baserow_views: List[Dict[str, Any]],
        view_name: str,
        view_type: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_name = _normalize_view_name(view_name)
        candidates = [
            view for view in baserow_views
            if _normalize_view_name(str(view.get("name", ""))) == normalized_name
            and view.get("type") == view_type
        ]
        if not candidates:
            return None
        if len(candidates) > 1:
            LOGGER.warning(
                "Multiple compatible Baserow views named '%s' (%s) were found; adopting id=%s.",
                view_name,
                view_type,
                candidates[0].get("id"),
            )
        return candidates[0]

    def ensure_baserow_view(
        self,
        airtable_table_id: str,
        airtable_view_id: str,
        airtable_view_name: str,
        airtable_view_type: str,
        baserow_table_id: int,
        baserow_view_type: str,
        baserow_views: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, str, List[Dict[str, Any]]]:
        desired_baserow_view_name = _sanitize_name(airtable_view_name, "View")
        known_view = self.mapping.get_view(airtable_table_id, airtable_view_id)
        live_views = baserow_views or []

        if self.config.dry_run:
            if known_view and known_view.get("baserow_view_id"):
                return int(known_view["baserow_view_id"]), "skipped_existing", live_views
            fake_id = self.create_baserow_view(
                baserow_table_id=baserow_table_id,
                view_name=airtable_view_name,
                view_type=baserow_view_type,
                view_config=None,
            )
            return int(fake_id), "created", live_views

        live_views_by_id = {
            int(view["id"]): view for view in live_views if view.get("id") is not None
        }
        if known_view and known_view.get("baserow_view_id") is not None:
            mapped_view_id = int(known_view["baserow_view_id"])
            live_view = live_views_by_id.get(mapped_view_id)
            if live_view and live_view.get("type") == baserow_view_type:
                self.mapping.set_view(
                    airtable_table_id=airtable_table_id,
                    airtable_view_id=airtable_view_id,
                    airtable_view_name=airtable_view_name,
                    airtable_view_type=airtable_view_type,
                    baserow_view_id=mapped_view_id,
                    baserow_view_name=live_view.get("name", desired_baserow_view_name),
                    baserow_view_type=live_view.get("type", baserow_view_type),
                    is_supported=1,
                    skip_reason=None,
                )
                return mapped_view_id, "skipped_existing", live_views
            LOGGER.warning(
                "Stored Baserow view mapping for Airtable view '%s' on table %s is stale or mismatched; "
                "mapped id=%s resolved to type=%s. Revalidating against live views.",
                airtable_view_name,
                baserow_table_id,
                mapped_view_id,
                live_view.get("type") if live_view else None,
            )

        adopted_view = self._find_matching_baserow_view(live_views, desired_baserow_view_name, baserow_view_type)
        if adopted_view:
            adopted_view_id = int(adopted_view["id"])
            self.mapping.set_view(
                airtable_table_id=airtable_table_id,
                airtable_view_id=airtable_view_id,
                airtable_view_name=airtable_view_name,
                airtable_view_type=airtable_view_type,
                baserow_view_id=adopted_view_id,
                baserow_view_name=adopted_view.get("name", desired_baserow_view_name),
                baserow_view_type=adopted_view.get("type", baserow_view_type),
                is_supported=1,
                skip_reason=None,
            )
            return adopted_view_id, "adopted_existing", live_views

        created_view_id = self.create_baserow_view(
            baserow_table_id=baserow_table_id,
            view_name=airtable_view_name,
            view_type=baserow_view_type,
            view_config=None,
        )
        live_views.append({
            "id": created_view_id,
            "name": desired_baserow_view_name,
            "type": baserow_view_type,
        })
        self.mapping.set_view(
            airtable_table_id=airtable_table_id,
            airtable_view_id=airtable_view_id,
            airtable_view_name=airtable_view_name,
            airtable_view_type=airtable_view_type,
            baserow_view_id=created_view_id,
            baserow_view_name=desired_baserow_view_name,
            baserow_view_type=baserow_view_type,
            is_supported=1,
            skip_reason=None,
        )
        return int(created_view_id), "created", live_views

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

        existing_fields = self.get_baserow_fields(baserow_table_id)
        for ef in existing_fields:
            if ef.get("name") == baserow_field_name:
                field_id = int(ef["id"])
                LOGGER.info(
                    "Field '%s' already exists in Baserow table %s (id=%s), adopting it",
                    baserow_field_name, baserow_table_id, field_id,
                )
                self.mapping.set_field(
                    airtable_table_id,
                    airtable_field["id"],
                    airtable_field.get("name", airtable_field["id"]),
                    field_id,
                    baserow_field_name,
                    baserow_type,
                    extra.get("linked_target_airtable_table_id"),
                )
                return field_id

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
            {200, 201},
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
    ) -> Optional[LinkFieldResult]:
        known = {f["airtable_field_id"]: f for f in self.mapping.get_fields_for_table(airtable_table_id)}
        if self.config.dry_run:
            LOGGER.info(
                "[dry-run] Would create link field '%s' in table %s -> table %s",
                baserow_field_name,
                baserow_table_id,
                target_baserow_table_id,
            )
            fake_id = self._next_fake_id()
            reverse_field_id = self._next_fake_id() if baserow_table_id != target_baserow_table_id else None
            self.mapping.set_field(
                airtable_table_id,
                airtable_field["id"],
                airtable_field.get("name", airtable_field["id"]),
                fake_id,
                baserow_field_name,
                "link_row",
                linked_target_airtable_table_id,
                reverse_field_id,
            )
            return LinkFieldResult(fake_id, True, reverse_field_id)

        existing_fields = self.get_baserow_fields(baserow_table_id)
        existing_fields_by_id = {int(f["id"]): f for f in existing_fields}
        known_field = known.get(airtable_field["id"])
        if (
            known_field
            and known_field["baserow_field_type"] == "link_row"
            and known_field["baserow_field_id"] is not None
        ):
            field_id = known_field["baserow_field_id"]
            actual_field = existing_fields_by_id.get(field_id)
            actual_name = actual_field.get("name") if actual_field else None
            actual_type = actual_field.get("type") if actual_field else None
            actual_target_table_id = _normalize_link_row_table_id(
                actual_field.get("link_row_table_id") if actual_field else None
            )
            if (
                actual_field
                and actual_name == baserow_field_name
                and actual_type == "link_row"
                and actual_target_table_id == target_baserow_table_id
            ):
                reverse_field_id = None
                if baserow_table_id != target_baserow_table_id:
                    reverse_field_id = self._get_reusable_reverse_link_field_id(
                        known_field,
                        target_baserow_table_id,
                        baserow_table_id,
                    )
                self.mapping.set_field(
                    airtable_table_id,
                    airtable_field["id"],
                    airtable_field.get("name", airtable_field["id"]),
                    field_id,
                    baserow_field_name,
                    "link_row",
                    linked_target_airtable_table_id,
                    reverse_field_id,
                )
                return LinkFieldResult(field_id, False, reverse_field_id)

            LOGGER.warning(
                "Stored link mapping for Airtable field '%s' on table %s is stale or mismatched; mapped Baserow field "
                "id=%s resolved to name=%s type=%s target=%s. Revalidating against live schema.",
                airtable_field.get("name", airtable_field["id"]),
                baserow_table_id,
                field_id,
                actual_name,
                actual_type,
                actual_target_table_id,
            )
            self._add_error(
                "create_link_field",
                "Stored Baserow link field mapping rejected after live schema verification",
                {
                    "airtable_table_id": airtable_table_id,
                    "airtable_field_id": airtable_field["id"],
                    "field_name": airtable_field.get("name", airtable_field["id"]),
                    "baserow_table_id": baserow_table_id,
                    "baserow_field_id": field_id,
                    "expected_baserow_field_name": baserow_field_name,
                    "actual_baserow_field_name": actual_name,
                    "expected_baserow_field_type": "link_row",
                    "actual_baserow_field_type": actual_type,
                    "expected_target_baserow_table_id": target_baserow_table_id,
                    "actual_target_baserow_table_id": actual_target_table_id,
                    "linked_target_airtable_table_id": linked_target_airtable_table_id,
                },
            )
            self.mapping.set_field(
                airtable_table_id,
                airtable_field["id"],
                airtable_field.get("name", airtable_field["id"]),
                None,
                baserow_field_name,
                "link_row",
                linked_target_airtable_table_id,
                None,
            )
            known_field = None

        for ef in existing_fields:
            if ef.get("name") == baserow_field_name:
                field_id = int(ef["id"])
                actual_type = ef.get("type")
                actual_target_table_id = _normalize_link_row_table_id(ef.get("link_row_table_id"))
                if actual_type == "link_row" and actual_target_table_id == target_baserow_table_id:
                    LOGGER.info(
                        "Field '%s' already exists in Baserow table %s (id=%s, type=%s), adopting as link field",
                        baserow_field_name, baserow_table_id, field_id, actual_type,
                    )
                    self.mapping.set_field(
                        airtable_table_id,
                        airtable_field["id"],
                        airtable_field.get("name", airtable_field["id"]),
                        field_id,
                        baserow_field_name,
                        "link_row",
                        linked_target_airtable_table_id,
                        None,
                    )
                    reverse_field_id = None
                    if baserow_table_id != target_baserow_table_id:
                        reverse_field_id = self._get_reusable_reverse_link_field_id(
                            known_field,
                            target_baserow_table_id,
                            baserow_table_id,
                        )
                    self.mapping.set_field(
                        airtable_table_id,
                        airtable_field["id"],
                        airtable_field.get("name", airtable_field["id"]),
                        field_id,
                        baserow_field_name,
                        "link_row",
                        linked_target_airtable_table_id,
                        reverse_field_id,
                    )
                    return LinkFieldResult(field_id, False, reverse_field_id)

                LOGGER.warning(
                    "Field '%s' already exists in Baserow table %s (id=%s) but does not match the expected link field "
                    "shape; expected type=link_row target=%s, actual type=%s target=%s. Skipping relation field.",
                    baserow_field_name,
                    baserow_table_id,
                    field_id,
                    target_baserow_table_id,
                    actual_type,
                    actual_target_table_id,
                )
                self.report["totals"]["fields_failed"] += 1
                self.mapping.set_field(
                    airtable_table_id,
                    airtable_field["id"],
                    airtable_field.get("name", airtable_field["id"]),
                    None,
                    baserow_field_name,
                    "link_row",
                    linked_target_airtable_table_id,
                    None,
                )
                self._add_error(
                    "create_link_field",
                    "Existing Baserow field mismatch for relation field",
                    {
                        "airtable_table_id": airtable_table_id,
                        "airtable_field_id": airtable_field["id"],
                        "field_name": airtable_field.get("name", airtable_field["id"]),
                        "baserow_table_id": baserow_table_id,
                        "baserow_field_id": field_id,
                        "baserow_field_name": baserow_field_name,
                        "expected_baserow_field_type": "link_row",
                        "actual_baserow_field_type": actual_type,
                        "expected_target_baserow_table_id": target_baserow_table_id,
                        "actual_target_baserow_table_id": actual_target_table_id,
                        "linked_target_airtable_table_id": linked_target_airtable_table_id,
                    },
                )
                return None

        is_self_ref = baserow_table_id == target_baserow_table_id
        reverse_field_ids_before: set[int] = set()
        if not is_self_ref:
            reverse_field_ids_before = {
                int(f["id"])
                for f in self.get_baserow_fields(target_baserow_table_id)
                if f.get("type") == "link_row"
                and _normalize_link_row_table_id(f.get("link_row_table_id")) == baserow_table_id
            }
        payload = self.baserow_management_request(
            "POST",
            f"/api/database/fields/table/{baserow_table_id}/",
            {200, 201},
            {
                "name": baserow_field_name,
                "type": "link_row",
                "link_row_table": target_baserow_table_id,
                "has_related_field": not is_self_ref,
            },
        )
        field_id = int(payload["id"])
        reverse_field_id = None
        if not is_self_ref:
            reverse_field_id = self._find_reverse_link_field(
                target_baserow_table_id,
                baserow_table_id,
                reverse_field_ids_before,
            )
        self.mapping.set_field(
            airtable_table_id,
            airtable_field["id"],
            airtable_field.get("name", airtable_field["id"]),
            field_id,
            baserow_field_name,
            "link_row",
            linked_target_airtable_table_id,
            reverse_field_id,
        )
        base_id = self.mapping.get_base_id_for_table(airtable_table_id) or airtable_table_id
        table_report = self._table_report(base_id, airtable_table_id)
        table_report["link_fields_created"] += 1
        LOGGER.info("Created link-row field '%s' in table %s", baserow_field_name, baserow_table_id)
        return LinkFieldResult(field_id, True, reverse_field_id)

    def _validate_reverse_link_field_id(
        self,
        reverse_table_id: int,
        expected_target_table_id: int,
        reverse_field_id: Optional[int],
    ) -> Optional[int]:
        if reverse_field_id is None:
            return None
        reverse_fields_by_id = {
            int(field["id"]): field for field in self.get_baserow_fields(reverse_table_id)
        }
        reverse_field = reverse_fields_by_id.get(reverse_field_id)
        reverse_target_table_id = _normalize_link_row_table_id(
            reverse_field.get("link_row_table_id") if reverse_field else None
        )
        if (
            reverse_field
            and reverse_field.get("type") == "link_row"
            and reverse_target_table_id == expected_target_table_id
        ):
            return reverse_field_id
        return None

    def _get_reusable_reverse_link_field_id(
        self,
        known_field: Optional[Dict[str, Any]],
        reverse_table_id: int,
        expected_target_table_id: int,
    ) -> Optional[int]:
        stored_reverse_field_id = known_field.get("reverse_baserow_field_id") if known_field else None
        valid_reverse_field_id = self._validate_reverse_link_field_id(
            reverse_table_id,
            expected_target_table_id,
            stored_reverse_field_id,
        )
        if valid_reverse_field_id is not None:
            return valid_reverse_field_id
        if stored_reverse_field_id is not None:
            LOGGER.warning(
                "Stored reverse link mapping id=%s on table %s no longer matches the expected reverse relation to %s; "
                "falling back to live reverse discovery.",
                stored_reverse_field_id,
                reverse_table_id,
                expected_target_table_id,
            )
        return self._find_reverse_link_field(reverse_table_id, expected_target_table_id)

    def _find_reverse_link_field(
        self, baserow_table_id: int, target_baserow_table_id: int, existing_field_ids: Optional[set[int]] = None
    ) -> Optional[int]:
        """Find the unique reverse link_row field on baserow_table_id that points to target_baserow_table_id."""
        if self.config.dry_run:
            return self._next_fake_id()
        fields = self.get_baserow_fields(baserow_table_id)
        candidate_ids: List[int] = []
        for f in fields:
            if (
                f.get("type") != "link_row"
                or _normalize_link_row_table_id(f.get("link_row_table_id")) != target_baserow_table_id
            ):
                continue
            field_id = int(f["id"])
            if existing_field_ids and field_id in existing_field_ids:
                continue
            candidate_ids.append(field_id)
        if len(candidate_ids) == 1:
            return candidate_ids[0]
        if candidate_ids:
            LOGGER.warning(
                "Could not uniquely identify auto-created reverse link field on table %s pointing to %s; candidates=%s",
                baserow_table_id,
                target_baserow_table_id,
                candidate_ids,
            )
            return None
        LOGGER.warning(
            "Could not find auto-created reverse link field on table %s pointing to %s",
            baserow_table_id, target_baserow_table_id,
        )
        return None
    
    def create_baserow_view(
        self,
        baserow_table_id: int,
        view_name: str,
        view_type: str,
        view_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Create a view in Baserow table."""
        if self.config.dry_run:
            fake_id = self._next_fake_id()
            LOGGER.info("[dry-run] Would create view '%s' (%s) in table %s", view_name, view_type, baserow_table_id)
            return fake_id
        
        body = {
            "name": _sanitize_name(view_name, "View"),
            "type": view_type,
        }
        
        # Add configuration if provided (filters, sorts will be skipped per requirements)
        if view_config:
            body.update(view_config)
        
        payload = self.baserow_management_request(
            "POST",
            f"/api/database/views/table/{baserow_table_id}/",
            {200, 201},
            body,
        )
        view_id = int(payload["id"])
        LOGGER.info("Created view '%s' in Baserow table %s", view_name, baserow_table_id)
        return view_id

    def _ensure_select_options(self, baserow_table_id: int, row_payload: Dict[str, Any]) -> None:
        """Check row payload for select values and add any missing options to Baserow fields."""
        actual_fields = self.get_baserow_fields(baserow_table_id)
        fields_by_name = {f["name"]: f for f in actual_fields}
        for field_name, value in row_payload.items():
            field = fields_by_name.get(field_name)
            if not field:
                continue
            ftype = field.get("type", "")
            if ftype not in ("single_select", "multiple_select"):
                continue
            existing_options = {opt["value"] for opt in field.get("select_options", [])}
            values_to_check = [value] if ftype == "single_select" else (value or [])
            missing = [v for v in values_to_check if v and v not in existing_options]
            if not missing:
                continue
            new_options = list(field.get("select_options", []))
            color_idx = len(new_options)
            for val in missing:
                new_options.append({
                    "value": val,
                    "color": BASEROW_SELECT_COLORS[color_idx % len(BASEROW_SELECT_COLORS)],
                })
                color_idx += 1
            self.baserow_management_request(
                "PATCH",
                f"/api/database/fields/{field['id']}/",
                {200},
                {"select_options": new_options},
            )
            LOGGER.info(
                "Added missing select options %s to field '%s' in table %s",
                missing, field_name, baserow_table_id,
            )

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
                expected_statuses={200, 201},
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
            v = str(value).strip() if value is not None else None
            return v if v else None
        if baserow_type in {"multiple_select"}:
            return [str(v) for v in (value or []) if str(v).strip()]
        if baserow_type == "number":
            return value
        if baserow_type == "boolean":
            return bool(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True)
        return value

    def _create_rows_batch(
        self,
        baserow_table_id: int,
        airtable_table_id: str,
        batch: List[Tuple[str, Dict[str, Any]]],
        table_report: Dict[str, Any],
    ) -> int:
        """Send a batch of rows via the Baserow batch create API.

        Falls back to individual row creation on any failure.
        Returns the number of rows successfully created.
        """
        if not batch:
            return 0
        items = [payload for _, payload in batch]
        try:
            result = self.baserow_row_request(
                "POST",
                f"/api/database/rows/table/{baserow_table_id}/batch/?user_field_names=true",
                {200, 201},
                {"items": items},
            )
            created_rows = result.get("items", [])
            if len(created_rows) != len(batch):
                raise RuntimeError(
                    f"Batch create returned {len(created_rows)} items, expected {len(batch)}"
                )
            for (airtable_record_id, _), row_data in zip(batch, created_rows):
                self.mapping.set_record(airtable_table_id, airtable_record_id, int(row_data["id"]))
            return len(batch)
        except Exception as batch_exc:
            LOGGER.warning(
                "Batch create failed for table %s (%s items), falling back to individual creation: %s",
                baserow_table_id, len(batch), str(batch_exc)[:200],
            )
            return self._create_rows_individually(
                baserow_table_id, airtable_table_id, batch, table_report
            )

    def _create_rows_individually(
        self,
        baserow_table_id: int,
        airtable_table_id: str,
        batch: List[Tuple[str, Dict[str, Any]]],
        table_report: Dict[str, Any],
    ) -> int:
        """Create rows one at a time with select-option retry.

        Used as the fallback when batch creation fails.
        Returns the number of rows successfully created.
        """
        created = 0
        for airtable_record_id, row_payload in batch:
            try:
                try:
                    row = self.baserow_row_request(
                        "POST",
                        f"/api/database/rows/table/{baserow_table_id}/?user_field_names=true",
                        {200, 201},
                        row_payload,
                    )
                except RuntimeError as exc:
                    if "not a valid select option" in str(exc).lower():
                        LOGGER.info("Missing select option detected, adding options and retrying row")
                        self._ensure_select_options(baserow_table_id, row_payload)
                        row = self.baserow_row_request(
                            "POST",
                            f"/api/database/rows/table/{baserow_table_id}/?user_field_names=true",
                            {200, 201},
                            row_payload,
                        )
                    else:
                        raise
                self.mapping.set_record(airtable_table_id, airtable_record_id, int(row["id"]))
                created += 1
            except Exception as exc:
                self.report["totals"]["rows_failed"] += 1
                table_report["rows_failed"] += 1
                self._add_error(
                    "create_row",
                    str(exc)[:300],
                    {"airtable_table_id": airtable_table_id, "airtable_record_id": airtable_record_id},
                )
        return created

    def _patch_links_batch(
        self,
        baserow_table_id: int,
        airtable_table_id: str,
        batch: List[Tuple[int, Dict[str, Any]]],
    ) -> int:
        """Patch link values via the Baserow batch update API.

        Falls back to individual patches on any failure.
        Returns the number of rows successfully patched.
        """
        if not batch:
            return 0
        items = [{"id": row_id, **payload} for row_id, payload in batch]
        try:
            self.baserow_row_request(
                "PATCH",
                f"/api/database/rows/table/{baserow_table_id}/batch/?user_field_names=true",
                {200},
                {"items": items},
            )
            return len(batch)
        except Exception as batch_exc:
            LOGGER.warning(
                "Batch link patch failed for table %s (%s items), falling back to individual patches: %s",
                baserow_table_id, len(batch), str(batch_exc)[:200],
            )
            return self._patch_links_individually(
                baserow_table_id, airtable_table_id, batch
            )

    def _patch_links_individually(
        self,
        baserow_table_id: int,
        airtable_table_id: str,
        batch: List[Tuple[int, Dict[str, Any]]],
    ) -> int:
        """Patch link values one row at a time.

        Used as the fallback when batch patching fails.
        Returns the number of rows successfully patched.
        """
        patched = 0
        for baserow_row_id, patch_payload in batch:
            try:
                self.baserow_row_request(
                    "PATCH",
                    f"/api/database/rows/table/{baserow_table_id}/{baserow_row_id}/?user_field_names=true",
                    {200},
                    patch_payload,
                )
                patched += 1
            except Exception as exc:
                self.report["totals"]["link_patches_failed"] += 1
                self._add_error(
                    "patch_links",
                    str(exc)[:300],
                    {"airtable_table_id": airtable_table_id, "baserow_row_id": baserow_row_id},
                )
        return patched
    
    def migrate_views_for_table(
        self,
        base_id: str,
        table: Dict[str, Any],
        baserow_table_id: int,
    ) -> None:
        """Migrate all views for a single table."""
        airtable_table_id = table["id"]
        table_report = self._table_report(base_id, airtable_table_id)
        airtable_views = self.get_airtable_views(base_id, airtable_table_id)
        total_views = len(airtable_views)
        baserow_views = [] if self.config.dry_run else self.get_baserow_views(baserow_table_id)
        counts = {
            "views_migrated": 0,
            "views_created": 0,
            "views_adopted_existing": 0,
            "views_skipped_existing": 0,
            "views_failed": 0,
            "views_unsupported_skipped": 0,
        }

        LOGGER.info(
            "Processing %s views for table %s",
            total_views, airtable_table_id,
        )

        for view in airtable_views:
            airtable_view_id = view.get("id")
            airtable_view_name = view.get("name", "Unnamed View")
            airtable_view_type = view.get("type", "").lower()
            baserow_type, is_supported = self.map_view_type(airtable_view_type)

            if not is_supported:
                counts["views_unsupported_skipped"] += 1
                self.mapping.set_view(
                    airtable_table_id=airtable_table_id,
                    airtable_view_id=airtable_view_id,
                    airtable_view_name=airtable_view_name,
                    airtable_view_type=airtable_view_type,
                    baserow_view_id=None,
                    baserow_view_name=airtable_view_name,
                    baserow_view_type="unknown",
                    is_supported=0,
                    skip_reason=f"Unsupported view type: {airtable_view_type}",
                )
                LOGGER.info(
                    "Skipping unsupported Airtable view '%s' (%s) on table %s",
                    airtable_view_name,
                    airtable_view_type,
                    airtable_table_id,
                )
                continue

            try:
                _baserow_view_id, outcome, baserow_views = self.ensure_baserow_view(
                    airtable_table_id=airtable_table_id,
                    airtable_view_id=airtable_view_id,
                    airtable_view_name=airtable_view_name,
                    airtable_view_type=airtable_view_type,
                    baserow_table_id=baserow_table_id,
                    baserow_view_type=baserow_type,
                    baserow_views=baserow_views,
                )
                counts["views_migrated"] += 1
                if outcome == "created":
                    counts["views_created"] += 1
                elif outcome == "adopted_existing":
                    counts["views_adopted_existing"] += 1
                else:
                    counts["views_skipped_existing"] += 1
            except Exception as exc:
                counts["views_failed"] += 1
                self.mapping.set_view(
                    airtable_table_id=airtable_table_id,
                    airtable_view_id=airtable_view_id,
                    airtable_view_name=airtable_view_name,
                    airtable_view_type=airtable_view_type,
                    baserow_view_id=None,
                    baserow_view_name=airtable_view_name,
                    baserow_view_type=baserow_type,
                    is_supported=1,
                    skip_reason=f"Migration failed: {str(exc)[:100]}",
                )
                self._add_error(
                    "migrate_view",
                    str(exc),
                    {
                        "airtable_base_id": base_id,
                        "airtable_table_id": airtable_table_id,
                        "airtable_view_id": airtable_view_id,
                        "airtable_view_name": airtable_view_name,
                        "airtable_view_type": airtable_view_type,
                        "baserow_view_type": baserow_type,
                    },
                )

        table_report["views_total"] += total_views
        table_report["views_migrated"] += counts["views_migrated"]
        table_report["views_created"] += counts["views_created"]
        table_report["views_adopted_existing"] += counts["views_adopted_existing"]
        table_report["views_skipped_existing"] += counts["views_skipped_existing"]
        table_report["views_failed"] += counts["views_failed"]
        table_report["views_unsupported_skipped"] += counts["views_unsupported_skipped"]

        self.report["totals"]["views_total"] += total_views
        self.report["totals"]["views_migrated"] += counts["views_migrated"]
        self.report["totals"]["views_created"] += counts["views_created"]
        self.report["totals"]["views_adopted_existing"] += counts["views_adopted_existing"]
        self.report["totals"]["views_skipped_existing"] += counts["views_skipped_existing"]
        self.report["totals"]["views_failed"] += counts["views_failed"]
        self.report["totals"]["views_unsupported_skipped"] += counts["views_unsupported_skipped"]
        self.report["totals"]["views_total_skipped"] += (
            counts["views_unsupported_skipped"] + counts["views_skipped_existing"]
        )

        LOGGER.info(
            "Views for table %s: total=%s, migrated=%s, created=%s, adopted_existing=%s, "
            "skipped_existing=%s, failed=%s, unsupported_skipped=%s",
            airtable_table_id,
            total_views,
            counts["views_migrated"],
            counts["views_created"],
            counts["views_adopted_existing"],
            counts["views_skipped_existing"],
            counts["views_failed"],
            counts["views_unsupported_skipped"],
        )

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
        pending_batch: List[Tuple[str, Dict[str, Any]]] = []
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

            pending_batch.append((airtable_record_id, row_payload))
            if len(pending_batch) >= self.config.batch_size:
                created += self._create_rows_batch(
                    baserow_table_id, airtable_table_id, pending_batch, table_report
                )
                pending_batch = []
                LOGGER.info("Table %s: created %s rows so far", airtable_table_id, created)

        if pending_batch:
            created += self._create_rows_batch(
                baserow_table_id, airtable_table_id, pending_batch, table_report
            )

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
        link_fields = [
            item for item in fields_meta
            if item["baserow_field_type"] == "link_row" and item["baserow_field_id"] is not None
        ]
        if not link_fields:
            return

        # Verify against actual Baserow field types to avoid patching
        # adopted fields that aren't really link_row in Baserow.
        # Also build a map from field ID to actual Baserow field name,
        # since the name in our mapping may differ from what Baserow has.
        actual_link_field_names: Dict[int, str] = {}
        if not self.config.dry_run:
            actual_fields = self.get_baserow_fields(baserow_table_id)
            actual_fields_by_id = {int(f["id"]): f for f in actual_fields}
            valid_link_fields = []
            for item in link_fields:
                mapped_field_id = item["baserow_field_id"]
                actual_field = actual_fields_by_id.get(mapped_field_id)
                expected_target_baserow_table_id = None
                target_airtable_table_id = item["linked_target_airtable_table_id"]
                if target_airtable_table_id:
                    expected_target_baserow_table_id = self.mapping.get_table(target_airtable_table_id)
                actual_target_baserow_table_id = _normalize_link_row_table_id(
                    actual_field.get("link_row_table_id") if actual_field else None
                )
                if (
                    actual_field
                    and actual_field.get("type") == "link_row"
                    and actual_target_baserow_table_id == expected_target_baserow_table_id
                ):
                    actual_link_field_names[mapped_field_id] = actual_field["name"]
                    valid_link_fields.append(item)
                    continue

                self._add_error(
                    "patch_links",
                    "Mapped Baserow link field missing or mismatched",
                    {
                        "airtable_table_id": airtable_table_id,
                        "airtable_field_id": item["airtable_field_id"],
                        "field_name": item["airtable_field_name"],
                        "baserow_table_id": baserow_table_id,
                        "baserow_field_id": mapped_field_id,
                        "mapped_baserow_field_name": item["baserow_field_name"],
                        "linked_target_airtable_table_id": target_airtable_table_id,
                        "expected_target_baserow_table_id": expected_target_baserow_table_id,
                        "actual_baserow_field_name": actual_field.get("name") if actual_field else None,
                        "actual_baserow_field_type": actual_field.get("type") if actual_field else None,
                        "actual_target_baserow_table_id": actual_target_baserow_table_id,
                    },
                )
            link_fields = valid_link_fields
        if not link_fields:
            return

        link_fields_by_name = {item["airtable_field_name"]: item for item in link_fields}
        patched = 0
        pending_batch: List[Tuple[int, Dict[str, Any]]] = []

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
                real_name = actual_link_field_names.get(mapped["baserow_field_id"], mapped["baserow_field_name"])
                patch_payload[real_name] = target_row_ids

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

            pending_batch.append((baserow_row_id, patch_payload))
            if len(pending_batch) >= self.config.batch_size:
                patched += self._patch_links_batch(
                    baserow_table_id, airtable_table_id, pending_batch
                )
                pending_batch = []
                LOGGER.info("Table %s: patched %s rows with link values so far", airtable_table_id, patched)

        if pending_batch:
            patched += self._patch_links_batch(
                baserow_table_id, airtable_table_id, pending_batch
            )
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
        deferred_links: List[Tuple[str, int, Dict[str, Any], str, str, Optional[str]]] = []

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
                known_fields_by_id = {
                    item["airtable_field_id"]: item
                    for item in self.mapping.get_fields_for_table(table["id"])
                }
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
                        known_fields_by_id[field["id"]] = {
                            "airtable_field_id": field["id"],
                            "airtable_field_name": field.get("name", field["id"]),
                            "baserow_field_id": primary_baserow_id,
                            "baserow_field_name": baserow_field_name,
                            "baserow_field_type": "text",
                            "linked_target_airtable_table_id": None,
                            "reverse_baserow_field_id": None,
                        }
                        continue
                    if defer:
                        linked_target = extra.get("linked_target_airtable_table_id")
                        if not linked_target:
                            LOGGER.warning("Skipping link field %s: missing linked table metadata", field.get("name"))
                            self.report["totals"]["fields_failed"] += 1
                            table_report = self._table_report(
                                base_id,
                                table["id"],
                                table.get("name", table["id"]),
                            )
                            table_report["errors"].append("Missing linked table metadata for a link field.")
                            self._add_error(
                                "create_link_field",
                                "Missing linked table metadata for relation field",
                                {
                                    "airtable_base_id": base_id,
                                    "airtable_table_id": table["id"],
                                    "airtable_field_id": field["id"],
                                    "field_name": field.get("name", field["id"]),
                                },
                            )
                            continue
                        deferred_links.append(
                            (
                                table["id"],
                                baserow_table_id,
                                field,
                                baserow_field_name,
                                linked_target,
                                extra.get("inverse_link_airtable_field_id"),
                            )
                        )
                        known_field = known_fields_by_id.get(field["id"], {})
                        self.mapping.set_field(
                            table["id"],
                            field["id"],
                            field.get("name", field["id"]),
                            known_field.get("baserow_field_id"),
                            baserow_field_name,
                            "link_row",
                            linked_target,
                            known_field.get("reverse_baserow_field_id"),
                        )
                        known_fields_by_id[field["id"]] = {
                            "airtable_field_id": field["id"],
                            "airtable_field_name": field.get("name", field["id"]),
                            "baserow_field_id": known_field.get("baserow_field_id"),
                            "baserow_field_name": baserow_field_name,
                            "baserow_field_type": "link_row",
                            "linked_target_airtable_table_id": linked_target,
                            "reverse_baserow_field_id": known_field.get("reverse_baserow_field_id"),
                        }
                        continue
                    try:
                        self.create_field_if_needed(
                            table["id"],
                            baserow_table_id,
                            field,
                            baserow_field_name,
                            baserow_type,
                            extra,
                        )
                    except Exception as exc:
                        self.report["totals"]["fields_failed"] += 1
                        self._add_error(
                            "create_field",
                            str(exc),
                            {"table_id": table["id"], "field_name": field.get("name"), "field_type": baserow_type},
                        )

        # Second pass for relation fields.
        # Only adopt auto-created reverse fields when Airtable gives us an
        # explicit inverse field id to claim against. If inverse metadata is
        # missing, report the reverse field as unclaimed instead of guessing.
        pending_reverse_link_claims: Dict[str, PendingReverseLinkClaim] = {}
        pending_ambiguous_reverse_link_claims: Dict[str, Dict[str, Any]] = {}
        pending_unclaimed_reverse_fields: Dict[tuple[int, int], List[int]] = {}
        deferred_link_context_by_field_id = {
            deferred_field["id"]: {
                "airtable_table_id": deferred_airtable_table_id,
                "field_name": deferred_field.get("name", deferred_field["id"]),
                "baserow_table_id": deferred_baserow_table_id,
                "linked_target_airtable_table_id": deferred_linked_target,
            }
            for (
                deferred_airtable_table_id,
                deferred_baserow_table_id,
                deferred_field,
                _deferred_baserow_field_name,
                deferred_linked_target,
                _deferred_inverse_link_airtable_field_id,
            ) in deferred_links
        }

        for (
            airtable_table_id,
            baserow_table_id,
            field,
            baserow_field_name,
            linked_target,
            inverse_link_airtable_field_id,
        ) in deferred_links:
            try:
                target_baserow_table_id = self.mapping.get_table(linked_target)
                if not target_baserow_table_id:
                    self.report["totals"]["fields_failed"] += 1
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

                if field["id"] in pending_ambiguous_reverse_link_claims:
                    ambiguity = pending_ambiguous_reverse_link_claims.pop(field["id"])
                    self.mapping.set_field(
                        airtable_table_id,
                        field["id"],
                        field.get("name", field["id"]),
                        None,
                        baserow_field_name,
                        "link_row",
                        linked_target,
                        None,
                    )
                    self._add_error(
                        "create_link_field",
                        "Ambiguous reverse link claim; refusing to adopt auto-created reverse field",
                        {
                            "airtable_table_id": airtable_table_id,
                            "airtable_field_id": field["id"],
                            "field_name": field.get("name", field["id"]),
                            "baserow_table_id": baserow_table_id,
                            "linked_target_airtable_table_id": linked_target,
                            "ambiguity": ambiguity,
                        },
                    )
                    continue

                if field["id"] in pending_reverse_link_claims:
                    claim = pending_reverse_link_claims.pop(field["id"])
                    self.mapping.set_field(
                        airtable_table_id,
                        field["id"],
                        field.get("name", field["id"]),
                        claim.reverse_field_id,
                        baserow_field_name,
                        "link_row",
                        linked_target,
                        claim.source_baserow_field_id,
                    )
                    LOGGER.info(
                        "Mapped reverse link field '%s' (id=%s) in table %s (auto-created by Baserow)",
                        baserow_field_name, claim.reverse_field_id, baserow_table_id,
                    )
                    continue

                if not inverse_link_airtable_field_id:
                    reverse_pair = (baserow_table_id, target_baserow_table_id)
                    unclaimed_reverse_field_ids = pending_unclaimed_reverse_fields.get(reverse_pair, [])
                    if unclaimed_reverse_field_ids:
                        self.report["totals"]["fields_failed"] += 1
                        self.mapping.set_field(
                            airtable_table_id,
                            field["id"],
                            field.get("name", field["id"]),
                            None,
                            baserow_field_name,
                            "link_row",
                            linked_target,
                            None,
                        )
                        self._add_error(
                            "create_link_field",
                            "Unclaimed auto-created reverse link field exists; refusing to guess reverse mapping",
                            {
                                "airtable_table_id": airtable_table_id,
                                "airtable_field_id": field["id"],
                                "field_name": field.get("name", field["id"]),
                                "baserow_table_id": baserow_table_id,
                                "target_baserow_table_id": target_baserow_table_id,
                                "unclaimed_reverse_field_ids": unclaimed_reverse_field_ids[:],
                            },
                        )
                        continue

                source_base_id = self.mapping.get_base_id_for_table(airtable_table_id)
                target_base_id = self.mapping.get_base_id_for_table(linked_target)
                source_db = self.mapping.get_base(source_base_id) if source_base_id else None
                target_db = self.mapping.get_base(target_base_id) if target_base_id else None
                if source_db and target_db and source_db != target_db:
                    self.report["totals"]["fields_failed"] += 1
                    LOGGER.warning(
                        "Skipping cross-database link field '%s' on table %s: "
                        "source db %s != target db %s (Baserow requires same database)",
                        field.get("name"), airtable_table_id, source_db, target_db,
                    )
                    self._add_error(
                        "create_link_field",
                        "Cross-database link not supported in Baserow",
                        {
                            "airtable_table_id": airtable_table_id,
                            "linked_target_airtable_table_id": linked_target,
                            "field_name": field.get("name"),
                            "source_baserow_db": source_db,
                            "target_baserow_db": target_db,
                        },
                    )
                    self.mapping.set_field(
                        airtable_table_id, field["id"],
                        field.get("name", field["id"]),
                        None, baserow_field_name, "link_row", linked_target,
                        None,
                    )
                    continue

                result = self.create_link_field_if_needed(
                    airtable_table_id,
                    baserow_table_id,
                    field,
                    baserow_field_name,
                    target_baserow_table_id,
                    linked_target,
                )
                if not result:
                    continue
                if baserow_table_id != target_baserow_table_id:
                    reverse_claim_key = (target_baserow_table_id, baserow_table_id)
                    if result.reverse_field_id is None:
                        self.report["totals"]["fields_failed"] += 1
                        self._add_error(
                            "create_link_field",
                            "Could not uniquely resolve auto-created reverse link field",
                            {
                                "airtable_table_id": airtable_table_id,
                                "airtable_field_id": field["id"],
                                "field_name": field.get("name", field["id"]),
                                "baserow_table_id": baserow_table_id,
                                "target_baserow_table_id": target_baserow_table_id,
                                "inverse_link_airtable_field_id": inverse_link_airtable_field_id,
                            },
                        )
                    elif inverse_link_airtable_field_id:
                        new_claim = PendingReverseLinkClaim(
                            source_airtable_table_id=airtable_table_id,
                            source_airtable_field_id=field["id"],
                            source_field_name=field.get("name", field["id"]),
                            source_baserow_table_id=baserow_table_id,
                            source_baserow_field_id=result.field_id,
                            reverse_field_id=result.reverse_field_id,
                        )
                        if inverse_link_airtable_field_id in pending_ambiguous_reverse_link_claims:
                            self.report["totals"]["fields_failed"] += 1
                            self._add_error(
                                "create_link_field",
                                "Additional reverse link claim collided with an already ambiguous inverse field claim",
                                {
                                    "airtable_table_id": airtable_table_id,
                                    "airtable_field_id": field["id"],
                                    "field_name": field.get("name", field["id"]),
                                    "baserow_table_id": baserow_table_id,
                                    "inverse_link_airtable_field_id": inverse_link_airtable_field_id,
                                    "reverse_field_id": result.reverse_field_id,
                                    "existing_ambiguity": pending_ambiguous_reverse_link_claims[inverse_link_airtable_field_id],
                                },
                            )
                        else:
                            existing_claim = pending_reverse_link_claims.get(inverse_link_airtable_field_id)
                            if existing_claim is None:
                                pending_reverse_link_claims[inverse_link_airtable_field_id] = new_claim
                            elif (
                                existing_claim.source_airtable_field_id == new_claim.source_airtable_field_id
                                and existing_claim.reverse_field_id == new_claim.reverse_field_id
                            ):
                                LOGGER.info(
                                    "Ignoring duplicate reverse link claim for Airtable field '%s' on inverse field %s",
                                    new_claim.source_field_name,
                                    inverse_link_airtable_field_id,
                                )
                            else:
                                pending_reverse_link_claims.pop(inverse_link_airtable_field_id, None)
                                ambiguity = {
                                    "inverse_link_airtable_field_id": inverse_link_airtable_field_id,
                                    "existing_claim": {
                                        "airtable_table_id": existing_claim.source_airtable_table_id,
                                        "airtable_field_id": existing_claim.source_airtable_field_id,
                                        "field_name": existing_claim.source_field_name,
                                        "baserow_table_id": existing_claim.source_baserow_table_id,
                                        "reverse_field_id": existing_claim.reverse_field_id,
                                    },
                                    "conflicting_claim": {
                                        "airtable_table_id": new_claim.source_airtable_table_id,
                                        "airtable_field_id": new_claim.source_airtable_field_id,
                                        "field_name": new_claim.source_field_name,
                                        "baserow_table_id": new_claim.source_baserow_table_id,
                                        "reverse_field_id": new_claim.reverse_field_id,
                                    },
                                }
                                pending_ambiguous_reverse_link_claims[inverse_link_airtable_field_id] = ambiguity
                                self.report["totals"]["fields_failed"] += 1
                                self._add_error(
                                    "create_link_field",
                                    "Ambiguous reverse link claim detected; refusing to overwrite an existing inverse field claim",
                                    ambiguity,
                                )
                    else:
                        pending_unclaimed_reverse_fields.setdefault(reverse_claim_key, []).append(result.reverse_field_id)
                        self._add_error(
                            "create_link_field",
                            "Auto-created reverse link field left unclaimed because Airtable inverse metadata is missing",
                            {
                                "airtable_table_id": airtable_table_id,
                                "airtable_field_id": field["id"],
                                "field_name": field.get("name", field["id"]),
                                "baserow_table_id": baserow_table_id,
                                "target_baserow_table_id": target_baserow_table_id,
                                "reverse_field_id": result.reverse_field_id,
                            },
                        )
            except Exception as exc:
                self.report["totals"]["fields_failed"] += 1
                self._add_error(
                    "create_link_field",
                    str(exc),
                    {"table_id": airtable_table_id, "field_name": field.get("name"), "linked_target": linked_target},
                )

        for claimed_airtable_field_id, claim in pending_reverse_link_claims.items():
            self.report["totals"]["fields_failed"] += 1
            claim_context = deferred_link_context_by_field_id.get(claimed_airtable_field_id, {})
            self._add_error(
                "create_link_field",
                "Unconsumed reverse link claim",
                {
                    "airtable_field_id": claimed_airtable_field_id,
                    "field_name": claim_context.get("field_name"),
                    "airtable_table_id": claim_context.get("airtable_table_id"),
                    "baserow_table_id": claim_context.get("baserow_table_id"),
                    "linked_target_airtable_table_id": claim_context.get("linked_target_airtable_table_id"),
                    "reverse_field_id": claim.reverse_field_id,
                    "claim_origin_airtable_table_id": claim.source_airtable_table_id,
                    "claim_origin_airtable_field_id": claim.source_airtable_field_id,
                    "claim_origin_field_name": claim.source_field_name,
                    "claim_origin_baserow_table_id": claim.source_baserow_table_id,
                },
            )

        for claimed_airtable_field_id, ambiguity in pending_ambiguous_reverse_link_claims.items():
            claim_context = deferred_link_context_by_field_id.get(claimed_airtable_field_id, {})
            self._add_error(
                "create_link_field",
                "Unconsumed ambiguous reverse link claim",
                {
                    "airtable_field_id": claimed_airtable_field_id,
                    "field_name": claim_context.get("field_name"),
                    "airtable_table_id": claim_context.get("airtable_table_id"),
                    "baserow_table_id": claim_context.get("baserow_table_id"),
                    "linked_target_airtable_table_id": claim_context.get("linked_target_airtable_table_id"),
                    "ambiguity": ambiguity,
                },
            )

        return plan

    def migrate_data(self, plan: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]]) -> None:
        for base, tables in plan:
            base_id = base["id"]
            LOGGER.info("Migrating rows for base %s (%s)", base.get("name", base_id), base_id)
            for table in tables:
                try:
                    self.migrate_rows_phase_a(base_id, table)
                except Exception as exc:
                    self._add_error(
                        "migrate_rows_phase_a",
                        str(exc)[:300],
                        {"base_id": base_id, "table_id": table.get("id")},
                    )
            for table in tables:
                try:
                    self.migrate_links_phase_b(base_id, table)
                except Exception as exc:
                    self._add_error(
                        "migrate_links_phase_b",
                        str(exc)[:300],
                        {"base_id": base_id, "table_id": table.get("id")},
                    )

    def migrate_views(self, plan: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]]) -> None:
        for base, tables in plan:
            base_id = base["id"]
            LOGGER.info("Migrating views for base %s (%s)", base.get("name", base_id), base_id)
            for table in tables:
                airtable_table_id = table.get("id")
                if not airtable_table_id:
                    continue
                try:
                    baserow_table_id = self.mapping.get_table(airtable_table_id)
                    if not baserow_table_id:
                        self._add_error(
                            "migrate_views",
                            "Missing Baserow table mapping for view migration",
                            {"base_id": base_id, "table_id": airtable_table_id},
                        )
                        continue
                    self.migrate_views_for_table(base_id, table, baserow_table_id)
                except Exception as exc:
                    self._add_error(
                        "migrate_views",
                        str(exc)[:300],
                        {"base_id": base_id, "table_id": airtable_table_id},
                    )

    def run(self) -> None:
        try:
            plan = self.create_schema()
            self.migrate_data(plan)
            self.migrate_views(plan)
            totals = self.report["totals"]
            error_count = len(self.report["errors"])
            if error_count:
                LOGGER.warning(
                    "Migration completed with %s error(s). "
                    "rows_failed=%s, fields_failed=%s, link_patches_failed=%s, views_failed=%s. "
                    "See migration report for details.",
                    error_count,
                    totals["rows_failed"],
                    totals["fields_failed"],
                    totals["link_patches_failed"],
                    totals["views_failed"],
                )
            else:
                LOGGER.info("Migration completed successfully with zero errors.")
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
