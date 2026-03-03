## Airtable -> Baserow Migration Script

This project migrates an Airtable workspace (all bases/tables/rows) into a self-hosted Baserow instance.

Key features:
- Airtable metadata-driven schema cloning.
- Two-phase relation migration for linked records.
- Attachment transfer (`multipleAttachments`) via local download + Baserow User Files upload.
- SQLite-backed ID mappings for resumable runs and lower memory pressure.
- Environment-variable based auth and configuration (`.env`).
- JSON migration report output (counts, per-table stats, and errors).

## Prerequisites

- Python 3.10+ available on PATH.
- `uv` installed:

```powershell
python -m pip install --user uv
```

## Setup (uv/venv)

From the project directory:

```powershell
python -m uv sync
```

This creates/updates `.venv` and installs dependencies pinned by `uv.lock`.

1. Copy `.env.example` to `.env` and fill values.
2. Confirm `REPORT_PATH` in `.env` if you want a custom report location.

## Run

```powershell
python -m uv run python .\main.py --log-level INFO
```

Dry run (no writes to Baserow):

```powershell
python -m uv run python .\main.py --dry-run --log-level DEBUG
```

Specify report output path at runtime:

```powershell
python -m uv run python .\main.py --report-path .\migration_report.json
```

## Report Output

- Default path: `migration_report.json` (or `.env` `REPORT_PATH`).
- Includes:
  - Base/table discovery and processing counts.
  - Rows created/skipped and link patch counts.
  - Attachment download/cache/upload/failure counters.
  - Timestamped error list with context.

## Auth Notes

- `BASEROW_MANAGEMENT_TOKEN` + `BASEROW_MANAGEMENT_TOKEN_TYPE` is required for:
  - Creating applications/tables/fields.
  - Uploading files via `/api/user-files/upload-file/`.
- `BASEROW_DATABASE_TOKEN` is optional and only used for row writes.
  - If omitted, row API calls also use the management token.

