## Airtable -> Baserow Migration Script

This project migrates an Airtable workspace (all bases/tables/rows) into a self-hosted Baserow instance.

Key features:
- Airtable metadata-driven schema cloning.
- Two-phase relation migration for linked records.
- Attachment transfer (`multipleAttachments`) via local download + Baserow User Files upload.
- SQLite-backed ID mappings for resumable runs and lower memory pressure.
- Environment-variable based auth and configuration (`.env`).
- JSON migration report output with totals, per-table field mappings, and errors.
- Field-fidelity handling that distinguishes direct mappings, snapshots, and auxiliary preservation fields.

## Prerequisites

1. Python 3.10+ available on PATH.

2. `uv` installed:

  ```
  python -m pip install --user uv
  ```

3. Docker installed and available on PATH: https://docs.docker.com/engine/install/ubuntu/ (as per your platform)

## Launching Baserow

- Start the Docker container with a detached storage volume & default ports:
  ```
  sudo docker run --name baserow -v baserow_data:/baserow/data -p 80:80 -p 443:443 baserow/baserow:latest
  ``` 

## Setup (uv/venv)

- From the project directory:
  ```
  python -m uv sync
  ```

- This creates/updates `.venv` and installs dependencies pinned by `uv.lock`.

- Copy `.env.example` to `.env` and fill values.

- Confirm `REPORT_PATH` in `.env` if you want a custom report location.

## Setup (.env variables & auth tokens)

1. Airtable Personal Access Token (PAT):

  - Login to Airtable, then navigate to:

  ```text
  My Apps -> User Profile (top-right) -> Builder Hub -> Personal Access Tokens -> + Create Token
  ```

  - Create a PAT with the following scopes:

  ```text
  data.records:read
  data.recordComments:read
  schema.bases:read
  user.email:read
  ```

  - Store PAT securely (will only be displayed once!)

2. Airtable Workspace/Base IDs

  - `AIRTABLE_WORKSPACE_ID` is still required by the script, but Airtable's bases API does not always return `workspaceId` metadata. In practice, `AIRTABLE_BASE_IDS` is the reliable filter when the API omits workspace IDs.

  - To inspect the bases visible to the PAT:
  ```powershell
  curl -s -H "Authorization: Bearer <PAT>" https://api.airtable.com/v0/meta/bases | python -m json.tool
  ```

3. Baserow auth

  - `BASEROW_WORKSPACE_ID` can be obtained from the URL in the browser's address bar: `http://localhost/workspace/33` (workspace ID is 33 here)

  - Obtain `BASEROW_DATABASE_TOKEN` - Navigate to Baserow in the browser:
  ```text
   http://localhost -> Workspace Name Icon (top-left) -> My Settings -> Database Tokens
  ```

  - Create a token with all permissions.

  - For `BASEROW_MANAGEMENT_TOKEN` and `BASEROW_REFRESH_TOKEN`:

    - In a terminal, create a temporary credential file (do NOT escape `\` any special chars!):
    ```
    cat << 'EOF' > /tmp/auth.json
    {"email": "username@account.com", "password": "pswd"}
    EOF
    ```

    - Curl the tokens:
    ```
    curl -X POST http://localhost/api/user/token-auth/ -H "Content-Type: application/json" -d @/tmp/auth.json
    ```

    - The response will contain the tokens:
    ```
    {"token":...,"access_token":...(same as token!)...,"refresh_token":...,---}
    ```

    - `BASEROW_MANAGEMENT_TOKEN_TYPE` should be `JWT` for the above.
  
  - Set `MAX_TOKEN_REFRESHES` to a value apt for the scale of the migration. 

4. Typical runtime settings

  - `SQLITE_PATH=id_mapping.db`
  - `ATTACHMENTS_DIR=attachments_cache`
  - `REPORT_PATH=migration_report.json`
  - `BATCH_SIZE=50`
  - `REQUEST_TIMEOUT_SECONDS=60`
  - `DRY_RUN=false`

## Field Handling

The migrator classifies each Airtable field into one of these fidelity modes:

- `direct`: the Airtable field maps cleanly to a Baserow field type and is migrated without intentional degradation.
- `snapshot`: the current Airtable value is preserved, but Airtable-specific behavior is not recreated.
- `auxiliary`: the main Baserow field stores a readable projection and one or more companion fields preserve typed or raw source structure.

Current implemented examples:

- Text-like fields such as `singleLineText`, `email`, `url`, and `phoneNumber` map directly to Baserow `text`.
- `multilineText` maps directly to `long_text`.
- `richText`, formulas, and scalar rollups are preserved as typed or readable snapshots.
- Lookup fields with text-like results are preserved as readable `long_text` snapshots.
- Lookup fields with non-text, mixed, or otherwise ambiguous results are migrated as readable text plus a `(raw)` JSON companion field.
- Rollup fields with mixed or structured results are migrated as readable text plus a `(raw)` JSON companion field.
- Collaborator-style fields such as `singleCollaborator`, `multipleCollaborators`, `createdBy`, and `lastModifiedBy` create a readable projection plus a raw JSON companion field.

### Auxiliary Field Suffixes

- `(raw)` stores raw JSON for structured Airtable values or multi-value computed outputs that would otherwise lose machine-readable shape.
- `(typed)` is used when a non-text Airtable primary field needs a companion field because Baserow requires the first field in a table to be text.

### Primary Field Constraint

Baserow requires the first field in every table to be text-like. When an Airtable primary field is not text-like, the migrator keeps the first Baserow field as a readable text projection and creates a companion field to preserve the typed value more faithfully.

## Reruns And Resumes

- Real runs persist ID mappings in `SQLITE_PATH` so interrupted migrations can resume.
- Dry runs use an in-memory mapping store and do not pollute the on-disk SQLite mapping database.
- Stored base and table mappings are live-validated against the current Baserow workspace and database before reuse. If a mapped database or table no longer matches the expected live object, the stale mapping is rejected and the migrator re-adopts by live name or recreates as needed.
- Stored ordinary field mappings are live-validated against the current Baserow schema before reuse. If the mapped field no longer matches the expected name or compatible type, the stale mapping is rejected and the migrator revalidates against the live table schema.
- Phase A row migration now reserves a per-table internal text field that stores the Airtable record ID in Baserow. Reruns use that marker to re-adopt rows whose earlier create succeeded in Baserow even if the local SQLite mapping missed the response.
- Mutating Baserow writes are no longer retried blindly on ambiguous failures. Batch row creates first reconcile against the live Baserow row-identity marker and only replay still-unmapped rows individually; batch link patches still fail closed and are reported instead of being replayed automatically.
- Claimed reverse link fields now persist the live Baserow field identity and live field name, so reruns can safely reuse Baserow auto-created inverse relations without invalidating them just because the Airtable-derived desired field name differs.
- Link patching fails closed when a linked Airtable target row is missing a Baserow row mapping. In that case the row is not patched with a partial relation set, and the missing dependency is reported as an error instead.

## Known Limits

- Cross-database Airtable links do not have direct Baserow parity and are still reported as migration errors instead of being silently flattened.
- Airtable computed behavior is not recreated. Formula, lookup, rollup, and similar fields migrate as current-value snapshots only.
- Airtable-specific formatting and UI semantics are not fully preserved for fields such as currency, percent, rating, duration, and collaborator objects.

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
  - Field fidelity totals and per-table `field_mappings` entries.
  - Rows created/skipped and link patch counts.
  - Attachment download/cache/upload/failure counters.
  - Timestamped error list with context.

The `field_mappings` section records the Airtable source type, the selected Baserow output fields, the transform policy, the fidelity class, the resolution status, and any reported degradation or failure reason for each migrated field.

