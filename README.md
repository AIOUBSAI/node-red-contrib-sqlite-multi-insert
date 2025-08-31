# node-red-contrib-sqlite-multi-insert

**Multi-table** bulk INSERT/UPSERT for SQLite in Node-RED, with per-table sources, mapping, conflict strategies, optional returned rows, chunking, and transaction control.

> Successor to single-table insert nodes; this one lets you define **multiple tables** (“Groups”) in one node, each with its own data source and mapping.

---

## Features

- **Multi-table**: Insert into several tables in one shot (per “Group”).
- **Typed sources per table**: `msg` / `flow` / `global` / `env` / `str` / `num` / `bool` / `json` / `jsonata`.
- **Auto-map or manual mapping** to columns.
- **Transforms**: `none`, `trim`, `upper`, `lower`, `nz` (empty/NA→null), `bool01`, `number`, `string`.
- **Conflict strategies**: `none`, `OR IGNORE`, `OR REPLACE`, `UPSERT (ON CONFLICT)` with keys & update columns.
- **Optional returning**: Return inserted/affected rows (with IDs) to `msg`/`flow`/`global`.
- **Transaction control**: `all` | `perTable` | `off` | `chunk` (+ chunk size & continue-on-error).
- **PRAGMAs**: WAL + synchronous + extra PRAGMAs for performance tuning.
- **Safety**: Basic identifier sanitization.

---

## Install

```bash
# in your Node-RED userDir (usually ~/.node-red)
npm i node-red-contrib-sqlite-multi-insert
````

Requires `sqlite3` (installed automatically).

---

## Node: “sqlite multi insert”

### Connection

* **Database path** (typed): where the `.sqlite/.db` file lives.
* **PRAGMAs** (optional)

  * **WAL**: enable `PRAGMA journal_mode=WAL;`
  * **synchronous**: `OFF | NORMAL | FULL | EXTRA`
  * **Extra PRAGMAs**: semicolon-separated (e.g. `temp_store=MEMORY; cache_size=20000`)

### Transaction

* **Mode**:

  * `all` — wrap *all groups* in a single transaction.
  * `perTable` — each table in its own transaction.
  * `off` — no explicit transaction.
  * `chunk` — per-row batches (with **Chunk size**).
* **Continue on error** — best-effort inserts instead of failing fast.

### Groups (one per target table)

Each group defines a per-table insert job.

* **Table**: target table name.
* **Source** (typed): where to read rows from (usually an array of objects).

  * Typical: `msg.payload`, `flow.myArray`, `global.someData`, `jsonata` expression, etc.
* **Auto-map**:

  * **On**: copy object keys → same-named columns.
  * **Off**: define a **Mapping** grid: for each column pick a source (typed) and optional transform.
* **Conflict**:

  * `none` | `ignore` (OR IGNORE) | `replace` (OR REPLACE) | `upsert`
  * For **upsert** set **keys** (conflict columns) & **update columns**.
* **Return rows** (optional):

  * Mode: `none` | `inserted` | `affected`
  * **ID column**: defaults to `id`
  * **Return path** (typed): where to store the returned rows, e.g. `msg.sqlite.MyTable.rows`

> The node attempts to use `RETURNING *` if your SQLite build supports it; otherwise it falls back to `lastID` where possible.

---

## Input & Output

### Input

Supply the data arrays at the configured **Source** paths. Each group reads its own array and maps it to the target columns.

### Output

* **`msg.sqlite`** (always): operation summary

  ```json
  {
    "ok": true,
    "counts": { "inserted": 42, "updated": 0, "errors": 0, "skipped": 0 },
    "tables": {
      "table": { "inserted": 42, "updated": 0, "errors": 0, "skipped": 0, "total": 42 }
    },
    "timings": { "msOpen": 2, "msExec": 18, "msTotal": 22 }
  }
  ```
* **Returned rows per group** (optional): written to the group’s **Return path** (typed).

  * Example row shape:

    ```json
    { "action":"inserted", "id": 123, "data": { "name":"C12" } }
    ```
* Optionally mirror the summary to `msg.payload` (editor toggle).

## Mapping Transforms

* `none` — leave value as-is
* `trim` — string trim
* `upper` / `lower`
* `nz` — empty / `NA` / `N/A` → `null`
* `bool01` — truey → `1`, else `0`
* `number` — to Number (empty → null)
* `string` — to String

---

## Limitations

* `UPSERT` and `RETURNING` require modern SQLite builds; the node falls back where possible.
* Column and table names are sanitized (alphanumeric + underscore).


## License

MIT © AIOUBSAI
