/**
 * node-red-contrib-sqlite-multi-insert
 * Multi-table bulk INSERT / UPSERT for SQLite with per-table sources & mappings.
 * - No preSQL/postSQL (as requested).
 * - Per-table config: source (typed), mapping/autoMap, conflict strategy, optional RETURN rows.
 * - One connection; transaction mode: all | perTable | off; chunking supported.
 */

module.exports = function (RED) {
  const sqlite3 = require("sqlite3");

  // ---------- small utils ----------
  const isObj = (v) => v && typeof v === "object" && !Array.isArray(v);
  const isArr = Array.isArray;

  const sanitizeIdent = (name) => {
    const s = String(name || "").trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(s)) throw new Error(`Invalid identifier: ${name}`);
    return s;
  };

  function coerce(v) { return v == null ? null : v; }

  function transformValue(v, tx) {
    if (v == null) return null;
    switch (tx) {
      case "trim":   return String(v).trim();
      case "upper":  return String(v).toUpperCase();
      case "lower":  return String(v).toLowerCase();
      case "nz": { // empty / NA / N/A -> null
        const s = String(v).trim();
        if (!s || /^N\/?A$/i.test(s)) return null;
        return s;
      }
      case "bool01": return (v === true || v === 1 || v === "1" || /^true$/i.test(String(v))) ? 1 : 0;
      case "number": return (String(v).trim() === "") ? null : Number(v);
      case "string": return String(v);
      case "none":
      default:       return v;
    }
  }

  function getTyped(RED, node, msg, type, value) {
    switch (type) {
      case "msg":    return RED.util.getMessageProperty(msg, String(value));
      case "flow":   return node.context().flow.get(String(value));
      case "global": return node.context().global.get(String(value));
      case "env":    return process.env[String(value)];
      case "json":   try { return JSON.parse(String(value)); } catch { return undefined; }
      case "jsonata": {
        try {
          const expr = RED.util.prepareJSONataExpression(String(value), node);
          return RED.util.evaluateJSONataExpression(expr, msg);
        } catch { return undefined; }
      }
      case "str":
      default:
        return value;
    }
  }

  function toRows(arrOrObj) {
    if (arrOrObj == null) return [];
    if (isArr(arrOrObj)) return arrOrObj;
    if (isObj(arrOrObj)) return [arrOrObj];
    return [];
  }

  function buildMappingRow(row, mapping, autoMap) {
    if (autoMap) {
      // Copy all keys as-is
      const out = {};
      Object.keys(row || {}).forEach(k => { out[k] = row[k]; });
      return out;
    }
    const out = {};
    for (const m of mapping || []) {
      const srcVal = m.srcType === "path"
        ? RED.util.getObjectProperty(row, String(m.src))
        : m.srcType === "jsonata"
          ? (function () {
              try {
                const expr = RED.util.prepareJSONataExpression(String(m.src), /* node not available here */ null);
                // jsonata without FULL context – only row scope:
                return RED.util.evaluateJSONataExpression(expr, row);
              } catch { return undefined; }
            })()
          : m.srcType === "str" || m.srcType === "num" || m.srcType === "bool" || m.srcType === "json"
            ? (m.srcType === "json" ? (function(){try{return JSON.parse(String(m.src));}catch{return undefined;}})()
               : m.srcType === "num" ? Number(m.src)
               : m.srcType === "bool" ? !!m.src
               : String(m.src))
            : m.srcType === "env" ? process.env[String(m.src)]
            : undefined;

      out[m.column] = transformValue(srcVal, m.transform || "none");
    }
    return out;
  }

  function buildSQL(table, columns, conflict) {
    const tbl = sanitizeIdent(table);
    const cols = columns.map(sanitizeIdent);
    const placeholders = "(" + cols.map(() => "?").join(",") + ")";

    let head = `INSERT `;
    switch ((conflict?.strategy || "none")) {
      case "ignore":  head += "OR IGNORE "; break;
      case "replace": head += "OR REPLACE "; break;
      case "upsert":  /* handled with ON CONFLICT below */ break;
      case "none":
      default: break;
    }
    head += `INTO ${tbl} (${cols.join(",")}) VALUES `;

    // UPSERT tail
    let tail = "";
    if (conflict?.strategy === "upsert") {
      const keys = (conflict.keys || []).map(sanitizeIdent);
      if (!keys.length) throw new Error("UPSERT requires conflict keys");
      const updateCols = (conflict.updateCols || []).map(sanitizeIdent);
      if (!updateCols.length) throw new Error("UPSERT requires update columns");
      const sets = updateCols.map(c => `${c}=excluded.${c}`).join(", ");
      tail = ` ON CONFLICT(${keys.join(",")}) DO UPDATE SET ${sets}`;
    }

    return { head, placeholders, tail };
  }

  function hasReturningSupport() {
    // Minimal detection: assume sqlite3 >= 5.0 is compiled with modern SQLite (not guaranteed).
    // We'll still guard at runtime with try/catch.
    return true;
  }

  // ---------- Node ----------
  function NodeMultiInsert(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.name = config.name || "";

    node.dbPath = config.dbPath || "";
    node.dbPathType = config.dbPathType || "str";
    node.pragmas = config.pragmas || { wal:false, sync:"", extra:"" };

    node.tx = config.tx || { mode:"all", chunkSize:500, continueOnError:false };
    node.groups = isArr(config.groups) ? config.groups : []; // [{ table, source/sourceType, autoMap, mapping[], conflict{}, ret{} }, ...]

    node.on("input", async (msg, send, done) => {
      const t0 = Date.now();
      const status = (fill, shape, text) => node.status({ fill, shape, text });

      try {
        // resolve DB path
        let dbPath = getTyped(RED, node, msg, node.dbPathType, node.dbPath);
        if (!dbPath || typeof dbPath !== "string") throw new Error("Invalid database path");

        const db = new sqlite3.Database(dbPath);
        const run = (sql, p=[]) => new Promise((res, rej) => db.run(sql, p, function (e) { e ? rej(e) : res(this); }));
        const all = (sql, p=[]) => new Promise((res, rej) => db.all(sql, p, (e, rows) => e ? rej(e) : res(rows)));
        const exec = (sql) => new Promise((res, rej) => db.exec(sql, (e) => e ? rej(e) : res()));

        const timings = { msOpen: 0, msExec: 0, msTotal: 0 };
        const tOpenEnd = Date.now();

        // Apply PRAGMAs
        try {
          const prag = node.pragmas || {};
          const ps = [];
          if (prag.wal) ps.push(`PRAGMA journal_mode=WAL;`);
          if (prag.sync) ps.push(`PRAGMA synchronous=${prag.sync};`);
          if (prag.extra) {
            String(prag.extra).split(";").map(s => s.trim()).filter(Boolean).forEach(x => ps.push(`PRAGMA ${x};`));
          }
          if (ps.length) await exec(ps.join("\n"));
        } catch (e) {
          // non-fatal
          node.warn(`PRAGMA warning: ${e.message}`);
        }

        // Prepare groups work
        const groups = [];
        for (const g of node.groups) {
          const table = sanitizeIdent(g.table);
          const sourceVal = getTyped(RED, node, msg, g.sourceType || "msg", g.source);
          const rowsIn = toRows(sourceVal);
          const autoMap = !!g.autoMap;
          const mapping = isArr(g.mapping) ? g.mapping : [];
          const conflict = g.conflict || { strategy:"none", keys:[], updateCols:[] };
          const ret = g.ret || { mode:"none", idCol:"id", pathType:"msg", path:`sqlite.${table}.rows` };

          // build mapped rows
          let rows = [];
          if (autoMap) {
            rows = rowsIn.map(r => isObj(r) ? r : {});
          } else {
            rows = rowsIn.map(r => buildMappingRow(r, mapping, false));
          }

          // compute columns set
          const colSet = new Set();
          if (autoMap) {
            rows.forEach(r => Object.keys(r || {}).forEach(k => colSet.add(k)));
          } else {
            mapping.forEach(m => colSet.add(m.column));
          }
          const columns = Array.from(colSet);
          if (!columns.length) {
            node.warn(`Group '${table}' has no columns after mapping; skipping.`);
            groups.push({ table, skip:true });
            continue;
          }
          groups.push({ table, rows, columns, conflict, ret, autoMap });
        }

        // global counters
        let totalInserted = 0, totalUpdated = 0, totalErrors = 0, totalSkipped = 0, totalProcessed = 0;
        const perTable = {};
        const returningEnabled = hasReturningSupport();

        async function processGroup(grp) {
          if (grp.skip) {
            perTable[grp.table] = { inserted:0, updated:0, errors:0, skipped:0, total:0 };
            return;
          }
          const { head, placeholders, tail } = buildSQL(grp.table, grp.columns, grp.conflict);

          const rows = grp.rows;
          const CHUNK = Math.max(1, Number(node.tx?.chunkSize || 500));
          const useChunks = node.tx?.mode === "chunk";
          const wrapAll   = node.tx?.mode === "all";
          const contOnErr = !!node.tx?.continueOnError;

          let inserted = 0, updated = 0, errors = 0, skipped = 0;
          const retRows = [];
          const perRowPlaceholders = `${head}${placeholders}${grp.ret?.mode !== "none" && returningEnabled ? " RETURNING *" : ""}${tail || ""};`;

          async function commitChunk(start, end) {
            const slice = rows.slice(start, end);
            if (!slice.length) return;

            // Build value tuples for each row
            for (const r of slice) {
              try {
                const vals = grp.columns.map(c => coerce(r?.[c]));
                // Execute single row insert/upsert to capture lastID reliably
                const res = await run(perRowPlaceholders, vals);
                totalProcessed++;
                if (grp.conflict?.strategy === "upsert") {
                  // sqlite3 'changes' is not directly returned; RETURNING handles it; otherwise we approximate
                }
                inserted++; // default count as inserted; see below for affected
                // RETURN rows
                if (grp.ret?.mode && grp.ret.mode !== "none") {
                  if (returningEnabled) {
                    try {
                      // When using `run` with RETURNING, sqlite3 library does not give rowset; we must switch to `all`
                      const rowsBack = await all(perRowPlaceholders, vals);
                      rowsBack.forEach(rb => {
                        retRows.push({ action: "affected", id: rb[grp.ret.idCol] ?? rb.rowid, data: r });
                      });
                      // adjust counts more accurately if we returned something
                      if (grp.conflict?.strategy === "upsert" && grp.ret.mode === "affected") {
                        // can't easily tell per-row whether updated or inserted; mark affected
                      }
                    } catch {
                      // fallback: lastID only
                      retRows.push({ action: "inserted", id: res && res.lastID, data: r });
                    }
                  } else {
                    // Fallback (older SQLite): lastID
                    retRows.push({ action: "inserted", id: res && res.lastID, data: r });
                  }
                }
              } catch (e) {
                if (!contOnErr) throw e;
                errors++;
              }
            }
          }

          try {
            if (wrapAll) await run("BEGIN");
            if (useChunks && !wrapAll) await run("BEGIN");

            if (useChunks) {
              for (let i = 0; i < rows.length; i += CHUNK) {
                if (!wrapAll && i > 0) await run("BEGIN");
                await commitChunk(i, Math.min(i + CHUNK, rows.length));
                await run("COMMIT");
              }
            } else {
              await commitChunk(0, rows.length);
              if (wrapAll) await run("COMMIT");
              else if (node.tx?.mode === "perTable") { /* we're inside per-table outer BEGIN/COMMIT handled below */ }
              else if (node.tx?.mode === "off") { /* nothing */ }
            }
          } catch (e) {
            try { await run("ROLLBACK"); } catch {}
            if (!contOnErr) throw e;
            errors++;
          }

          // record per table
          perTable[grp.table] = {
            inserted, updated, errors, skipped, total: rows.length
          };

          // write returned rows if requested
          if (grp.ret?.mode && grp.ret.mode !== "none" && retRows.length) {
            const tgtVal = grp.ret.path;
            const tgtType = grp.ret.pathType || "msg";
            if (tgtType === "msg") RED.util.setMessageProperty(msg, tgtVal, retRows, true);
            else if (tgtType === "flow") node.context().flow.set(tgtVal, retRows);
            else if (tgtType === "global") node.context().global.set(tgtVal, retRows);
          }

          totalInserted += inserted;
          totalUpdated  += updated;
          totalErrors   += errors;
          totalSkipped  += skipped;
        }

        // Transaction mode orchestration
        if (node.tx?.mode === "perTable") {
          for (const g of groups) {
            try {
              await run("BEGIN");
              await processGroup(g);
              await run("COMMIT");
            } catch (e) {
              try { await run("ROLLBACK"); } catch {}
              if (!node.tx?.continueOnError) throw e;
              totalErrors++;
            }
          }
        } else if (node.tx?.mode === "all") {
          try {
            await run("BEGIN");
            for (const g of groups) await processGroup(g);
            await run("COMMIT");
          } catch (e) {
            try { await run("ROLLBACK"); } catch {}
            if (!node.tx?.continueOnError) throw e;
            totalErrors++;
          }
        } else { // off
          for (const g of groups) await processGroup(g);
        }

        const tExecEnd = Date.now();
        timings.msOpen = tOpenEnd - t0;
        timings.msExec = tExecEnd - tOpenEnd;
        timings.msTotal = Date.now() - t0;

        const summary = {
          ok: totalErrors === 0,
          counts: {
            inserted: totalInserted,
            updated: totalUpdated,
            errors: totalErrors,
            skipped: totalSkipped,
          },
          tables: perTable,
          timings
        };

        // Write summary at msg.sqlite (fixed) + also emit as msg.payload for convenience
        msg.sqlite = summary;
        if (config.emitPayloadSummary) msg.payload = summary;

        status(summary.ok ? "green" : "red", "dot",
          `OK: ins ${totalInserted} upd ${totalUpdated} err ${totalErrors}`);

        db.close();
        send(msg);
        done();
      } catch (err) {
        node.status({ fill:"red", shape:"ring", text: "error" });
        done(err);
      }
    });
  }

  RED.nodes.registerType("sqlite-multi-insert", NodeMultiInsert);
};
