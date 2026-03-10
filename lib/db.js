const path = require("path");
const fs = require("fs");

// On Render/Heroku etc. use DATA_DIR env var pointing to a persistent disk path (e.g. /data)
const DATA_DIR = process.env.DATA_DIR
    ? path.resolve(process.env.DATA_DIR)
    : path.join(__dirname, "..", "data");
const DB_PATH = path.join(DATA_DIR, "app.db");

// Use Turso (remote SQLite) when env is set — works on Render free tier (no persistent disk)
const USE_TURSO = !!(process.env.TURSO_DATABASE_URL && process.env.TURSO_AUTH_TOKEN);

let db = null;
let tursoClient = null;

function ensureDataDir() {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function rowToObj(columns, values) {
    const o = {};
    columns.forEach((c, i) => { o[c] = values[i]; });
    return o;
}

// ---------- Turso (libsql) backend ----------
async function initTurso() {
    if (tursoClient) return tursoClient;
    const { createClient } = require("@libsql/client");
    tursoClient = createClient({
        url: process.env.TURSO_DATABASE_URL,
        authToken: process.env.TURSO_AUTH_TOKEN
    });
    await tursoClient.execute({
        sql: `CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            access_key_id TEXT NOT NULL,
            secret_access_key TEXT NOT NULL
        );`
    });
    await tursoClient.execute({
        sql: `CREATE TABLE IF NOT EXISTS buckets (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            bucket_name TEXT NOT NULL,
            label TEXT
        );`
    });
    await tursoClient.execute({
        sql: `CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );`
    });
    const r = await tursoClient.execute({ sql: "SELECT COUNT(*) as n FROM accounts" });
    const n = (r.rows && r.rows[0]) ? (Number(r.rows[0].n ?? r.rows[0][0]) || 0) : 0;
    if (n === 0 && fs.existsSync(path.join(DATA_DIR, "buckets.json"))) {
        try {
            const data = JSON.parse(fs.readFileSync(path.join(DATA_DIR, "buckets.json"), "utf8"));
            const accounts = Array.isArray(data.accounts) ? data.accounts : [];
            const buckets = Array.isArray(data.buckets) ? data.buckets : [];
            if (accounts.length > 0 && buckets.length > 0) {
                for (const a of accounts) {
                    await tursoClient.execute({
                        sql: "INSERT INTO accounts (id, label, endpoint, access_key_id, secret_access_key) VALUES (?, ?, ?, ?, ?)",
                        args: [a.id, a.label || "", a.endpoint || "", a.accessKeyId || "", a.secretAccessKey || ""]
                    });
                }
                for (const b of buckets) {
                    await tursoClient.execute({
                        sql: "INSERT INTO buckets (id, account_id, bucket_name, label) VALUES (?, ?, ?, ?)",
                        args: [b.id, b.accountId, b.bucketName || "", b.label || b.bucketName || ""]
                    });
                }
                const activeId = data.activeBucketId || data.activeId || (buckets[0] && buckets[0].id);
                if (activeId) {
                    await tursoClient.execute({
                        sql: "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        args: ["activeBucketId", activeId]
                    });
                }
            }
        } catch (e) {
            console.warn("Migration from buckets.json failed:", e.message);
        }
    }
    return tursoClient;
}

async function tursoLoadBucketsConfig() {
    const client = tursoClient || (await initTurso());
    const accounts = [];
    const accRes = await client.execute({ sql: "SELECT id, label, endpoint, access_key_id, secret_access_key FROM accounts" });
    const accRows = accRes.rows || [];
    const accCols = accRes.columns || ["id", "label", "endpoint", "access_key_id", "secret_access_key"];
    for (const row of accRows) {
        const v = Array.isArray(row) ? row : [row.id, row.label, row.endpoint, row.access_key_id, row.secret_access_key];
        accounts.push({
            id: v[0],
            label: v[1],
            endpoint: v[2],
            accessKeyId: v[3],
            secretAccessKey: v[4]
        });
    }
    const buckets = [];
    const bktRes = await client.execute({ sql: "SELECT id, account_id, bucket_name, label FROM buckets" });
    const bktRows = bktRes.rows || [];
    for (const row of bktRows) {
        const v = Array.isArray(row) ? row : [row.id, row.account_id, row.bucket_name, row.label];
        buckets.push({ id: v[0], accountId: v[1], bucketName: v[2], label: v[3] });
    }
    let activeBucketId = null;
    const setRes = await client.execute({ sql: "SELECT value FROM settings WHERE key = 'activeBucketId'" });
    if (setRes.rows && setRes.rows[0]) {
        const r = setRes.rows[0];
        activeBucketId = (typeof r === "object" && r !== null && "value" in r) ? r.value : r[0];
    }
    return { accounts, buckets, activeBucketId };
}

async function tursoRunAndPersist(sql, params = []) {
    const client = tursoClient || (await initTurso());
    await client.execute({ sql, args: params });
}

async function tursoGetOne(sql, params = []) {
    const client = tursoClient || (await initTurso());
    const res = await client.execute({ sql, args: params });
    const rows = res.rows || [];
    const columns = res.columns || [];
    if (rows.length === 0) return null;
    const row = rows[0];
    const values = Array.isArray(row) ? row : (columns.map((c) => row[c]));
    return rowToObj(columns, values);
}

async function tursoGetAll(sql, params = []) {
    const client = tursoClient || (await initTurso());
    const res = await client.execute({ sql, args: params });
    const rows = res.rows || [];
    const columns = res.columns || [];
    return rows.map((row) => {
        const values = Array.isArray(row) ? row : (columns.map((c) => row[c]));
        return rowToObj(columns, values);
    });
}

// ---------- SQL.js (file) backend ----------
function persistDb() {
    if (db) {
        ensureDataDir();
        const data = db.export();
        fs.writeFileSync(DB_PATH, Buffer.from(data), "binary");
    }
}

async function initSqlite() {
    if (db) return db;
    const initSqlJs = require("sql.js");
    const SQL = await initSqlJs();
    ensureDataDir();
    if (fs.existsSync(DB_PATH)) {
        const buf = fs.readFileSync(DB_PATH);
        db = new SQL.Database(new Uint8Array(buf));
    } else {
        db = new SQL.Database();
    }
    db.run(`
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            access_key_id TEXT NOT NULL,
            secret_access_key TEXT NOT NULL
        );
    `);
    db.run(`
        CREATE TABLE IF NOT EXISTS buckets (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            bucket_name TEXT NOT NULL,
            label TEXT
        );
    `);
    db.run(`
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    `);
    const countRes = db.exec("SELECT COUNT(*) as n FROM accounts");
    const n = countRes.length && countRes[0].values.length ? countRes[0].values[0][0] : 0;
    if (n === 0 && fs.existsSync(path.join(DATA_DIR, "buckets.json"))) {
        try {
            const data = JSON.parse(fs.readFileSync(path.join(DATA_DIR, "buckets.json"), "utf8"));
            const accounts = Array.isArray(data.accounts) ? data.accounts : [];
            const buckets = Array.isArray(data.buckets) ? data.buckets : [];
            if (accounts.length > 0 && buckets.length > 0) {
                accounts.forEach((a) => db.run("INSERT INTO accounts (id, label, endpoint, access_key_id, secret_access_key) VALUES (?, ?, ?, ?, ?)", [a.id, a.label || "", a.endpoint || "", a.accessKeyId || "", a.secretAccessKey || ""]));
                buckets.forEach((b) => db.run("INSERT INTO buckets (id, account_id, bucket_name, label) VALUES (?, ?, ?, ?)", [b.id, b.accountId, b.bucketName || "", b.label || b.bucketName || ""]));
                const activeId = data.activeBucketId || data.activeId || (buckets[0] && buckets[0].id);
                if (activeId) db.run("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ["activeBucketId", activeId]);
                persistDb();
            }
        } catch (e) {
            console.warn("Migration from buckets.json failed:", e.message);
        }
    }
    return db;
}

function sqliteLoadBucketsConfig() {
    const database = db;
    const accounts = [];
    const accRes = database.exec("SELECT id, label, endpoint, access_key_id, secret_access_key FROM accounts");
    if (accRes.length && accRes[0].values.length) {
        accRes[0].values.forEach((v) => {
            const o = {};
            accRes[0].columns.forEach((c, i) => {
                const key = c === "access_key_id" ? "accessKeyId" : c === "secret_access_key" ? "secretAccessKey" : c;
                o[key] = v[i];
            });
            accounts.push(o);
        });
    }
    const buckets = [];
    const bktRes = database.exec("SELECT id, account_id, bucket_name, label FROM buckets");
    if (bktRes.length && bktRes[0].values.length) {
        bktRes[0].values.forEach((v) => {
            const o = {};
            bktRes[0].columns.forEach((c, i) => {
                const key = c === "account_id" ? "accountId" : c === "bucket_name" ? "bucketName" : c;
                o[key] = v[i];
            });
            buckets.push(o);
        });
    }
    let activeBucketId = null;
    const setRes = database.exec("SELECT value FROM settings WHERE key = 'activeBucketId'");
    if (setRes.length && setRes[0].values.length) activeBucketId = setRes[0].values[0][0];
    return { accounts, buckets, activeBucketId };
}

function sqliteRunAndPersist(sql, params = []) {
    db.run(sql, params);
    persistDb();
}

function sqliteGetOne(sql, params = []) {
    const stmt = db.prepare(sql);
    stmt.bind(params);
    if (!stmt.step()) return null;
    return rowToObj(stmt.getColumnNames(), stmt.get());
}

function sqliteGetAll(sql, params = []) {
    const stmt = db.prepare(sql);
    stmt.bind(params);
    const columns = stmt.getColumnNames();
    const rows = [];
    while (stmt.step()) rows.push(rowToObj(columns, stmt.get()));
    return rows;
}

// ---------- Unified async API ----------
async function initDb() {
    if (USE_TURSO) {
        await initTurso();
        if (USE_TURSO) console.log("Database: Turso (remote SQLite)");
        return tursoClient;
    }
    await initSqlite();
    return db;
}

function getDb() {
    if (USE_TURSO) {
        if (!tursoClient) throw new Error("Database not initialized. Call initDb() at startup.");
        return tursoClient;
    }
    if (!db) throw new Error("Database not initialized. Call initDb() at startup.");
    return db;
}

async function loadBucketsConfig() {
    if (USE_TURSO) return tursoLoadBucketsConfig();
    return Promise.resolve(sqliteLoadBucketsConfig());
}

async function runAndPersist(sql, params = []) {
    if (USE_TURSO) return tursoRunAndPersist(sql, params);
    return Promise.resolve(sqliteRunAndPersist(sql, params));
}

async function getOne(sql, params = []) {
    if (USE_TURSO) return tursoGetOne(sql, params);
    return Promise.resolve(sqliteGetOne(sql, params));
}

async function getAll(sql, params = []) {
    if (USE_TURSO) return tursoGetAll(sql, params);
    return Promise.resolve(sqliteGetAll(sql, params));
}

function runSql(sql, params = []) {
    getDb();
    if (USE_TURSO) return tursoRunAndPersist(sql, params);
    return Promise.resolve(sqliteRunAndPersist(sql, params));
}

function persistDbNoop() {}
module.exports = { initDb, getDb, loadBucketsConfig, persistDb: USE_TURSO ? persistDbNoop : persistDb, runSql, getOne, getAll, runAndPersist };
