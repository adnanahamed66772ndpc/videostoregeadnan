const path = require("path");
const fs = require("fs");
const initSqlJs = require("sql.js");

const DATA_DIR = path.join(__dirname, "..", "data");
const DB_PATH = path.join(DATA_DIR, "app.db");
let db = null;

function ensureDataDir() {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function persistDb() {
    if (db) {
        ensureDataDir();
        const data = db.export();
        fs.writeFileSync(DB_PATH, Buffer.from(data), "binary");
    }
}

function rowToObj(columns, values) {
    const o = {};
    columns.forEach((c, i) => { o[c] = values[i]; });
    return o;
}

async function initDb() {
    if (db) return db;
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

function getDb() {
    if (!db) throw new Error("Database not initialized. Call initDb() at startup.");
    return db;
}

function loadBucketsConfig() {
    const database = getDb();
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

function runSql(sql, params = []) {
    getDb().run(sql, params);
}

function getOne(sql, params = []) {
    const database = getDb();
    const stmt = database.prepare(sql);
    stmt.bind(params);
    if (!stmt.step()) return null;
    const columns = stmt.getColumnNames();
    const values = stmt.get();
    return rowToObj(columns, values);
}

function getAll(sql, params = []) {
    const database = getDb();
    const stmt = database.prepare(sql);
    stmt.bind(params);
    const columns = stmt.getColumnNames();
    const rows = [];
    while (stmt.step()) rows.push(rowToObj(columns, stmt.get()));
    return rows;
}

function runAndPersist(sql, params = []) {
    getDb().run(sql, params);
    persistDb();
}

module.exports = { initDb, getDb, loadBucketsConfig, persistDb, runSql, getOne, getAll, runAndPersist };
