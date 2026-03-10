const express = require("express");
const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const { initDb, loadBucketsConfig, runAndPersist, getOne, getAll } = require("./lib/db");
const { S3Client, PutObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, GetObjectCommand, HeadObjectCommand, ListObjectsV2Command, CopyObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const crypto = require("crypto");

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors({ origin: true, credentials: true }));
app.use(cookieParser());

const LOGIN_USER = process.env.LOGIN_USER || "admin";
const LOGIN_PASS = process.env.LOGIN_PASS || "Adnan2020";
const AUTH_SECRET = process.env.AUTH_SECRET || process.env.SESSION_SECRET || "r2-file-manager-auth-secret";
const AUTH_COOKIE = "r2fm_auth";
const AUTH_TTL_MS = 7 * 24 * 60 * 60 * 1000;

function signAuth(username, issuedAt) {
    return crypto.createHmac("sha256", AUTH_SECRET).update(`${username}|${issuedAt}`).digest("hex");
}

function parseAuthCookie(value) {
    if (!value || typeof value !== "string") return null;
    const parts = value.split(".");
    if (parts.length !== 3) return null;
    const [uB64, issuedAtStr, sig] = parts;
    const issuedAt = Number(issuedAtStr);
    if (!Number.isFinite(issuedAt)) return null;
    let username = "";
    try {
        username = Buffer.from(uB64, "base64url").toString("utf8");
    } catch {
        return null;
    }
    if (!username) return null;
    const expected = signAuth(username, issuedAtStr);
    const sigBuf = Buffer.from(sig);
    const expBuf = Buffer.from(expected);
    if (sigBuf.length !== expBuf.length) return null;
    const ok = crypto.timingSafeEqual(sigBuf, expBuf);
    if (!ok) return null;
    if (Date.now() - issuedAt > AUTH_TTL_MS) return null;
    return { username, issuedAt };
}

function setAuthCookie(res, username) {
    const issuedAt = Date.now().toString();
    const uB64 = Buffer.from(username, "utf8").toString("base64url");
    const sig = signAuth(username, issuedAt);
    const value = `${uB64}.${issuedAt}.${sig}`;
    const secure = String(process.env.NODE_ENV || "").toLowerCase() === "production";
    res.cookie(AUTH_COOKIE, value, { httpOnly: true, sameSite: "lax", secure, maxAge: AUTH_TTL_MS, path: "/" });
}

function clearAuthCookie(res) {
    res.clearCookie(AUTH_COOKIE, { path: "/" });
}

function getAuthedUser(req) {
    const v = req.cookies ? req.cookies[AUTH_COOKIE] : null;
    const parsed = parseAuthCookie(v);
    return parsed ? parsed.username : null;
}

function requireAuth(req, res, next) {
    const isLoginPage = req.path === "/login";
    const isLogout = req.path === "/logout";
    const user = getAuthedUser(req);

    if (isLogout) return next();
    if (isLoginPage && req.method === "GET") return next();
    if (isLoginPage && req.method === "POST") return next();
    if (user) return next();

    if (req.xhr || (req.headers.accept && String(req.headers.accept).indexOf("application/json") !== -1)) {
        return res.status(401).json({ error: "Unauthorized" });
    }
    return res.redirect("/login");
}
app.use(requireAuth);

app.get("/login", (req, res) => {
    if (getAuthedUser(req)) return res.redirect("/");
    res.sendFile(path.join(__dirname, "templates/login.html"));
});

app.post("/login", (req, res) => {
    const username = (req.body.username || "").trim();
    const password = req.body.password || "";
    if (username === LOGIN_USER && password === LOGIN_PASS) {
        setAuthCookie(res, username);
        return res.redirect("/");
    }
    res.redirect("/login?error=1");
});

app.get("/logout", (req, res) => {
    clearAuthCookie(res);
    res.redirect("/login");
});

app.get("/api/auth-check", (req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.status(200).json({ ok: true, user: getAuthedUser(req) });
});

const upload = multer({ storage: multer.memoryStorage() });

function getAccountById(accounts, accountId) {
    return accounts.find((a) => a.id === accountId) || null;
}

const s3ClientCache = new Map();

async function getActiveBucket() {
    const { accounts, buckets, activeBucketId } = await loadBucketsConfig();
    if (!activeBucketId) return null;
    const bucket = buckets.find((b) => b.id === activeBucketId);
    if (!bucket) return null;
    const account = getAccountById(accounts, bucket.accountId);
    if (!account) return null;
    return {
        id: bucket.id,
        accountId: account.id,
        bucketName: bucket.bucketName,
        label: bucket.label,
        endpoint: account.endpoint,
        accessKeyId: account.accessKeyId,
        secretAccessKey: account.secretAccessKey
    };
}

function getS3Client(account) {
    if (!account) return null;
    if (s3ClientCache.has(account.id)) return s3ClientCache.get(account.id);
    const client = new S3Client({
        region: "auto",
        endpoint: account.endpoint,
        credentials: { accessKeyId: account.accessKeyId, secretAccessKey: account.secretAccessKey }
    });
    s3ClientCache.set(account.id, client);
    return client;
}

async function getActiveS3AndBucket() {
    const bucket = await getActiveBucket();
    if (!bucket) return { s3: null, bucketName: null, bucket };
    const { accounts } = await loadBucketsConfig();
    const account = getAccountById(accounts, bucket.accountId);
    return {
        s3: getS3Client(account),
        bucketName: bucket.bucketName,
        bucket
    };
}

// Helper to convert S3 streams to strings
const streamToString = async (stream) => {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks).toString("utf-8");
};

app.get("/", (req, res) => {
    res.setHeader("Cache-Control", "private, no-store");
    res.sendFile(path.join(__dirname, "templates/index.html"));
});

app.get("/settings", (req, res) => {
    res.setHeader("Cache-Control", "private, no-store");
    res.sendFile(path.join(__dirname, "templates/settings.html"));
});

// Health check - server status
app.get("/health", (req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.status(200).json({
        status: "ok",
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

// Bucket status - R2 connectivity (uses active bucket)
app.get("/bucket-status", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) {
        return res.status(503).json({
            status: "error",
            message: "No bucket selected. Add a bucket in Settings → Buckets."
        });
    }
    try {
        const command = new ListObjectsV2Command({ Bucket: bucketName, MaxKeys: 1 });
        await s3.send(command);
        res.status(200).json({ status: "ok", bucket: bucketName, message: "Connected" });
    } catch (error) {
        res.status(503).json({
            status: "error",
            bucket: bucketName,
            message: error.message || "Failed to connect to bucket"
        });
    }
});

// 1. Create a file inside a folder
app.post("/create-file", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected. Add one in Settings → Buckets." });
    const { folder, fileName, content } = req.body;
    const params = {
        Bucket: bucketName,
        Key: `${folder}/${fileName}`,
        Body: JSON.stringify(content),
        ContentType: "application/json"
    };
    try {
        await s3.send(new PutObjectCommand(params));
        res.status(201).send({ message: "File created successfully" });
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 2. Delete a file inside a folder
app.delete("/delete-file", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { folder, fileName, fileNames } = req.body;
    try {
        if (fileNames && Array.isArray(fileNames)) {
            const deleteParams = { Bucket: bucketName, Delete: { Objects: fileNames.map((name) => ({ Key: `${folder}/${name}` })) } };
            const result = await s3.send(new DeleteObjectsCommand(deleteParams));
            const deletedCount = result.Deleted ? result.Deleted.length : 0;
            const errorCount = result.Errors ? result.Errors.length : 0;
            if (errorCount > 0) {
                return res.status(207).send({ message: `Deleted ${deletedCount} files, ${errorCount} failed`, deleted: deletedCount, failed: errorCount, errors: result.Errors });
            }
            res.status(200).send({ message: `Deleted ${deletedCount} files successfully` });
            return;
        }
        if (fileName === "*") {
            const listData = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, Prefix: `${folder}/` }));
            if (!listData.Contents || listData.Contents.length === 0) {
                return res.status(404).send({ message: "No files found in the folder" });
            }
            await s3.send(new DeleteObjectsCommand({ Bucket: bucketName, Delete: { Objects: listData.Contents.map((item) => ({ Key: item.Key })) } }));
            res.status(200).send({ message: `Deleted ${listData.Contents.length} files successfully` });
        } else {
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: `${folder}/${fileName}` }));
            } catch (error) {
                if (error.name === 'NotFound') return res.status(200).send({ message: "File already deleted or doesn't exist" });
                throw error;
            }
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: `${folder}/${fileName}` }));
            res.status(200).send({ message: "File deleted successfully" });
        }
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 3. Update a file inside a folder (overwrite)
app.put("/update-file", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { folder, fileName, content } = req.body;
    try {
        await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: `${folder}/${fileName}`, Body: JSON.stringify(content), ContentType: "application/json" }));
        res.status(200).send({ message: "File updated successfully" });
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 4. Read a file inside a folder
app.get("/read-file", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { folder, fileName } = req.query;
    try {
        const data = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: `${folder}/${fileName}` }));
        const content = await streamToString(data.Body);
        res.status(200).send(JSON.parse(content));
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 5. List files inside a folder or root
app.get("/list-files", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { folder } = req.query;
    const params = { Bucket: bucketName, Prefix: folder ? `${folder}/` : "", Delimiter: "/" };
    try {
        const data = await s3.send(new ListObjectsV2Command(params));
        res.status(200).send({ files: data.Contents?.map((item) => item.Key) || [], folders: data.CommonPrefixes?.map((item) => item.Prefix) || [] });
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 6. List all folders
app.get("/list-folders", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    try {
        const data = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, Delimiter: "/" }));
        res.status(200).send(data.CommonPrefixes?.map((prefix) => prefix.Prefix) || []);
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 7. Duplicate a folder
app.post("/duplicate-folder", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { sourceFolder, targetFolder } = req.body;
    try {
        const listData = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, Prefix: `${sourceFolder}/` }));
        if (!listData.Contents || listData.Contents.length === 0) return res.status(404).send({ message: "Source folder is empty or not found" });
        await Promise.all(listData.Contents.map((item) => {
            const newKey = item.Key.replace(sourceFolder, targetFolder);
            return s3.send(new CopyObjectCommand({ Bucket: bucketName, CopySource: `${bucketName}/${item.Key}`, Key: newKey }));
        }));
        res.status(201).send({ message: "Folder duplicated successfully" });
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 8. Rename a folder
app.put("/rename-folder", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { sourceFolder, targetFolder } = req.body;
    try {
        const listData = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, Prefix: `${sourceFolder}/` }));
        if (!listData.Contents || listData.Contents.length === 0) return res.status(404).send({ message: "Source folder is empty or not found" });
        await Promise.all(listData.Contents.map(async (item) => {
            const newKey = item.Key.replace(sourceFolder, targetFolder);
            await s3.send(new CopyObjectCommand({ Bucket: bucketName, CopySource: `${bucketName}/${item.Key}`, Key: newKey }));
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: item.Key }));
        }));
        res.status(200).send({ message: "Folder renamed successfully" });
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

// 9. Upload media file
app.post(
    "/upload-files",
    (req, res, next) => {
        upload.array("files", 50)(req, res, (err) => {
            if (err instanceof multer.MulterError) {
                if (err.code === "LIMIT_FILE_COUNT") return res.status(400).send({ error: "Too many files. Maximum 50 files allowed." });
                return res.status(400).send({ error: err.message });
            }
            if (err) return res.status(500).send({ error: err.message });
            next();
        });
    },
    async (req, res) => {
        const { s3, bucketName } = await getActiveS3AndBucket();
        if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
        const folder = req.body.folder || "uploads";
        const files = req.files;
        if (!files || files.length === 0) return res.status(400).send({ message: "No files uploaded" });
        try {
            await Promise.all(files.map((file) => s3.send(new PutObjectCommand({ Bucket: bucketName, Key: `${folder}/${file.originalname}`, Body: file.buffer, ContentType: file.mimetype }))));
            res.status(201).send({ message: "Files uploaded successfully", fileNames: files.map((f) => f.originalname) });
        } catch (error) {
            res.status(500).send({ error: error.message });
        }
    }
);

// --- Buckets API (one account, multiple buckets) ---
app.get("/api/accounts", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    try {
        const config = await loadBucketsConfig();
        const accounts = Array.isArray(config.accounts) ? config.accounts : [];
        res.status(200).json({ accounts: accounts.map((a) => ({ id: a.id, label: a.label, endpoint: a.endpoint })) });
    } catch (err) {
        console.error("GET /api/accounts error:", err);
        res.status(500).json({ error: err.message || "Failed to load accounts" });
    }
});

app.post("/api/accounts", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { label, endpoint, accessKeyId, secretAccessKey } = req.body || {};
    if (!label || !endpoint || !accessKeyId || !secretAccessKey) {
        return res.status(400).json({ error: "Missing: label, endpoint, accessKeyId, secretAccessKey" });
    }
    try {
        const id = require("crypto").randomUUID();
        await runAndPersist("INSERT INTO accounts (id, label, endpoint, access_key_id, secret_access_key) VALUES (?, ?, ?, ?, ?)", [id, label, endpoint, accessKeyId, secretAccessKey]);
        res.status(201).json({ id, label, endpoint });
    } catch (err) {
        res.status(500).json({ error: err.message || "Failed to add account" });
    }
});

app.delete("/api/accounts/:id", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { id } = req.params;
    try {
        const acc = await getOne("SELECT id FROM accounts WHERE id = ?", [id]);
        if (!acc) return res.status(404).json({ error: "Account not found" });
        const activeRow = await getOne("SELECT value FROM settings WHERE key = ?", ["activeBucketId"]);
        const bucketsOfAccount = await getAll("SELECT id FROM buckets WHERE account_id = ?", [id]);
        const activeBucketId = activeRow ? activeRow.value : null;
        const activeWasInAccount = activeBucketId && bucketsOfAccount.some((b) => b.id === activeBucketId);
        await runAndPersist("DELETE FROM buckets WHERE account_id = ?", [id]);
        await runAndPersist("DELETE FROM accounts WHERE id = ?", [id]);
        if (activeWasInAccount) {
            const firstRemaining = await getOne("SELECT id FROM buckets LIMIT 1");
            await runAndPersist("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ["activeBucketId", firstRemaining ? firstRemaining.id : null]);
        }
        s3ClientCache.delete(id);
        res.status(200).json({ message: "Account removed" });
    } catch (err) {
        res.status(500).json({ error: err.message || "Failed to remove account" });
    }
});

app.get("/api/buckets", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    try {
        const config = await loadBucketsConfig();
        const accounts = Array.isArray(config.accounts) ? config.accounts : [];
        const buckets = Array.isArray(config.buckets) ? config.buckets : [];
        const activeBucketId = config.activeBucketId || null;
        const list = buckets.map((b) => {
            const acc = getAccountById(accounts, b.accountId);
            return {
                id: b.id,
                accountId: b.accountId || "",
                accountLabel: acc ? acc.label : "",
                label: b.label || b.bucketName || "",
                bucketName: b.bucketName || "",
                isActive: b.id === activeBucketId
            };
        });
        res.status(200).json({
            accounts: accounts.map((a) => ({ id: a.id, label: a.label || "" })),
            buckets: list,
            activeBucketId
        });
    } catch (err) {
        console.error("GET /api/buckets error:", err);
        res.status(500).json({ error: err.message || "Failed to load buckets" });
    }
});

app.get("/api/buckets/active", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const bucket = await getActiveBucket();
    if (!bucket) return res.status(200).json({ active: null });
    res.status(200).json({ active: { id: bucket.id, label: bucket.label, bucketName: bucket.bucketName } });
});

app.post("/api/buckets", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { accountId, bucketName, label } = req.body || {};
    if (!accountId || !bucketName) return res.status(400).json({ error: "Missing: accountId, bucketName" });
    try {
        const acc = await getOne("SELECT id FROM accounts WHERE id = ?", [accountId]);
        if (!acc) return res.status(404).json({ error: "Account not found" });
        const id = require("crypto").randomUUID();
        const labelVal = label || bucketName;
        await runAndPersist("INSERT INTO buckets (id, account_id, bucket_name, label) VALUES (?, ?, ?, ?)", [id, accountId, bucketName, labelVal]);
        const activeRow = await getOne("SELECT value FROM settings WHERE key = ?", ["activeBucketId"]);
        if (!activeRow || !activeRow.value) {
            await runAndPersist("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ["activeBucketId", id]);
        }
        res.status(201).json({ id, accountId, bucketName, label: labelVal, isActive: !activeRow || !activeRow.value });
    } catch (err) {
        res.status(500).json({ error: err.message || "Failed to add bucket" });
    }
});

app.put("/api/buckets/active", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { id } = req.body || {};
    try {
        const b = await getOne("SELECT id FROM buckets WHERE id = ?", [id]);
        if (!b) return res.status(404).json({ error: "Bucket not found" });
        await runAndPersist("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ["activeBucketId", id]);
        res.status(200).json({ activeBucketId: id });
    } catch (err) {
        res.status(500).json({ error: err.message || "Failed to set active bucket" });
    }
});

app.delete("/api/buckets/:id", async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    const { id } = req.params;
    try {
        const b = await getOne("SELECT id FROM buckets WHERE id = ?", [id]);
        if (!b) return res.status(404).json({ error: "Bucket not found" });
        const activeRow = await getOne("SELECT value FROM settings WHERE key = ?", ["activeBucketId"]);
        await runAndPersist("DELETE FROM buckets WHERE id = ?", [id]);
        if (activeRow && activeRow.value === id) {
            const first = await getOne("SELECT id FROM buckets LIMIT 1");
            await runAndPersist("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ["activeBucketId", first ? first.id : null]);
        }
        res.status(200).json({ message: "Bucket removed" });
    } catch (err) {
        res.status(500).json({ error: err.message || "Failed to remove bucket" });
    }
});

// Get single file signed URL (for preview / download)
app.get("/get-file-url", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).json({ error: "No bucket selected." });
    const { key, expires } = req.query;
    if (!key) return res.status(400).json({ error: "Missing key." });
    try {
        const urlOptions = expires ? { expiresIn: parseInt(expires, 10) } : undefined;
        const url = await getSignedUrl(s3, new GetObjectCommand({ Bucket: bucketName, Key: key }), urlOptions);
        res.status(200).json({ url });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 10. Get URLs for files inside a folder or root
app.get("/get-file-urls", async (req, res) => {
    const { s3, bucketName } = await getActiveS3AndBucket();
    if (!s3 || !bucketName) return res.status(503).send({ error: "No bucket selected." });
    const { folder, expires } = req.query;
    try {
        const data = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, Prefix: folder ? `${folder}/` : "" }));
        const fileKeys = data.Contents?.map((item) => item.Key) || [];
        const urlOptions = expires ? { expiresIn: parseInt(expires, 10) } : undefined;
        const urls = await Promise.all(fileKeys.map(async (fileKey) => ({
            fileKey,
            url: await getSignedUrl(s3, new GetObjectCommand({ Bucket: bucketName, Key: fileKey }), urlOptions)
        })));
        res.status(200).send(urls);
    } catch (error) {
        res.status(500).send({ error: error.message });
    }
});

module.exports = app;

// Start server only when run locally (not on Vercel serverless)
if (require.main === module) {
    const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
    (async () => {
        try {
            await initDb();
            app.listen(PORT, () => {
                console.log(`Server is running on port ${PORT}`);
            });
        } catch (err) {
            console.error("Failed to start server:", err);
            process.exit(1);
        }
    })();
}
