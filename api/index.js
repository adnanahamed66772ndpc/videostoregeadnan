const app = require("../index");
const { initDb } = require("../lib/db");

let dbReady = null;

module.exports = async (req, res) => {
    try {
        if (!dbReady) dbReady = initDb();
        await dbReady;
        return app(req, res);
    } catch (err) {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: err && err.message ? err.message : "Failed to initialize app" }));
    }
};

