const fs = require("fs");
const pg = require("pg");
const {createReadStream } = require("node:fs");
const {createInterface } = require("node:readline");

// //////////////////////////////////////////
// Notes: 
// 1) Need add lock mechanizem, to prevent colision between 'storeEvent()'
//   (write to file) and 'readFileLines()' (read and truncate the file).
// 2) In order to minimize the lock time during the read - we can rename the file (temporarily, since
//    we can delete it at end of the read), and imidiatly create new file, with the original name.
// //////////////////////////////////////////
class FsHelper {
    static SRC_EVENTS_FILE_PATH = "events.jsonl";
    static RECIEVED_EVENTS_FILE_PATH = "recieved-events.jsonl";

    static async storeEvent(event) {
        await fs.promises.appendFile(FsHelper.RECIEVED_EVENTS_FILE_PATH, JSON.stringify(event) + "\n", {encoding: "utf-8"});
    }

    static async readFileLines(path, linesHandler, truncate) {

        // The 'linesReader' enable us to read the file content in 'line by line' manner.
        const linesReader = createInterface({
            input: createReadStream(path),
            crlfDelay: Infinity,
        });

        // Deal with events of the lines reader.
        return new Promise(async (resolve, reject) => {
            linesReader.on("close", () => {
                // The 'setTimeout()' is workaround, to deal with bizarre behavior of the lines reader ('close' event arrived BEFORE arrival of last 'line' event).
                setTimeout(async () => {
                    if (truncate) {
                        await fs.promises.truncate(path);
                    }
                    // File's lines reading ended, resolve the promise.
                    resolve();
                }, 1000);
            });
            linesReader.on("error", (err) => {
                // File's lines reading failed, reject the promise.
                reject(err);
            });
            linesReader.on("line", async (line) => {
                // File's lines reader notify about current fetched line, notify client about the line.
                await linesHandler(line);
            });
        });
    }
}

class DbHelper {
    static DATABASE_NAME = "public";
    static PASSWORD ="postgres";
    static USER = "postgres";
    static SCHEMA_NAME = "public";
    static TABLE_NANE = "users_revenue";
    
    static async create() {
        let instance = new DbHelper()
        instance._client = new pg.Client({database: DbHelper.DATABASE_NAME, password: DbHelper.PASSWORD, user: DbHelper.USER });
        await instance._client.connect();
        return instance;
    }

    async createTableAndIndexIfNotExists() {
        await this._client.query(`CREATE TABLE IF NOT EXISTS ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE} (user_id varchar, revenue integer)`);
        await this._client.query(`CREATE UNIQUE INDEX IF NOT EXISTS user_id_unique ON ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE} (user_id)`); // Index on 'user_id' will improve our SELECT clauses, and also - 'ON CONFLICT (user_id)' within the 'upsert' query (see method ' updateDbFromLine()') cannot work without such a definition.
    }

    async updateUserData(event, eventOperator) {
    
        // Lock the row for update, to prevent other transactions from modifying it.
        await this._client.query(`
            SELECT * FROM ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE}
            WHERE user_id = '${event.userId}'
            FOR UPDATE;
        `);
    
        // Perform 'upsert' (i.e. update or insert) of the relevnt row.
        await this._client.query(`
            INSERT INTO ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE} (user_id, revenue)
            VALUES ('${event.userId}', ${eventOperator} ${event.value})
            ON CONFLICT (user_id)
            DO UPDATE SET revenue = ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE}.revenue ${eventOperator} ${event.value};
        `);
    }

    async getUserData(userId) {
        const result = await this._client.query(`
            SELECT * FROM ${DbHelper.SCHEMA_NAME}.${DbHelper.TABLE_NANE}
            WHERE user_id = '${userId}'
        `);
        return result.rows;
    }

    async beginTransaction() {
        await this._client.query("BEGIN");
    }

    async commitTransaction() {
        await this._client.query("COMMIT");
    }

    async rollbackTransaction() {
        await this._client.query("ROLLBACK");
    }

    async destroy() {
        if (this._client) {
            await this._client.end();
        }
    }
}

module.exports = {
    DbHelper,
    FsHelper
}

class DataProcessor {

    static async run() {
        let dbHelper;
        try {
            dbHelper = await DbHelper.create();
            await dbHelper.beginTransaction();
            await dbHelper.createTableAndIndexIfNotExists();   
            await DataProcessor._handleAllEvents(dbHelper);
            await dbHelper.commitTransaction();
        } catch (err) {
            if (dbHelper) {
                await dbHelper.rollbackTransaction();
            }
            throw err;
        } finally {
            if (dbHelper) {
                await dbHelper.destroy();
            }
        }
    }

    static _getEventOperator(event) {
        switch (event.name) {
            case "add_revenue":
                return "+";
            case "subtract_revenue":
                return "-";
            default:
                throw new Error("Invalid line");
        }
    }

    static async _handleSingleEvent(line, dbHelper) {

        // Convers string to JSON object.
        const event = JSON.parse(line);

        // Get operator (+/-) which relevant for value of current event.
        const eventOperator = DataProcessor._getEventOperator(event);

        // Update row of relevant user.
        await dbHelper.updateUserData(event, eventOperator);
    }

    static async _handleAllEvents(dbHelper) {
        await FsHelper.readFileLines(FsHelper.RECIEVED_EVENTS_FILE_PATH, async (line) => {
            // File's lines reader notify about current fetched line, create/update relevant row in DB.
            await DataProcessor._handleSingleEvent(line, dbHelper);
        }, true);
    }
}

// Current file may loaded due to invocation of other JS files (becuse they consume classes
// 'DbHelper' or 'FSHelper'), in such a case - do not invoke the method 'DataProcessor.run()'.
if (!process.argv[1].endsWith("data_processor.js")) {
    return;
}

console.info("Update started");
DataProcessor.run()
    .then(() => {
        console.info("Update ended");
    })
    .catch((err) => {
        console.error("Update failed", {err});
    })