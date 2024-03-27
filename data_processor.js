const pg = require("pg");
const { createReadStream } = require("node:fs");
const { createInterface } = require("node:readline");

class FsFacade {
    static FILE_PATH = "events.txt";

    static createLinesReader() {
        return createInterface({
            input: createReadStream(FsFacade.FILE_PATH),
            crlfDelay: Infinity,
        });
    }
}

class DbFacade {
    static DATABASE_NAME = "public";
    static PASSWORD ="postgres";
    static USER = "postgres";
    static SCHEMA_NAME = "public";
    static TABLE_NANE = "users_revenue";
    
    static async create() {
        let instance = new DbFacade()
        instance._client = new pg.Client({ database: DbFacade.DATABASE_NAME, password: DbFacade.PASSWORD, user: DbFacade.USER });
        await instance._client.connect();
        return instance;
    }

    async createTableAndIndexIfNotExists() {
        await this._client.query(`CREATE TABLE IF NOT EXISTS ${DbFacade.SCHEMA_NAME}.${DbFacade.TABLE_NANE} (user_id varchar, revenue integer)`);
        await this._client.query(`CREATE UNIQUE INDEX IF NOT EXISTS user_id_unique ON ${DbFacade.SCHEMA_NAME}.${DbFacade.TABLE_NANE} (user_id)`); // Index on 'user_id' will improve our SELECT clauses, and also - 'ON CONFLICT (user_id)' within the 'upsert' query (see method ' updateDbFromLine()') cannot work without such a definition.
    }

    async updateDbRow(event, eventOperator) {
    
        // Lock the row for update, to prevent other transactions from modifying it.
        await this._client.query(`
            SELECT * FROM ${DbFacade.SCHEMA_NAME}.${DbFacade.TABLE_NANE}
            WHERE user_id = '${event.userId}'
            FOR UPDATE;
        `);
    
        // Perform 'upsert' (i.e. update or insert) of the relevnt row.
        await this._client.query(`
            INSERT INTO ${DbFacade.SCHEMA_NAME}.${DbFacade.TABLE_NANE} (user_id, revenue)
            VALUES ('${event.userId}', ${eventOperator} ${event.value})
            ON CONFLICT (user_id)
            DO UPDATE SET revenue = ${DbFacade.SCHEMA_NAME}.${DbFacade.TABLE_NANE}.revenue ${eventOperator} ${event.value};
        `);
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

class DataProcessor {

    static async update() {
        let dbFacade;
        try {
            dbFacade = await DbFacade.create();
            await dbFacade.beginTransaction();
            await dbFacade.createTableAndIndexIfNotExists();   
            await DataProcessor._updateDbFromFile(dbFacade);
            await dbFacade.commitTransaction();
        } catch (err) {
            if (dbFacade) {
                await dbFacade.rollbackTransaction();
            }
            throw err;
        } finally {
            if (dbFacade) {
                await dbFacade.destroy();
            }
        }
    }

    static _getEventOperator(event) {
        switch (event.name) {
            case "add_revenue":
                return '+';
            case "subtract_revenue":
                return '-';
            default:
                console.error("Invalid line", { event });
                throw new Error("Invalid line");
        }
    }

    static async _updateDbFromLine(line, dbFacade) {

        // Convers string to JSON object.
        const event = JSON.parse(line);

        // Get operator (+/-) which relevant for value of current event.
        const eventOperator = DataProcessor._getEventOperator(event);

        await dbFacade.updateDbRow(event, eventOperator);
    }

    static async _updateDbFromFile(dbFacade) {

        // The 'linesReader' enable us to read the file content in 'line by line' manner.
        const linesReader = FsFacade.createLinesReader();

        // Deal with events of the lines reader.
        return new Promise(async (resolve, reject) => {
            linesReader.on("close", () => {
                // * File's lines reading ended, resolve the promise.
                // * The 'setTimeout()' is workaround, to deal with bizarre behavior of the lines reader ('close' event triggered before last 'line' event).
                setTimeout(resolve, 1000);
            });
            linesReader.on("error", (err) => {
                // File's lines reading failed, reject the promise.
                reject(err);
            });
            linesReader.on("line", async (line) => { 
                // File's lines reader notify about current fetched line, create/update relevant row in DB.
                await DataProcessor._updateDbFromLine(line, dbFacade);
            });
        });
    }
}

console.info("Update started");
DataProcessor.update()
    .then(() => {
        console.info("Update ended");
    })
    .catch((err) => {
        console.error("Update failed", {err});
    })

