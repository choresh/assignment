const pg = require("pg");
const { createReadStream } = require("node:fs");
const { createInterface } = require("node:readline");

const DATABASE_NAME = "public";
const PASSWORD ="postgres";
const USER = "postgres";
const SCHEMA_NAME = "public";
const TABLE_NANE = "users_revenue";
const FILE_PATH = "events.txt";


function getEventOperator(event) {
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

async function updateDbFromLine(line, client) {

    // Convers string to JSON object.
    const event = JSON.parse(line);

    // Get operator (+/-) which relevant for value of current event.
    const eventOperator = getEventOperator(event);

    // Lock the row for update, to prevent other transactions from modifying it.
    await client.query(`
        SELECT * FROM ${SCHEMA_NAME}.${TABLE_NANE}
        WHERE user_id = '${event.userId}'
        FOR UPDATE;
    `);

    // Perform 'upsert' (i.e. update or insert) of the relevnt row.
    await client.query(`
        INSERT INTO ${SCHEMA_NAME}.${TABLE_NANE} (user_id, revenue)
        VALUES ('${event.userId}', ${eventOperator} ${event.value})
        ON CONFLICT (user_id)
        DO UPDATE SET revenue = ${SCHEMA_NAME}.${TABLE_NANE}.revenue ${eventOperator} ${event.value};
    `);
}

async function updateDbFromFile(client) {

    // The 'linesReader' enable us to read the file content in 'line by line' manner.
    const linesReader = createInterface({
        input: createReadStream(FILE_PATH),
        crlfDelay: Infinity,
    });

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
            await updateDbFromLine(line, client);
        });
    });
}

async function connectToDb() {
    const client = new pg.Client({ database: DATABASE_NAME, password: PASSWORD, user: USER });
    await client.connect();
    return client;
}

async function createTableAndIndexIfNotExists(client) {
    await client.query(`CREATE TABLE IF NOT EXISTS ${SCHEMA_NAME}.${TABLE_NANE} (user_id varchar, revenue integer)`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS user_id_unique ON ${SCHEMA_NAME}.${TABLE_NANE} (user_id)`); // Index on 'user_id' will improve our SELECT clauses, and also - 'ON CONFLICT (user_id)' within the 'upsert' query (see method ' updateDbFromLine()') cannot work without such a definition.
}

async function update() {
    let client;
    try {
        client = await connectToDb();
        await client.query("BEGIN"); // Begin transaction.
        await createTableAndIndexIfNotExists(client);   
        await updateDbFromFile(client);
        await client.query("COMMIT"); // Commit transaction.
    } catch (err) {
        if (client) {
            await client.query("ROLLBACK"); // Rollback transaction.
        }
        throw err;
    } finally {
        if (client) {
            await client.end();
        }
    }
}

console.info("Update started");
update()
    .then(() => {
        console.info("Update ended");
    })
    .catch((err) => {
        console.error("Update failed", {err});
    })

