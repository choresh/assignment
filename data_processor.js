const pg = require("pg");
const { once } = require("node:events");
const { createReadStream } = require("node:fs");
const { createInterface } = require("node:readline");

const SCHEMA_NAME = "public";
const TABLE_NANE = "users_revenue";
const FILE_PATH = "events.txt"

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

    console.log("ZZZ-3", { event });

    // Perform 'upsert' (i.e. update or insert) of the relevnt row.
    await client.query(`
        INSERT INTO ${SCHEMA_NAME}.${TABLE_NANE} (user_id, revenue)
        VALUES ('${event.userId}', ${eventOperator} ${event.value})
        ON CONFLICT (user_id)
        DO UPDATE SET revenue = ${SCHEMA_NAME}.${TABLE_NANE}.revenue ${eventOperator} ${event.value};
    `);

    console.log("ZZZ-4", { event });
}

async function updateDbFromFile(client) {

    // The 'linesReader' enable us to read the file content in 'line by line' manner.
    const linesReader = createInterface({
        input: createReadStream(FILE_PATH),
        crlfDelay: Infinity,
    });

    // Deal with events of the lines reader.
    return new Promise((resolve, reject) => {
        linesReader.on("close", () => {
            // resolve(); // File's lines reading ended, resolve the promise.
        });
        linesReader.on("error", (err) => {
            reject(err); // File's lines reading failed, reject the promise.
        });
        linesReader.on("line", async (line) => {  // File's lines reader notify about current fetched line.
            await updateDbFromLine(line, client);
        });
    });
}

async function connectToDb() {
    client = new pg.Client({ database: "public", password: "postgres", user: "postgres" });
    await client.connect();
    return client;
}

async function createTableAndIndexIfNotExists(client) {
    await client.query(`CREATE TABLE IF NOT EXISTS ${SCHEMA_NAME}.${TABLE_NANE} (user_id varchar, revenue integer)`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS user_id_unique ON ${SCHEMA_NAME}.${TABLE_NANE} (user_id)`);
}

async function update() {
    let client;
    try {
        client = await connectToDb();
        // await client.query("BEGIN")
        await createTableAndIndexIfNotExists(client);   
        await updateDbFromFile(client);
        console.log("ZZZ-2");
        // await client.query("COMMIT")
    } catch (err) {
        if (client) {
            // await client.query("ROLLBACK")
        }
        throw err;
    } finally {
        if (client) {
            await client.end();
        }
    }
}

console.info("Update will started");
update()
    .then(() => {
        console.info("Update was ended");
    })
    .catch((err) => {
        console.error("Update was failed", {err});
    })

