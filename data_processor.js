const pg = require("pg");
const { once } = require("node:events");
const { createReadStream } = require("node:fs");
const { createInterface } = require("node:readline");

const SCHEMA_NAME = "public";
const TABLE_NANE = "users_revenue";
const FILE_PATH = "events.txt"

async function readLineByLine(path, lineHandler) {
    const rl = createInterface({
        input: createReadStream(path),
        crlfDelay: Infinity,
    });
    rl.on("line", async (line) => {
        await lineHandler(line)
    });
    await once(rl, "close");
}

async function update() {
    let client;
    try {
        client = new pg.Client({password: "postgres", user: "postgres"});
        await client.connect();

        await client.query("BEGIN")

        const res = await client.query(`CREATE TABLE IF NOT EXISTS ${SCHEMA_NAME}.${TABLE_NANE} (user_id varchar, revenue integer)`)
        console.log("Table was created if not exists yet", { res });

        await readLineByLine(FILE_PATH, async (line) => {
            const lineObj = JSON.parse(line);
            console.info("Curr line:", lineObj)
            let revenueChange;
            switch(lineObj.name) {
                case "add_revenue":
                    break;
                case "subtract_revenue":
                    break;
                default:
                    console.error("Invalid line:", lineObj);
                    return; // Ignore the error.
            }
            const res = await client.query(`
                UPDATE ${SCHEMA_NAME}.${TABLE_NANE}
                SET usr_score = revenue + {}
                WHERE user_id = '${lineObj.userId}';`
            );
        });

        await client.query("COMMIT")
    } catch (err) {
        if (client) {
            await client.query("ROLLBACK")
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