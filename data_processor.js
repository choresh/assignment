const pg = require("pg");
const SCHEMA_NAME = "public";
const TABLE_NANE = "users_revenue";

async function update() {
    const client = new pg.Client({password: "postgres", user: "postgres"});
    await client.connect();

    const res = await client.query(`CREATE TABLE IF NOT EXISTS ${SCHEMA_NAME}.${TABLE_NANE} (user_id varchar, revenue integer)`)
    console.log("Table was created if not exists yet", { res });
    await client.end()
}

console.info("Update will started");
update()
    .then(() => {
        console.info("Update was ended");
    })
    .catch((err) => {
        console.error("Update was failed", {err});
    })