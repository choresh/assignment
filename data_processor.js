const pg = require("pg");

async function update() {
    const client = new pg.Client({password: "postgres", user: "postgres"});
    await client.connect();
}

console.info("Update will started");
update()
    .then(() => {
        console.info("Update was ended");
    })
    .catch((err) => {
        console.error("Update was failed", {err});
    })