const helpers = require("./data_processor");
const axios = require("axios");

class Client {
    static async run() {
        await helpers.FsHelper.readFileLines(helpers.FsHelper.SRC_EVENTS_FILE_PATH, async (line) => {
            // File's lines reader notify about current fetched line, send relevant event to our server.
            console.info({line});
            const event = JSON.parse(line);
            axios.post("http://localhost:8000/liveEvent", event);
        });
    }
}

console.info("Client started");
Client.run()
    .then(() => {
        console.info("Client ended");
    })
    .catch((err) => {
        console.error("Client failed", {err});
    })
