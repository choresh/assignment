const helpers = require("./data_processor");
const axios = require("axios");

class Client {
    static async run() {
        await helpers.FsHelper.readFileLines(helpers.FsHelper.SRC_EVENTS_FILE_PATH, async (line) => {
            // File's lines reader notify about current fetched line, send relevant event to our server.
            const event = JSON.parse(line);
            try {
                await axios.post("http://localhost:8000/liveEvent", event, {headers: {Authorization: "secret"}});
            } catch {
                throw new Error("Server is unreachable");
            }
        });
    }
}

console.info("Client started");
Client.run()
    .then(() => {
        console.info("Client ended");
    })
    .catch((err) => {
        console.error("Client failed", {message: err.message});
    })
