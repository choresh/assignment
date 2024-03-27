var http = require('http');
var url = require("url");

async function addEvent() {
    console.log("Add event - started")
}


async function getEvent(eventId) {
    console.log("Get event - started", { eventId})
}

http.createServer(async (req, res) => {
    const parsedUrl = url.parse(req.url);
    const pathTokens = parsedUrl.path.split('/')
    console.info(`method: '${req.method}', url: '${req.url}'`, { parsedUrl, pathTokens })
    switch (req.method) {
        case "GET":
            switch (pathTokens[1]) {
                case "liveEvent":
                    await getEvent(pathTokens[2]);
                    break;
                default:
                    // Do nothing
            }
            break;
        case "POST":
            switch (pathTokens[1]) {
                case "liveEvent":
                    await addEvent();
                    break;
                default:
                    // Do nothing
            }
            break;
        default:
            // Do nothing
    }
    res.end();
}).listen(8000);