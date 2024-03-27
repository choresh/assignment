const http = require('http');
const url = require("url");
const fs = require("fs")
const fsPromises = fs.promises;
const FILE_PATH = "events.txt"

async function addEvent(request) {
    console.log("Add event - started")
    return new Promise((resolve, reject) => {
        const  body = [];
        request.on('data', (chunk) => {
            body.push(chunk);
        })
        .on('end', async () => {
            const bodyJson = JSON.parse(Buffer.concat(body).toString());
            await fsPromises.appendFile(FILE_PATH, JSON.stringify(bodyJson), {encoding: "utf-8"})
            console.log("Add event - ended", {bodyJson})
            resolve();
        });
    });
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
                    await addEvent(req);
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