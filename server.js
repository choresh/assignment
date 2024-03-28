const http = require("http");
const url = require("url");
const helpers = require("./data_processor");

async function addEvent(request) {
    console.log("Add event - started");
    return new Promise((resolve, reject) => {
        const body = [];
        request.on("data", (chunk) => {
            body.push(chunk);
        })
        .on("end", async () => {
            try {
                const event = JSON.parse(Buffer.concat(body).toString());
                if (!event.userId || !event.value || (event.name !== "add_revenue" && event.name !== "subtract_revenue")) {
                    const err = new Error("Invalid event data")
                    console.error("Add event - failed", {err});
                    reject(err);
                } else {
                    await helpers.FsHelper.storeEvent(event);
                    console.log("Add event - ended", {event});
                    resolve();
                }
            } catch (err) {
                console.error("Add event - failed", {err});
                reject(err);
            }
        });
    });
}

async function getEvent(eventId, dbHelper) {
    console.log("Get event - started", {eventId});
    const eventRows = await dbHelper.getEvent(eventId);
    console.log("Get event - ended", {eventRows});
    return eventRows;
}

http.createServer(async (req, res) => {
    const parsedUrl = url.parse(req.url);
    const pathTokens = parsedUrl.path.split("/");
    const dbHelper = await helpers.DbHelper.create();
    console.info(`method: '${req.method}', url: '${req.url}'`, {parsedUrl, pathTokens});
    switch (req.method) {
        case "GET":
            switch (pathTokens[1]) {
                case "userEvents":
                    const eventRows = await getEvent(pathTokens[2], dbHelper);
                    let resBody;
                    if (eventRows.length > 1) {
                        res.writeHead(500);
                        resBody = "";
                    } else if (eventRows.length === 0) {
                        resBody = "";
                    } else {
                        res.writeHead(200, {"Content-Type": "application/json"});
                        resBody = JSON.stringify(eventRows[0]);
                    }
                    res.end(resBody);
                    return;
                default:
                    // Do nothing here.
            }
            break;
        case "POST":
            switch (pathTokens[1]) {
                case "liveEvent":
                    try {
                        await addEvent(req);
                    } catch (err) {
                        res.writeHead(400);
                    }
                    res.end();
                    return;
                default:
                    // Do nothing here.
            }
            break;
        default:
            // Do nothing here.
    }
    res.writeHead(400);
    res.end();
}).listen(8000);