const http = require("http");
const url = require("url");
const helpers = require("./data_processor");

async function addEvent(request) {
    return new Promise((resolve, reject) => {
        const body = [];
        request.on("data", (chunk) => {
            body.push(chunk);
        })
        .on("end", async () => {
            try {
                const event = JSON.parse(Buffer.concat(body).toString());
                if (!event.userId || !event.value || (event.name !== "add_revenue" && event.name !== "subtract_revenue")) {
                    reject(new Error("Invalid event data"));
                } else {
                    await helpers.FsHelper.storeEvent(event);
                    resolve();
                }
            } catch (err) {
                reject(err);
            }
        });
    });
}

async function getUserData(userId, dbHelper) {
    return await dbHelper.getUserData(userId);
}

http.createServer(async (req, res) => {
    const parsedUrl = url.parse(req.url);
    const pathTokens = parsedUrl.path.split("/");
    const dbHelper = await helpers.DbHelper.create();
    switch (req.method) {
        case "GET":
            switch (pathTokens[1]) {
                case "userEvents":
                    const userRows = await getUserData(pathTokens[2], dbHelper);
                    let resBody;
                    if (userRows.length > 1) {
                        res.writeHead(500);
                        resBody = "";
                    } else if (userRows.length === 0) {
                        resBody = "";
                    } else {
                        res.writeHead(200, {"Content-Type": "application/json"});
                        resBody = JSON.stringify(userRows[0]);
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
                    if (req.headers.authorization !== "secret") {
                        res.writeHead(401);
                    } else {
                        try {
                            await addEvent(req);
                        } catch (err) {
                            res.writeHead(400);
                        }
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