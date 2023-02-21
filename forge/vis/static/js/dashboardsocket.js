let DashboardSocket = {};
(function() {
    const waitingForConnected = new Map();
    let serverSocket = undefined;


    const listResults = new Set();
    function issueList() {
        serverSocket.send(JSON.stringify({
            type: 'list',
        }));
    }
    DashboardSocket.listEntries = function(result) {
        const listPending = listResults.size !== 0;
        listResults.add(result);
        if (listPending) {
            return;
        }

        if (serverSocket.readyState === 0) {
            waitingForConnected.set(issueList, issueList);
        } else {
            issueList();
        }
    }


    const statusResults = new Set();
    function issueStatus() {
        const nextIssue = statusResults.values().next().value;
        if (!nextIssue) {
            return;
        }

        serverSocket.send(JSON.stringify({
            type: 'status',
            station: nextIssue.station,
            code: nextIssue.code,
            start_epoch_ms: nextIssue.startTime,
        }));
    }
    DashboardSocket.Status = class {
        constructor(station, code) {
            this.station = station.toLowerCase();
            this.code = code.toLowerCase();
        }

        get startTime() {
            return Date.now() - 24 * 60 * 60 * 1000;
        }

        load() {
            const statusPending = statusResults.size !== 0;
            statusResults.add(this);
            if (statusPending) {
                return;
            }

            if (serverSocket.readyState === 0) {
                waitingForConnected.set(issueStatus, issueStatus);
            } else {
                issueStatus();
            }
        }

        cancel() {
            statusResults.delete(this);
        }

        updated(data) {}
    };

    let socketReopenTimer = undefined;
    let socketIsConnected = false;
    function socketOpen(event) {
        if (socketReopenTimer) {
            clearInterval(socketReopenTimer);
            socketReopenTimer = undefined;
        }
        socketIsConnected = true;

        waitingForConnected.forEach((cb) => {
            cb();
        });
        waitingForConnected.clear();
    }

    function socketMessage(event) {
        const reply = JSON.parse(event.data);
        if (reply.type === 'list') {
            const entries = reply.entries;
            for (const cb of listResults) {
                cb(entries);
            }
            listResults.clear();
        } else if (reply.type === 'status') {
            const station = reply.station;
            const code = reply.code;
            const data = reply.status;
            for (const entry of statusResults) {
                if (entry.station !== station) {
                    continue;
                }
                if (entry.code !== code) {
                    continue;
                }
                entry.updated(data);
                statusResults.delete(entry);
                break;
            }

            issueStatus();
        }
    }

    function disconnectSocket() {
        if (serverSocket) {
            serverSocket.removeEventListener('open', socketOpen);
            serverSocket.removeEventListener('close', socketClose);
            serverSocket.removeEventListener('message', socketMessage);
            try {
                serverSocket.close();
            } catch (e) {
            }
        }
        serverSocket = undefined;

        socketIsConnected = false;
    }

    function socketClose(event) {
        const wasConnected = socketIsConnected;
        if (socketReopenTimer) {
            clearInterval(socketReopenTimer);
            socketReopenTimer = undefined;
        }
        disconnectSocket();
        socketReopenTimer = setInterval(connectToServer, 30 * 1000);
        if (wasConnected) {
            setTimeout(connectToServer, 1000);
        }
    }

    function connectToServer() {
        disconnectSocket();
        try {
            serverSocket = new WebSocket(DASHBOARD_SOCKET_URL);
        } catch (e) {
            return;
        }
        serverSocket.addEventListener('open', socketOpen);
        serverSocket.addEventListener('close', socketClose);
        serverSocket.addEventListener('message', socketMessage);
    }

    socketReopenTimer = setInterval(connectToServer, 30 * 1000);
    connectToServer();
})();

