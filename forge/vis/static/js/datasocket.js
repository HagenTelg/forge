let DataSocket = {};
(function() {
    const serverSocket = new WebSocket(DATASOCKET_URL);
    let streamSequenceNumber = 0;
    const waitingForConnected = new Map();
    const activeStreams = new Map();
    
    DataSocket.Stream = class {
        constructor(dataName) {
            this.dataName = dataName;
            this._streamID = undefined;
        }
    
        startOfData() {}
    
        endOfData() {}
    
        incomingDataContent(content) {}
    
        beginStream() {
            if (this._streamID !== undefined) {
                return;
            }
            this._streamID = streamSequenceNumber;
            streamSequenceNumber += 1;
    
            this.startOfData();
    
            const stream = this;
            const startStreaming = function() {
                if (stream._streamID === undefined) {
                    return;
                }
                activeStreams.set(stream._streamID, stream);
                serverSocket.send(JSON.stringify({
                    action: 'start',
                    stream: stream._streamID,
                    data: stream.dataName,
                    start_epoch_ms: TimeSelect.start_ms,
                    end_epoch_ms: TimeSelect.end_ms,
                }));
            }
    
            if (serverSocket.readyState === 0) {
                waitingForConnected.set(this._streamID, startStreaming);
            } else {
                startStreaming();
            }
        }
    
        stopStream() {
            const id = this._streamID;
            this._streamID = undefined;
            if (id !== undefined) {
                this.endOfData();
            }
    
            waitingForConnected.delete(id);
            if (!activeStreams.delete(id)) {
                return;
            }
            if (serverSocket.readyState !== 1) {
                return;
            }
            serverSocket.send(JSON.stringify({
                action: 'stop',
                stream: id,
            }));
        }
    };
    
    DataSocket.toPlotTime = function(epoch_ms) {
        const date = new Date(Math.floor(epoch_ms));
        return date.getUTCFullYear().toString().padStart(4, '0') + '-' +
            (date.getUTCMonth()+1).toString().padStart(2, '0') + '-' +
            date.getUTCDate().toString().padStart(2, '0') + ' ' +
            date.getUTCHours().toString().padStart(2, '0') + ':' +
            date.getUTCMinutes().toString().padStart(2, '0') + ':' +
            date.getUTCSeconds().toString().padStart(2, '0');
    }
    const rePlotTimeSplit = /[\s.:TZtz-]/;
    DataSocket.fromPlotTime = function(plot_time) {
        if (!plot_time) {
            return undefined;
        }
        const parts = plot_time.trim().split(rePlotTimeSplit);
        let date = new Date();
        date.setUTCFullYear(parseInt(parts[0]));
        date.setUTCMonth(parseInt(parts[1])-1);
        date.setUTCDate(parseInt(parts[2]));
        date.setUTCHours(parseInt(parts[3]));
        date.setUTCMinutes(parseInt(parts[4]));
        date.setUTCSeconds(parseInt(parts[5]));
        date.setUTCMilliseconds(parseInt(parts[6]));
        return date.getTime();
    }
    
    DataSocket.RecordStream = class extends DataSocket.Stream {
        constructor(dataName) {
            super(dataName);
        }
    
        incomingDataContent(content) {
            let epoch = content.time.offset;
            let delta = content.time.delta;
            let plotTime = [];
            const epochOrigin = content.time.origin;
            epoch.forEach(function(_, index, target) {
                target[index] += epochOrigin + index * delta;
                plotTime.push(DataSocket.toPlotTime(target[index]));
            });
    
            const fields = content.data;
            for (const fieldName of Object.keys(fields)) {
                const fieldRaw = fields[fieldName];
                if (Array.isArray(fieldRaw)) {
                    fieldRaw.forEach(function(_, index, target) {
                        const value = target[index];
                        if (value === undefined || value === null || !isFinite(value)) {
                            target[index] = Number.NaN;
                        }
                    });
                    this.incomingData(fieldName, plotTime, fieldRaw, epoch);
                    continue;
                }
    
                let fieldValues = fieldRaw.offset;
                const fieldOrigin = fieldRaw.origin;
                fieldValues.forEach(function(_, index, target) {
                    let value = target[index];
    
                    if (value === undefined || value === null || !isFinite(value)) {
                        target[index] = Number.NaN;
                        return;
                    }
    
                    value += fieldOrigin;
                    target[index] = value;
                });
    
                this.incomingData(fieldName, plotTime, fieldValues, epoch);
            }
        }
    
        incomingData(fieldName, plotTime, values, epoch) {}
    };
    
    serverSocket.addEventListener('open', (event) => {
        waitingForConnected.forEach((cb) => {
            cb();
        });
        waitingForConnected.clear();
    });
    
    serverSocket.addEventListener('message', (event) => {
        const reply = JSON.parse(event.data);
        if (reply.type === "end") {
            const index = reply.stream;
            const stream = activeStreams.get(index)
            activeStreams.delete(index);
            if (stream) {
                stream._streamID = undefined;
                stream.endOfData();
            }
        } else if (reply.type === "data") {
            const target = activeStreams.get(reply.stream);
            if (target === undefined) {
                return;
            }
            target.incomingDataContent(reply.content);
        }
    });

    const RecordDispatch = class extends DataSocket.RecordStream {
        constructor(dataName) {
            super(dataName);
            this.fieldToCallbacks = new Map();
        }

        incomingData(fieldName, plotTime, values, epoch) {
            const cbs = this.fieldToCallbacks.get(fieldName);
            if (cbs === undefined) {
                return;
            }
            cbs.forEach((cb) => {
                cb(plotTime, values, epoch);
            });
        }
    };
    
    const loadingRecords = new Map();
    DataSocket.onRecordReload = () => {};
    DataSocket.resetLoadedRecords = function() {
        loadingRecords.forEach((dispatch) => {
            dispatch.stopStream();
        });
        loadingRecords.clear();
        DataSocket.onRecordReload = () => {};
    };
    DataSocket.addLoadedRecordField = function(dataName, field, callback) {
        let dispatch = loadingRecords.get(dataName);
        if (dispatch === undefined) {
            dispatch = new RecordDispatch(dataName);
            loadingRecords.set(dataName, dispatch);
        }
        let cbs = dispatch.fieldToCallbacks.get(field);
        if (cbs === undefined) {
            cbs = [];
            dispatch.fieldToCallbacks.set(field, cbs);
        }
        cbs.push(callback);
    }
    DataSocket.startLoadingRecords = function() {
        loadingRecords.forEach((dispatch) => {
            dispatch.beginStream();
        });
    };

    TimeSelect.onChanged(DataSocket, () => {
        loadingRecords.forEach((dispatch) => {
            dispatch.stopStream();
            DataSocket.onRecordReload();
            dispatch.beginStream();
        });
    });
})();

