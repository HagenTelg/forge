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
        const date = new Date(Date.UTC(
            parseInt(parts[0]),
            (parseInt(parts[1]) || 1) - 1,
            parseInt(parts[2]) || 1,
            parseInt(parts[3]) || 0,
            parseInt(parts[4]) || 0,
            parseInt(parts[5]) || 0,
            parseInt(parts[6]) || 0
        ));
        return date.getTime();
    }

    let activeRecordStreams = 0;
    let activeRecordStalled = undefined;
    
    DataSocket.RecordStream = class extends DataSocket.Stream {
        constructor(dataName) {
            super(dataName);
        }

        startOfData() {
            activeRecordStreams += 1;
            DataSocket.onActiveRecordUpdate(activeRecordStreams, activeRecordStalled);
        }

        endOfData() {
            activeRecordStreams -= 1;
            DataSocket.onActiveRecordUpdate(activeRecordStreams, activeRecordStalled);
        }
    
        incomingDataContent(content) {
            function toDataView(str) {
                return new DataView(Uint8Array.from(atob(str), c => c.charCodeAt(0)).buffer);
            }
            function unpackArray(view, elementSize, unpacker) {
                const total = view.byteLength / elementSize;
                let result = [];
                for (let i=0; i < total; i++) {
                    const offset = i * elementSize;
                    result.push(unpacker(view, offset));
                }
                return result;
            }

            const epochOrigin = content.time.origin;
            const timeCount = content.time.count;
            const timeOffset = toDataView(content.time.offset);
            let epoch;
            if (timeOffset.byteLength <= timeCount * 4) {
                epoch = unpackArray(timeOffset, 4, (view, offset) => {
                    return view.getInt32(offset, true);
                });
            } else {
                epoch = unpackArray(timeOffset, 8, (view, offset) => {
                    return Number(view.getBigInt64(offset, true));
                });
            }
            let plotTime = [];
            epoch.forEach(function(_, index, target) {
                target[index] += epochOrigin;
                plotTime.push(DataSocket.toPlotTime(target[index]));
            });
    
            const fields = content.data;
            const fieldOutput = new Map();
            for (const fieldName of Object.keys(fields)) {
                const fieldRaw = fields[fieldName];
                if (Array.isArray(fieldRaw)) {
                    fieldRaw.forEach(function(_, index, target) {
                        const value = target[index];
                        if (value === undefined || value === null) {
                            target[index] = Number.NaN;
                        } else if (typeof value === 'number' && !isFinite(value)) {
                            target[index] = Number.NaN;
                        }
                    });
                    fieldOutput.set(fieldName, fieldRaw);
                    continue;
                }

                if (typeof fieldRaw === 'string') {
                    fieldOutput.set(fieldName, unpackArray(toDataView(fieldRaw), 4, (view, offset) => {
                        return view.getFloat32(offset, true);
                    }));
                    continue;
                }

                if (fieldRaw.type === 'array') {
                    let values = [];
                    for (let timeIndex=0; timeIndex < fieldRaw.values.length; timeIndex++) {
                        values.push(unpackArray(toDataView(fieldRaw.values[timeIndex]), 4, (view, offset) => {
                            return view.getFloat32(offset, true);
                        }));
                    }
                    fieldOutput.set(fieldName, values);
                    continue;
                }

                fieldOutput.set(fieldName, undefined);
            }

            this.processRecord(fieldOutput, epoch, plotTime);

            fieldOutput.forEach((values, fieldName) => {
                this.incomingData(fieldName, plotTime, values, epoch);
            });
        }

        processRecord(record, epoch, plotTime) {}
    
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
        } else if (reply.type === "stalled") {
            if (!reply.stalled) {
                activeRecordStalled = undefined;
            } else {
                activeRecordStalled = reply.reason;
                if (!activeRecordStalled) {
                    activeRecordStalled = "Waiting for data";
                }
            }
            DataSocket.onActiveRecordUpdate(activeRecordStreams, activeRecordStalled);
        }
    });

    DataSocket.RecordDispatch = class extends DataSocket.RecordStream {
        constructor(dataName) {
            super(dataName);
            this.fieldToCallbacks = new Map();
            this.callOnFinished = [];
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

        endOfData() {
            super.endOfData();
            this.callOnFinished.forEach((cb) => {
                cb();
            });
        }

        attach(field, callback, finished) {
            let cbs = this.fieldToCallbacks.get(field);
            if (cbs === undefined) {
                cbs = [];
                this.fieldToCallbacks.set(field, cbs);
            }
            cbs.push(callback);
            if (finished) {
                this.callOnFinished.push(finished);
            }
        }
    };
    
    const loadingRecords = new Map();
    DataSocket.onRecordReload = () => {};
    DataSocket.resetLoadedRecords = function() {
        loadingRecords.forEach((dispatch) => {
            dispatch.stopStream();
        });
        loadingRecords.clear();
        activeRecordStreams = 0;
        activeRecordStalled = undefined;
        DataSocket.onActiveRecordUpdate(0);
        DataSocket.onRecordReload = () => {};
    };
    DataSocket.addLoadedRecord = function(dataName, loader, ...args) {
        let dispatch = loadingRecords.get(dataName);
        if (dispatch === undefined) {
            dispatch = loader(dataName);
            loadingRecords.set(dataName, dispatch);
        }
        dispatch.attach(...args);
        return dispatch;
    };
    DataSocket.addLoadedRecordField = function(dataName, field, callback, loader, finished) {
        if (loader === undefined) {
            loader = (dataName) => { return new DataSocket.RecordDispatch(dataName); };
        }
        return DataSocket.addLoadedRecord(dataName, loader, field, callback, finished);
    }
    DataSocket.startLoadingRecords = function() {
        loadingRecords.forEach((dispatch) => {
            dispatch.beginStream();
        });
    };
    DataSocket.onActiveRecordUpdate = (count, stalled) => {};

    DataSocket.reloadData = function() {
        loadingRecords.forEach((dispatch) => {
            dispatch.stopStream();
        });
        DataSocket.onRecordReload();
        loadingRecords.forEach((dispatch) => {
            dispatch.beginStream();
        });
    };
    TimeSelect.onChanged(DataSocket, DataSocket.reloadData);
})();

