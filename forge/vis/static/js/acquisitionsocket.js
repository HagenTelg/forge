let AcquisitionSocket = {};
(function() {
    let socket = undefined;
    let socketReopenTimer = undefined;

    let messageLogDone = undefined;

    function callContextOnWeakSet(set, call) {
         set.forEach((ref) => {
            const context = ref.deref();
            if (!context) {
                set.delete(ref);
                return;
            }
            call(context);
         });
    }

    const stateContexts = new Set();
    let socketIsConnected = false;
    function socketOpen(event) {
        if (socketReopenTimer) {
            clearInterval(socketReopenTimer);
            socketReopenTimer = undefined;
        }

        socketIsConnected = true;
        callContextOnWeakSet(stateContexts, (context) => {
            context._incomingConnectionState(true);
        });
    }

    function disconnectSocket() {
        if (socket) {
            socket.removeEventListener('open', socketOpen);
            socket.removeEventListener('close', socketClose);
            socket.removeEventListener('message', socketData);
            try {
                socket.close();
            } catch (e) {}
        }
        socket = undefined;

        if (socketIsConnected) {
            callContextOnWeakSet(stateContexts, (context) => {
                context._incomingConnectionState(false);
            });
        }
        socketIsConnected = false;

        if (messageLogDone) {
            messageLogDone();
        }
        messageLogDone = undefined;
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
            socket = new WebSocket(ACQUISITION_SOCKET_URL);
        } catch (e) {
            return;
        }
        socket.addEventListener('open', socketOpen);
        socket.addEventListener('close', socketClose);
        socket.addEventListener('message', socketData);
    }
    socketReopenTimer = setInterval(connectToServer, 30 * 1000);
    connectToServer();


    function socketData(event) {
        const message = JSON.parse(event.data);
        if (message.type === 'data') {
            const source = message.source;
            const values = new Map();

            const messageValues = message.values;
            if (messageValues) {
                for (const fieldName of Object.keys(messageValues)) {
                    let v = messageValues[fieldName];
                    if (v === null) {
                        v = undefined;
                    }
                    values.set(fieldName, v);
                }
            }

            function toDataView(str) {
                return new DataView(Uint8Array.from(atob(str), c => c.charCodeAt(0)).buffer);
            }

            const simple = message.simple;
            if (simple) {
                const fields = simple.fields;
                const data = toDataView(simple.values);
                for (let i=0; i<fields.length; i++) {
                    const offset = i * 4;
                    const value = data.getFloat32(offset, true);
                    values.set(fields[i], value);
                }
            }

            const array = message.array;
            if (array) {
                const fields = array.fields;
                const contents = array.contents;
                for (let i=0; i<fields.length; i++) {
                    const data = toDataView(contents[i]);
                    const decoded = [];
                    values.set(fields[i], decoded);

                    const total = data.byteLength / 4;
                    for (let j=0; j<total; j++) {
                        const offset = j * 4;
                        const value = data.getFloat32(offset, true);
                        decoded.push(value);
                    }
                }
            }

            dispatchData(source, values);
        } else if (message.type === 'instrument_add') {
            dispatchInstrumentAdd(message.source, message.info);
        } else if (message.type === 'instrument_update') {
            const source = message.source;
            dispatchInstrumentRemove(source);
            dispatchInstrumentAdd(source, message.info);
        } else if (message.type === 'instrument_remove') {
            const source = message.source;
            sourceDataCache.delete(source);
            instrumentsPresent.delete(source);
            instrumentsState.delete(source);
            dispatchInstrumentRemove(source);
        } else if (message.type === 'instrument_state') {
            dispatchInstrumentState(message.source, message.state);
        } else if (message.type === 'acknowledge_message_log') {
            if (messageLogDone) {
                messageLogDone(message.result);
            }
            messageLogDone = undefined;
        } else if (message.type === 'event_log') {
            const source = message.source;
            dispatchEventLog(source, message.event);
        } else if (message.type === 'chat') {
            dispatchChatMessage(message.epoch_ms, message.from, message.message);
        }
    }


    const sourceDataCache = new Map();
    const dataTargetContexts = new Set();
    function dispatchData(source, values) {
        if (source) {
            let cacheTarget = sourceDataCache.get(source);
            if (!cacheTarget) {
                cacheTarget = new Map();
                sourceDataCache.set(source, cacheTarget);
            }
            const now = Date.now();
            values.forEach((value, fieldName) => {
                let valueTarget = cacheTarget.get(fieldName);
                if (!valueTarget) {
                    valueTarget = {};
                    cacheTarget.set(fieldName, valueTarget);
                }
                valueTarget.value = value;
                valueTarget.updated = now;
            });
        }

        callContextOnWeakSet(dataTargetContexts, (context) => {
            context._incomingData(source, values);
        });
    }


    const instrumentsPresent = new Map();
    const instrumentContexts = new Set();
    function dispatchInstrumentAdd(source, info) {
        instrumentsPresent.set(source, info);
        callContextOnWeakSet(instrumentContexts, (context) => {
            context._incomingInstrumentAdd(source, info);
        });
    }
    function dispatchInstrumentRemove(source) {
        callContextOnWeakSet(instrumentContexts, (context) => {
            context._incomingInstrumentRemove(source);
        });
    }
    const instrumentsState = new Map();
    function dispatchInstrumentState(source, state) {
        instrumentsState.set(source, state);
        callContextOnWeakSet(instrumentContexts, (context) => {
            context._incomingInstrumentState(source, state);
        });
    }


    const eventLogContexts = new Set();
    function dispatchEventLog(source, event) {
        callContextOnWeakSet(eventLogContexts, (context) => {
            context._incomingEventLog(source, event);
        });
    }


    const chatMessageContexts = new Set();
    function dispatchChatMessage(epoch_ms, from, message) {
        if (!from || !message) {
            return;
        }
        if (!isFinite(epoch_ms)) {
            epoch_ms = Date.now();
        }
        callContextOnWeakSet(chatMessageContexts, (context) => {
            context._incomingChatMessage(epoch_ms, from, message);
        });
    }


    AcquisitionSocket.writeMessageLog = function(author, text, ondone) {
        if (messageLogDone) {
            messageLogDone();
            messageLogDone = undefined;
        }

        if (!socket) {
            if (ondone) {
                ondone();
            }
            return;
        }

        messageLogDone = ondone;
        socket.send(JSON.stringify({
            'type': 'write_message_log',
            'author': author,
            'text': text,
        }));
    }
    AcquisitionSocket.sendRestartRequest = function() {
        if (!socket) {
            return;
        }

        socket.send(JSON.stringify({
            'type': 'restart_acquisition',
        }));
    }
    AcquisitionSocket.sendCommand = function(target, command, data) {
        if (!socket) {
            return;
        }

        socket.send(JSON.stringify({
            'type': 'command',
            'target': target,
            'command': command,
            'data': data,
        }));
    }
    AcquisitionSocket.sendChatMessage = function(from, message) {
        if (!socket) {
            return false;
        }

        socket.send(JSON.stringify({
            'type': 'chat',
            'from': from,
            'message': message,
        }));
        return true;
    }
    AcquisitionSocket.sendSetBypass = function(bypassed) {
        if (!socket) {
            return;
        }

        socket.send(JSON.stringify({
            'type': 'set_bypass',
            'bypassed': bypassed,
        }));
    }

    AcquisitionSocket.DispatchContext = class {
        constructor() {
            this._ref = undefined;
            this._detached = false;

            this._dataAnyDispatch = [];
            this._dataSourceDispatch = new Map();

            this._connectionStateDispatch = [];

            this._instrumentAddAnyDispatch = [];
            this._instrumentAddSourceDispatch = new Map();
            this._instrumentRemoveAnyDispatch = [];
            this._instrumentRemoveSourceDispatch = new Map();
            this._instrumentStateAnyDispatch = [];
            this._instrumentStateSourceDispatch = new Map();

            this._eventLogDispatch = [];

            this._chatMessageDispatch = [];
        }

        detach() {
            this._detached = true;

            if (!this._ref) {
                return;
            }
            stateContexts.delete(this._ref);
            dataTargetContexts.delete(this._ref);
            instrumentContexts.delete(this._ref);
        }

        get _context() {
            if (!this._ref) {
                this._ref = new WeakRef(this);
            }
            return this._ref;
        }

        _incomingData(source, values) {
            this._dataAnyDispatch.forEach((cb) => {
                cb(source, values);
            });
            const targets = this._dataSourceDispatch.get(source);
            if (targets) {
                targets.forEach((cb) => {
                    cb(source, values);
                });
            }
        }

        _incomingConnectionState(connected) {
            this._connectionStateDispatch.forEach((cb) => {
                cb(connected);
            });
        }

        _incomingInstrumentAdd(source, info) {
            this._instrumentAddAnyDispatch.forEach((cb) => {
                cb(source, info);
            });
            const targets = this._instrumentAddSourceDispatch.get(source);
            if (targets) {
                targets.forEach((cb) => {
                    cb(source, info);
                });
            }
        }

        _incomingInstrumentRemove(source) {
            this._instrumentRemoveAnyDispatch.forEach((cb) => {
                cb(source);
            });
            const targets = this._instrumentRemoveSourceDispatch.get(source);
            if (targets) {
                targets.forEach((cb) => {
                    cb(source);
                });
            }
        }

        _incomingInstrumentState(source, state) {
            this._instrumentStateAnyDispatch.forEach((cb) => {
                cb(source, state);
            });
            const targets = this._instrumentStateSourceDispatch.get(source);
            if (targets) {
                targets.forEach((cb) => {
                    cb(source, state);
                });
            }
        }

        _incomingEventLog(source, event) {
            this._eventLogDispatch.forEach((cb) => {
                cb(source, event);
            });
        }

        _incomingChatMessage(epoch_ms, from, message) {
            this._chatMessageDispatch.forEach((cb) => {
                cb(epoch_ms, from, message);
            });
        }

        addConnectionState(call) {
            if (this._detached) {
                return;
            }

            stateContexts.add(this._context);
            this._connectionStateDispatch.push(call);

            call(socketIsConnected);
        }

        addDataTarget(call) {
            if (this._detached) {
                return;
            }

            dataTargetContexts.add(this._context);
            this._dataAnyDispatch.push(call);
        }

        addSourceTarget(call, source, ...values) {
            if (this._detached) {
                return;
            }
            if (!source) {
                return;
            }

            let target = this._dataSourceDispatch.get(source);
            if (!target) {
                target = [];
                this._dataSourceDispatch.set(source, target);
            }

            dataTargetContexts.add(this._context);
            target.push((source, inputValues) => {
                const args = [];
                for (let i=0; i<values.length; i++) {
                    const fieldName = values[i];
                    if (!inputValues.has(fieldName)) {
                        continue;
                    }

                    while (args.length <= i) {
                        args.push(undefined);
                    }
                    const v = values[i];
                    if (v !== null) {
                        args[i] = inputValues.get(v);
                    }
                }
                if (args.length > 0) {
                    while (args.length < values.length) {
                        args.push(undefined);
                    }
                    call(...args);
                }
            });

            const cached = sourceDataCache.get(source);
            if (cached) {
                const args = [];
                const expireTime = Date.now() - 2 * 60 * 1000;
                for (let i=0; i<values.length; i++) {
                    const fieldName = values[i];
                    const cachedValue = cached.get(fieldName);
                    if (!cachedValue) {
                        continue;
                    }
                    if (cachedValue.updated < expireTime) {
                        continue;
                    }
                    while (args.length <= i) {
                        args.push(undefined);
                    }

                    const v = cachedValue.value;
                    if (v !== null) {
                        args[i] = v;
                    }
                }
                if (args.length > 0) {
                    while (args.length < values.length) {
                        args.push(undefined);
                    }
                    call(...args);
                }
            }
        }

        addInstrumentPresent(call, source) {
            if (this._detached) {
                return;
            }

            instrumentContexts.add(this._context);
            if (source) {
                let target = this._instrumentAddSourceDispatch.get(source);
                if (!target) {
                    target = [];
                    this._instrumentAddSourceDispatch.set(source, target);
                }
                target.push(call);

                const info = instrumentsPresent.get(source);
                if (info) {
                    call(source, info);
                }
            } else {
                this._instrumentAddAnyDispatch.push(call);
                instrumentsPresent.forEach((info, infoSource) => {
                    call(infoSource, info);
                });
            }
        }

        addInstrumentRemove(call, source) {
            if (this._detached) {
                return;
            }

            instrumentContexts.add(this._context);
            if (source) {
                let target = this._instrumentRemoveSourceDispatch.get(source);
                if (!target) {
                    target = [];
                    this._instrumentRemoveSourceDispatch.set(source, target);
                }
                target.push(call);
            } else {
                this._instrumentRemoveAnyDispatch.push(call);
            }
        }

        addInstrumentState(call, source) {
            if (this._detached) {
                return;
            }

            instrumentContexts.add(this._context);
            if (source) {
                let target = this._instrumentStateSourceDispatch.get(source);
                if (!target) {
                    target = [];
                    this._instrumentStateSourceDispatch.set(source, target);
                }
                target.push(call);

                const state = instrumentsState.get(source);
                if (state) {
                    call(source, state);
                }
            } else {
                this._instrumentStateAnyDispatch.push(call);
                instrumentsState.forEach((state, stateSource) => {
                    call(stateSource, state);
                });
            }
        }

        addEventLog(call) {
            if (this._detached) {
                return;
            }

            eventLogContexts.add(this._context);
            this._eventLogDispatch.push(call);
        }

        addChatMessage(call) {
            if (this._detached) {
                return;
            }

            chatMessageContexts.add(this._context);
            this._chatMessageDispatch.push(call);
        }
    }
})();