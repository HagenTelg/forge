var Contamination = {};
(function() {
    Contamination.DataStream = class extends DataSocket.Stream {
        constructor(dataName) {
            super(dataName);
            this.callbacks = [];
        }

        incomingDataContent(content) {
            const start_ms = content.start_epoch_ms;
            const end_ms = content.end_epoch_ms;
            const flags = content.flags;

            this.callbacks.forEach((cb) => {
                cb(start_ms, end_ms, flags);
            });
        }

        attach(callback) {
            this.callbacks.push(callback);
        }
    };
})();