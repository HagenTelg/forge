var Contamination = {};
(function() {
    Contamination.DataStream = class extends DataSocket.Stream {
        constructor(dataName) {
            super(dataName);
            this.callbacks = [];
            this.finished = [];
        }

        incomingDataContent(content) {
            const start_ms = content.start_epoch_ms;
            const end_ms = content.end_epoch_ms;

            this.callbacks.forEach((cb) => {
                cb(start_ms, end_ms);
            });
        }

        endOfData() {
            this.finished.forEach((cb) => {
                cb();
            });
        }

        attach(callback, finished) {
            this.callbacks.push(callback);
            if (finished) {
                this.finished.push(finished);
            }
        }
    };
})();