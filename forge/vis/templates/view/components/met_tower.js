var MetTower = {};
(function() {
    MetTower.difference = function(a, b) {
        if (!isFinite(a)) {
            return undefined;
        }
        if (!isFinite(b)) {
            return undefined;
        }

        return a - b;
    }

    MetTower.CalculateDifference = class extends DataSocket.RecordDispatch {
        constructor(dataName, a, b, output) {
            super(dataName);
            this.inputA = a;
            this.inputB = b;
            if (output === undefined) {
                output = b;
            }
            this.outputDifference = output;
        }

        processRecord(record, epoch) {
            let inputA = record.get(this.inputA);
            if (!inputA) {
                inputA = [];
            }
            let inputB = record.get(this.inputB);
            if (!inputB) {
                inputB = [];
            }

            let outputDifference = record.get(this.outputDifference);
            if (!outputDifference) {
                outputDifference = [];
                for (let i=0; i<epoch.length; i++) {
                    inputB.push(undefined);
                }
                record.set(this.outputDifference, outputDifference);
            }

            for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                const a = inputA[timeIndex];
                const b = inputB[timeIndex];
                outputDifference[timeIndex] = MetTower.difference(a, b);
            }
        }
    }
})();