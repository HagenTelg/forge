var PurpleAir = {};
(function() {
    const scatteringEfficiency = 0.015;

    PurpleAir.CalculateDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, inputA, inputB, outputScattering) {
            super(dataName);
            this.inputA = inputA;
            this.inputB = inputB;
            this.outputScattering = outputScattering;
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

            let outputScattering = record.get(this.outputScattering);
            if (!outputScattering) {
                outputScattering = [];
                for (let i=0; i<inputA.length; i++) {
                    outputScattering.push(undefined);
                }
                record.set(this.outputScattering, outputScattering);
            }

            for (let timeIndex=0; timeIndex<inputA.length; timeIndex++) {
                if (isFinite(outputScattering[timeIndex])) {
                    continue;
                }
                const A = inputA[timeIndex];
                const B = inputB[timeIndex];
                outputScattering[timeIndex] = (A + B) / 2.0 * scatteringEfficiency;
            }
        }
    }
})();