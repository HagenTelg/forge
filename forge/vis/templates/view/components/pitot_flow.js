var PitotFlow = {};
(function() {
    // {{ '\n' }}{% include 'global/calculations/pitot_flow.js' %}

    PitotFlow.CalculateDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, inputPressure, outputFlow) {
            super(dataName);
            this.inputPressure = inputPressure;
            if (outputFlow === undefined) {
                outputFlow = inputPressure;
            }
            this.outputFlow = outputFlow;
        }

        processRecord(record, epoch) {
            let inputPressure = record.get(this.inputPressure);
            if (!inputPressure) {
                inputPressure = [];
            }

            let outputFlow = record.get(this.outputFlow);
            if (!outputFlow) {
                outputFlow = [];
                for (let i=0; i < inputPressure.length; i++) {
                    outputFlow.push(undefined);
                }
                record.set(this.outputFlow, outputFlow);
            }

            for (let timeIndex=0; timeIndex < inputPressure.length; timeIndex++) {
                const dP = inputPressure[timeIndex];
                outputFlow[timeIndex] = calculatePitotFlow(dP, stackPitotArea, 273.15, 1013.25);
            }
        }
    }
})();