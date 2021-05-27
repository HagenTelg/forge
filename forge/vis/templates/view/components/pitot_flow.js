var PitotFlow = {};
(function() {
    const stackPitotArea = (function() {
        let d = 1.75 * 25.4;
        d /= 1000.0;
        d /= 2.0;
        return d * d * Math.PI;
    })();

    PitotFlow.calculate = function(dP, A, t, p) {
        if (!isFinite(dP) || dP < 0.0) {
            return undefined;
        }
        if (!isFinite(A) || A <= 0.0) {
            return undefined;
        }
        if (!isFinite(t) || t <= 0.0) {
            return undefined;
        }
        if (!isFinite(p) || p <= 0.0) {
            return undefined;
        }

        let density = 1.2922;   /* kg/m^3 at 0C */
        density *= (273.15 / t) * (p / 1013.25);

        const v = Math.sqrt(2.0 * (dP * 100.0) / density);   /* m/s */
        let Q = v * A;   /* m^3/s */

        Q *= 60000.0;   /* lpm */
        Q *= (p / 1013.25) * (273.15 / t);  /* To mass flow */

        return Q;
    }

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
                for (let i=0; i<inputPressure.length; i++) {
                    outputFlow.push(undefined);
                }
                record.set(this.outputFlow, outputFlow);
            }

            for (let timeIndex=0; timeIndex<inputPressure.length; timeIndex++) {
                const dP = inputPressure[timeIndex];
                outputFlow[timeIndex] = PitotFlow.calculate(dP, stackPitotArea, 273.15, 1013.25);
            }
        }
    }
})();