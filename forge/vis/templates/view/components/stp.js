var STP = {};
(function() {
    // {{ '\n' }}{% include 'global/calculations/stp.js' %}

    STP.CorrectOpticalRecord = class {
        constructor(values, inputTemperature, inputPressure, defaultTemperature, defaultPressure) {
            this.correctValues = values;
            this.inputTemperature = inputTemperature;
            this.inputPressure = inputPressure;
            this.defaultTemperature = defaultTemperature;
            this.defaultPressure = defaultPressure;
        }

        correctRecord(record, numberOfValues) {
            let valueData = [];
            for (const f of this.correctValues) {
                let d = record.get(f);
                if (!d) {
                    d = [];
                }
                valueData.push(d);
            }

            let inputTemperature = record.get(this.inputTemperature);
            if (!inputTemperature) {
                inputTemperature = [];
            }
            let inputPressure = record.get(this.inputPressure);
            if (!inputPressure) {
                inputPressure = [];
            }

            for (let timeIndex=0; timeIndex < numberOfValues; timeIndex++) {
                let t = inputTemperature[timeIndex];
                if (!isFinite(t)) {
                    t = this.defaultTemperature;
                }

                let p = inputPressure[timeIndex];
                if (!isFinite(p)) {
                    p = this.defaultPressure;
                }

                for (const d of valueData) {
                    d[timeIndex] = correctOpticalSTP(d[timeIndex], t, p);
                }
            }
        }
    }

    STP.CorrectOpticalDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, values, inputTemperature, inputPressure, defaultTemperature, defaultPressure) {
            super(dataName);
            this.correction = new STP.CorrectOpticalRecord(values, inputTemperature, inputPressure,
                defaultTemperature, defaultPressure);
        }

        processRecord(record, epoch) {
            this.correction.correctRecord(record, epoch.length);
        }
    }
})();