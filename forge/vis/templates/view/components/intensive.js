var Intensive = {};
(function() {
    Intensive.CalculateDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, outputNames, inputScattering, inputBackscattering, inputAbsorption, inputExtinction,
                    assumedAngstromExponent) {
            super(dataName);

            const outputScattering = new Map();
            const outputBackscattering = new Map();
            const outputAbsoprtion = new Map();
            const outputExtinction = new Map();
            this._calculations = [];
            outputNames.forEach((wavelength, fieldName) => {
                outputScattering.set('Bs' + fieldName, wavelength);
                outputBackscattering.set('Bbs' + fieldName, wavelength);
                outputAbsoprtion.set('Ba' + fieldName, wavelength);
                outputExtinction.set('Be' + fieldName, wavelength);
                this._calculations.push(fieldName);
            });

            this._scatteringWavelengths = new Map();
            inputScattering.forEach((wavelength, fieldName) => {
                this._scatteringWavelengths.set(fieldName, wavelength);
            });

            this._absorptionWavelengths = new Map();
            inputAbsorption.forEach((wavelength, fieldName) => {
                this._absorptionWavelengths.set(fieldName, wavelength);
            });

            this.scatteringAdjuster = new WavelengthAdjust.RecordAdjust(inputScattering, outputScattering,
                assumedAngstromExponent);
            this.backscatteringAdjuster = new WavelengthAdjust.RecordAdjust(inputBackscattering, outputBackscattering,
                assumedAngstromExponent);
            this.absorptionAdjuster = new WavelengthAdjust.RecordAdjust(inputAbsorption, outputAbsoprtion,
                assumedAngstromExponent);
            this.extinctionAdjuster = new WavelengthAdjust.RecordAdjust(inputExtinction, outputExtinction,
                assumedAngstromExponent);
        }

        processRecord(record, epoch) {
            function calculateAngstrom(timeIndex, wavelengths) {
                let firstValue = undefined;
                let firstWavelength = undefined;
                let lastValue = undefined;
                let lastWavelength = undefined;

                wavelengths.forEach((wavelength, fieldName) => {
                    const recordValues = record.get(fieldName);
                    if (!recordValues) {
                        return;
                    }
                    const value = recordValues[timeIndex];
                    if (!isFinite(value) || value <= 0.0) {
                        return;
                    }

                    lastValue = value;
                    lastWavelength = wavelength;

                    if (firstValue === undefined) {
                        firstValue = value;
                        firstWavelength = wavelength;
                    }
                });

                let ang = Math.log(firstValue / lastValue) / Math.log(lastWavelength / firstWavelength);
                if (isFinite(ang)) {
                    ang = Math.min(ang, 5.0);
                    ang = Math.max(ang, -1.0);
                }
                return ang;
            }

            const angstromScatteringOutput = [];
            record.set('AngBs', angstromScatteringOutput);
            const angstromAbsorptionOutput = [];
            record.set('AngBa', angstromAbsorptionOutput);
            for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                angstromScatteringOutput.push(calculateAngstrom(timeIndex, this._scatteringWavelengths));
                angstromAbsorptionOutput.push(calculateAngstrom(timeIndex, this._absorptionWavelengths));
            }

            this.scatteringAdjuster.adjustRecord(record, epoch.length);
            this.backscatteringAdjuster.adjustRecord(record, epoch.length);
            this.absorptionAdjuster.adjustRecord(record, epoch.length);
            this.extinctionAdjuster.adjustRecord(record, epoch.length);

            function processField(fieldName, calculate, ...inputs) {
                let fieldData = record.get(fieldName);
                if (!fieldData) {
                    fieldData = [];
                    record.set(fieldName, fieldData);
                    for (let i=0; i<epoch.length; i++) {
                        fieldData.push(undefined);
                    }
                }

                let args = [];
                for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                    if (isFinite(fieldData[timeIndex])) {
                        continue;
                    }

                    args.length = 0;
                    inputs.forEach((inputValues) => {
                        let input = undefined;
                        if (inputValues) {
                            input = inputValues[timeIndex];
                        }
                        args.push(input);
                    });

                    let value = calculate(...args);
                    if (!isFinite(value)) {
                        value = undefined;
                    }
                    fieldData[timeIndex] = value;
                }

                return fieldData;
            }

            this._calculations.forEach((fieldName) => {
                let Bs = record.get('Bs' + fieldName);
                let Bbs = record.get('Bbs' + fieldName);
                let Ba = record.get('Ba' + fieldName);
                let Be = record.get('Be' + fieldName);

                Bs = processField('Bs' + fieldName, (Be, Ba) => { return Be - Ba; }, Be, Ba);
                Ba = processField('Ba' + fieldName, (Be, Bs) => { return Be - Bs; }, Be, Bs);
                Be = processField('Be' + fieldName, (Bs, Ba) => { return Bs + Ba; }, Bs, Ba);

                processField('SSA' + fieldName, (Bs, Be) => {
                    return Math.max(Math.min(Bs / Be, 2.0), -0.5);
                }, Bs, Be);
                processField('Bfr' + fieldName, (Bbs, Bs) => {
                    return Math.max(Math.min(Bbs / Bs, 1.0), -0.5);
                }, Bbs, Bs);
            });
        }
    }
})();