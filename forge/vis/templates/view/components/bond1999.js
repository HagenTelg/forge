var Bond1999 = {};
(function() {
    Bond1999.CorrectRecord = class {
        constructor(absorption, scattering) {
            this.absorption = absorption;
            this.scattering = scattering;

            this._scatteringForAbsorption = new Map();
            const adjustedScattering = new Map();
            absorption.forEach((abs, fieldName) => {
                const outputName = fieldName + '_Bs';
                adjustedScattering.set(outputName, abs.wavelength);
                this._scatteringForAbsorption.set(fieldName, outputName);
            });
            this.adjust = new WavelengthAdjust.RecordAdjust(scattering, adjustedScattering);
        }

        correctRecord(record, numberOfValues) {
            this.adjust.adjustRecord(record, numberOfValues);

            let correctionData = [];
            this.absorption.forEach((abs, fieldName) => {
                let Ba = record.get(fieldName);
                if (!Ba) {
                    return;
                }

                let Bs = record.get(this._scatteringForAbsorption.get(fieldName));
                if (!Bs) {
                    Bs = [];
                }

                let correction = {
                    Ba: Ba,
                    Bs: Bs,
                };
                correctionData.push(correction);

                if (abs.transmittance) {
                    let d = record.get(abs.transmittance);
                    if (!d) {
                        d = [];
                    }
                    correction.Tr = d;
                }
            });

            for (let timeIndex=0; timeIndex < numberOfValues; timeIndex++) {
                for (const corr of correctionData) {
                    if (corr.Tr) {
                        const weissFactor = 0.814 + corr.Tr[timeIndex] * 1.237;
                        corr.Ba[timeIndex] = corr.Ba[timeIndex] / weissFactor;
                    }

                    corr.Ba[timeIndex] = (corr.Ba[timeIndex] * 0.97 - 0.02 * corr.Bs[timeIndex]) / 1.22;
                }
            }
        }
    }

    Bond1999.CorrectDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, absorption, scattering, scattering_temperature, scattering_pressure) {
            super(dataName);

            if (scattering_temperature && scattering_pressure) {
                this.scattering_stp = new STP.CorrectOpticalRecord(Array.from(scattering.keys()),
                    scattering_temperature, scattering_pressure);
            } else {
                this.scattering_stp = undefined;
            }

            this.correct = new Bond1999.CorrectRecord(absorption, scattering)
        }
        processRecord(record, epoch) {
            if (this.scattering_stp) {
                this.scattering_stp.correctRecord(record, epoch.length);
            }
            this.correct.correctRecord(record, epoch.length);
        }
    }
})();