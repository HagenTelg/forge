var Truncation = {};
(function() {
    Truncation.CorrectRecord = class {
        constructor(channels) {
            this.channels = [];
            channels.forEach((config, fieldName) => {
                this.channels.push({
                    fieldName: fieldName,
                    wavelength: config.wavelength,
                    no_angstrom: config.no_angstrom,
                    angstrom_fit: config.angstrom_fit,
                });
            });
            this.channels.sort((a, b) => {
                const ca = a.wavelength;
                const cb = b.wavelength;
                if (ca < cb) {
                    return -1;
                } else if (ca > cb) {
                    return 1;
                }
                return 0;
            });
        }

        correctRecord(record, numberOfValues) {
            let wavelengthData = [];
            for (const c of this.channels) {
                let d = record.get(c.fieldName);
                if (!d) {
                    d = [];
                }
                wavelengthData.push(d);
            }

            for (let timeIndex=0; timeIndex < numberOfValues; timeIndex++) {
                const wavelengthValues = [];
                for (const d of wavelengthData) {
                    wavelengthValues.push(d[timeIndex]);
                }
                for (let i=0; i<wavelengthValues.length; i++) {
                    let angLower = Math.max(i-1, 0);
                    let angUpper = Math.min(i+1, wavelengthValues.length-1);

                    const ang = Math.log(wavelengthValues[angLower] / wavelengthValues[angUpper]) /
                        Math.log(this.channels[angUpper].wavelength / this.channels[angLower].wavelength);

                    const angFit = this.channels[i].angstrom_fit;
                    const noAngFactor = this.channels[i].no_angstrom;
                    if (!isFinite(ang) || !angFit) {
                        wavelengthData[i][timeIndex] = wavelengthData[i][timeIndex] * noAngFactor;
                        continue;
                    }

                    let fit = 0.0;
                    let add = 1.0;
                    for (const c of angFit) {
                        fit += c * add;
                        add *= ang;
                    }
                    wavelengthData[i][timeIndex] = wavelengthData[i][timeIndex] * fit;
                }
            }
        }
    }

    Truncation.AndersonOgren1998Coarse = {
        BsB: {
            wavelength: 450.0,
            no_angstrom: 1.29,
            angstrom_fit: [1.365, -0.156],
        },
        BsG: {
            wavelength: 550.0,
            no_angstrom: 1.29,
            angstrom_fit: [1.337, -0.138],
        },
        BsR: {
            wavelength: 700.0,
            no_angstrom: 1.26,
            angstrom_fit: [1.297, -0.113],
        },
        BbsB: {
            wavelength: 450.0,
            no_angstrom: 0.981,
        },
        BbsG: {
            wavelength: 550.0,
            no_angstrom: 0.982,
        },
        BbsR: {
            wavelength: 700.0,
            no_angstrom: 0.985,
        },
    };
    Truncation.AndersonOgren1998Fine = {
        BsB: {
            wavelength: 450.0,
            no_angstrom: 1.094,
            angstrom_fit: [1.165, -0.046],
        },
        BsG: {
            wavelength: 550.0,
            no_angstrom: 1.073,
            angstrom_fit: [1.152, -0.044],
        },
        BsR: {
            wavelength: 700.0,
            no_angstrom: 1.049,
            angstrom_fit: [1.120, -0.035],
        },
        BbsB: {
            wavelength: 450.0,
            no_angstrom: 0.951,
        },
        BbsG: {
            wavelength: 550.0,
            no_angstrom: 0.947,
        },
        BbsR: {
            wavelength: 700.0,
            no_angstrom: 0.952,
        },
    };

    Truncation.Mueller2011EcotechCoarse = {
        BsB: {
            wavelength: 450.0,
            no_angstrom: 1.37,
            angstrom_fit: [1.455, -0.189],
        },
        BsG: {
            wavelength: 525.0,
            no_angstrom: 1.38,
            angstrom_fit: [1.434, -0.176],
        },
        BsR: {
            wavelength: 635.0,
            no_angstrom: 1.36,
            angstrom_fit: [1.403, -0.156],
        },
        BbsB: {
            wavelength: 450.0,
            no_angstrom: 0.963,
        },
        BbsG: {
            wavelength: 525.0,
            no_angstrom: 0.971,
        },
        BbsR: {
            wavelength: 635.0,
            no_angstrom: 0.968,
        },
    };
    Truncation.Mueller2011EcotechFine = {
        BsB: {
            wavelength: 450.0,
            no_angstrom: 1.125,
            angstrom_fit: [1.213, -0.060],
        },
        BsG: {
            wavelength: 525.0,
            no_angstrom: 1.103,
            angstrom_fit: [1.207, -0.061],
        },
        BsR: {
            wavelength: 635.0,
            no_angstrom: 1.078,
            angstrom_fit: [1.176, -0.053],
        },
        BbsB: {
            wavelength: 450.0,
            no_angstrom: 0.932,
        },
        BbsG: {
            wavelength: 525.0,
            no_angstrom: 0.935,
        },
        BbsR: {
            wavelength: 635.0,
            no_angstrom: 0.935,
        },
    };

    Truncation.TSI3563Dispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, channels, temperature, pressure) {
            super(dataName);

            if (!temperature) {
                temperature = "T";
            }
            if (!pressure) {
                pressure = "P";
            }

            const stp_vars = [];
            const total = new Map();
            for (const k of ['BsB', 'BsG', 'BsR']) {
                const c = channels[k];
                if (!c) {
                    continue;
                }
                total.set(k, c);
                stp_vars.push(k);
            }
            const back = new Map();
            for (const k of ['BbsB', 'BbsG', 'BbsR']) {
                const c = channels[k];
                if (!c) {
                    continue;
                }
                back.set(k, c);
                stp_vars.push(k);
            }

            this.stp = new STP.CorrectOpticalRecord(stp_vars, temperature, pressure);
            this.total = new Truncation.CorrectRecord(total);
            this.back = new Truncation.CorrectRecord(back);
        }
        processRecord(record, epoch) {
            this.stp.correctRecord(record, epoch.length);
            this.total.correctRecord(record, epoch.length);
            this.back.correctRecord(record, epoch.length);
        }
    }

    Truncation.Ecotech3000Dispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, channels) {
            super(dataName);

            const total = new Map();
            for (const k of ['BsB', 'BsG', 'BsR']) {
                const c = channels[k];
                if (!c) {
                    continue;
                }
                total.set(k, c);
            }
            const back = new Map();
            for (const k of ['BbsB', 'BbsG', 'BbsR']) {
                const c = channels[k];
                if (!c) {
                    continue;
                }
                back.set(k, c);
            }

            this.total = new Truncation.CorrectRecord(total);
            this.back = new Truncation.CorrectRecord(back);
        }
        processRecord(record, epoch) {
            this.total.correctRecord(record, epoch.length);
            this.back.correctRecord(record, epoch.length);
        }
    }
})();