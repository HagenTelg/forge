var WavelengthAdjust = {};
(function() {
    function calculateAngstromExponents(inputValues, inputWavelengths, angstromExponents) {
        for (let il=0; il<inputValues.length-1; ) {
            const vl = inputValues[il];
            if (!isFinite(vl) || vl <= 0.0) {
                il++;
                continue;
            }

            let iu = il + 1;
            let vu;
            while (iu<inputValues.length) {
                vu = inputValues[iu];
                if (isFinite(vu) && vu > 0.0) {
                    break;
                }
                iu++;
            }
            if (iu >= inputValues.length) {
                break;
            }

            const wl = inputWavelengths[il];
            if (!isFinite(wl) || wl <= 0.0) {
                il = iu;
                continue;
            }

            const wu = inputWavelengths[iu];
            if (!isFinite(wu) || wu <= 0.0) {
                il = iu;
                continue;
            }

            angstromExponents.push({
                angstrom: Math.log(vl / vu) / Math.log(wu / wl),
                start: wl,
                end: wu,
            });
            il = iu;
        }

        if (angstromExponents.length > 0) {
            angstromExponents[0].start = undefined;
            angstromExponents[angstromExponents.length-1].end = undefined;
        }
    }
    function convertAssumedAngstromExponents(assumedAngstromExponent, angstromExponents) {
        function addAssumedAngstromExponent(value) {
            if (typeof value === 'number') {
                angstromExponents.push({
                    angstrom: value,
                });
            } else {
                const add = {
                    angstrom: value.angstrom,
                    start: value.start,
                    end: value.end,
                };
                if (isFinite(value.center)) {
                    add.center = value.center;
                    if (isFinite(value.validDistance)) {
                        const start = value.center - value.validDistance;
                        if (!isFinite(add.start) || add.start < start) {
                            add.start = start;
                        }

                        const end = value.center + value.validDistance;
                        if (!isFinite(add.end) || add.end > end) {
                            add.end = end;
                        }
                    }
                }
                angstromExponents.push(add);
            }
        }
        if (Array.isArray(assumedAngstromExponent)) {
            assumedAngstromExponent.forEach((content) => {
                addAssumedAngstromExponent(content);
            });
        } else {
            addAssumedAngstromExponent(assumedAngstromExponent);
        }
    }

    function adjustOpticalData(inputValues, inputWavelengths, outputWavelengths, angstromExponents,
                               outputValues) {
        function linearInterpolate(sourceIndex, sourceValue, sourceWavelength, targetWavelength) {
            function interpolate(lowerValue, lowerWavelength, upperValue, upperWavelength) {
                const slope = (upperValue - lowerValue) / (upperWavelength - lowerWavelength);
                const delta = (targetWavelength - lowerWavelength) * slope;
                return lowerValue + delta;
            }
            function walkDown() {
                for (let checkIndex=sourceIndex-1; checkIndex >= 0; checkIndex--) {
                    const checkValue = inputValues[checkIndex];
                    if (!isFinite(checkValue)) {
                        continue;
                    }
                    const checkWavelength = inputWavelengths[checkIndex];
                    if (!isFinite(checkWavelength)) {
                        continue;
                    }
                    return interpolate(checkValue, checkWavelength, sourceValue, sourceWavelength);
                }
                return undefined;
            }
            function walkUp() {
                for (let checkIndex=sourceIndex+1; checkIndex < inputValues.length; checkIndex++) {
                    const checkValue = inputValues[checkIndex];
                    if (!isFinite(checkValue)) {
                        continue;
                    }
                    const checkWavelength = inputWavelengths[checkIndex];
                    if (!isFinite(checkWavelength)) {
                        continue;
                    }
                    return interpolate(sourceValue, sourceWavelength, checkValue, checkWavelength);
                }
                return undefined;
            }
            if (targetWavelength < sourceWavelength) {
                let value = walkDown();
                if (!isFinite(value)) {
                    value = walkUp();
                }
                return value;
            } else {
                let value = walkUp();
                if (!isFinite(value)) {
                    value = walkDown();
                }
                return value;
            }
        }
        function angstromInterpolate(sourceIndex, sourceValue, sourceWavelength, targetWavelength) {
            if (sourceValue <= 0.0 || sourceWavelength <= 0.0 || targetWavelength <= 0.0) {
                return undefined;
            }

            function selectAngstrom() {
                let selectedAngstrom = undefined;
                for (let i=0; i<angstromExponents.length; i++) {
                    const checkAngstrom = angstromExponents[i];
                    if (checkAngstrom.start && targetWavelength < checkAngstrom.start) {
                        continue;
                    }
                    if (checkAngstrom.end && targetWavelength > checkAngstrom.end) {
                        continue;
                    }
                    if (!checkAngstrom.center) {
                        return checkAngstrom.angstrom;
                    }

                    if (selectedAngstrom === undefined) {
                        selectedAngstrom = checkAngstrom;
                    } else if (Math.abs(selectedAngstrom.center - targetWavelength) >
                            Math.abs(checkAngstrom.center - targetWavelength)) {
                        selectedAngstrom = checkAngstrom;
                    }
                }
                if (selectedAngstrom) {
                    return selectedAngstrom.angstrom;
                }
                return undefined;
            }

            const angstrom = selectAngstrom(targetWavelength);
            if (!isFinite(angstrom) || angstrom <= 0.0) {
                return undefined;
            }
            return sourceValue * Math.pow((sourceWavelength / targetWavelength), angstrom);
        }

        outputWavelengths.forEach((targetWavelength) => {
            let sourceIndex = 0;
            for (let checkIndex = 0; checkIndex<inputValues.length; checkIndex++) {
                if (!isFinite(inputValues[sourceIndex]) || !isFinite(inputWavelengths[sourceIndex])) {
                    sourceIndex = checkIndex;
                    continue;
                }
                if (Math.abs(targetWavelength - inputWavelengths[sourceIndex]) <
                        Math.abs(targetWavelength - inputWavelengths[checkIndex])) {
                    continue;
                }

                sourceIndex = checkIndex;
            }

            const sourceValue = inputValues[sourceIndex];
            if (!isFinite(sourceValue)) {
                outputValues.push(undefined);
                return;
            }
            const sourceWavelength = inputWavelengths[sourceIndex];
            if (!isFinite(sourceWavelength)) {
                outputValues.push(undefined);
                return;
            }
            if (sourceWavelength === targetWavelength) {
                outputValues.push(sourceValue);
                return;
            }

            const interpolated = angstromInterpolate(sourceIndex, sourceValue, sourceWavelength, targetWavelength);
            if (isFinite(interpolated)) {
                outputValues.push(interpolated);
            } else {
                outputValues.push(linearInterpolate(sourceIndex, sourceValue, sourceWavelength, targetWavelength));
            }
        });
    }

    WavelengthAdjust.adjust = function(inputValues, inputWavelengths, outputWavelengths, assumedAngstromExponent) {
        let angstromExponents = [];
        if (inputValues.length >= 2) {
            calculateAngstromExponents(inputValues, inputWavelengths, angstromExponents);
        }
        if (assumedAngstromExponent) {
            convertAssumedAngstromExponents(assumedAngstromExponent, angstromExponents);
        }

        let outputValues = [];
        adjustOpticalData(inputValues, inputWavelengths, outputWavelengths, angstromExponents, outputValues);
        return outputValues;
    }

    const emptyInput = [];
    WavelengthAdjust.RecordAdjust = class {
        constructor(inputFields, outputFields, assumedAngstromExponent) {
            this._constantAngstromExponents = [];
            if (assumedAngstromExponent) {
                convertAssumedAngstromExponents(assumedAngstromExponent, this._constantAngstromExponents);
            }
            this._angstromExponents = [];

            this._inputWavelengths = [];
            this._inputFieldNames = [];
            this._inputFieldData = [];
            inputFields.forEach((wavelength, fieldName) => {
                this._inputWavelengths.push(wavelength);
                this._inputFieldNames.push(fieldName);
                this._inputFieldData.push(emptyInput);
            });

            this._outputWavelengths = [];
            this._outputFieldNames = [];
            this._outputFieldData = [];
            this._outputPrecision = [];
            outputFields.forEach((fieldData, fieldName) => {
                this._outputFieldNames.push(fieldName);
                this._outputFieldData.push(undefined);
                if (typeof fieldData === 'number') {
                    this._outputWavelengths.push(fieldData);
                    this._outputPrecision.push(undefined);
                } else {
                    this._outputWavelengths.push(fieldData.wavelength);
                    this._outputPrecision.push(fieldData.precision);
                }
            });

            this._inputValues = [];
            this._outputValues = [];
        }

        adjustRecord(record, numberOfValues) {
            this._inputFieldNames.forEach((fieldName, index) => {
                let fieldData = record.get(fieldName);
                if (!fieldData) {
                    fieldData = emptyInput;
                }
                this._inputFieldData[index] = fieldData;
            });

            this._outputFieldNames.forEach((fieldName, index) => {
                let fieldData = record.get(fieldName);
                if (!fieldData) {
                    fieldData = [];
                    for (let i=0; i<numberOfValues; i++) {
                        fieldData.push(undefined);
                    }
                    record.set(fieldName, fieldData);
                }
                this._outputFieldData[index] = fieldData;
            });

            for (let timeIndex=0; timeIndex<numberOfValues; timeIndex++) {
                this._inputValues.length = 0;
                this._inputFieldData.forEach((fieldData) => {
                    this._inputValues.push(fieldData[timeIndex]);
                });

                this._angstromExponents.length = 0;
                if (this._inputValues.length >= 2) {
                    calculateAngstromExponents(this._inputValues, this._inputWavelengths, this._angstromExponents);
                }
                this._constantAngstromExponents.forEach((angstrom) => {
                    this._angstromExponents.push(angstrom);
                });

                this._outputValues.length = 0;

                adjustOpticalData(this._inputValues, this._inputWavelengths, this._outputWavelengths,
                    this._angstromExponents, this._outputValues);

                this._outputFieldData.forEach((fieldData, fieldIndex) => {
                    let resultValue = this._outputValues[fieldIndex];
                    const precision = this._outputPrecision[fieldIndex];
                    if (precision !== undefined) {
                        const scale = Math.pow(10, precision);
                        resultValue = Math.round(resultValue * scale) / scale;
                    }
                    fieldData[timeIndex] = resultValue;
                });
            }
        }
    }
    WavelengthAdjust.AdjustedDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, inputFields, outputFields, assumedAngstromExponent) {
            super(dataName);
            this.adjuster = new WavelengthAdjust.RecordAdjust(inputFields, outputFields, assumedAngstromExponent);
        }

        processRecord(record, epoch) {
            this.adjuster.adjustRecord(record, epoch.length);
        }
    }
})();