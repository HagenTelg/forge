var GenericOperations = {};
(function() {
    GenericOperations.difference = function(...args) {
        let result = undefined;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            if (result === undefined) {
                result = val;
            } else {
                result -= val;
            }
        }
        return result;
    }
    GenericOperations.divide = function(...args) {
        let result = undefined;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            if (result === undefined) {
                result = val;
            } else {
                if (val === 0.0) {
                    return undefined;
                }
                result /= val;
            }
        }
        return result;
    }
    GenericOperations.sum = function(...args) {
        let result = 0;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            result += val;
        }
        return result;
    }
    GenericOperations.product = function(...args) {
        let result = 1.0;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            result *= val;
        }
        return result;
    }
    GenericOperations.pow = function(...args) {
        let result = undefined;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            if (result === undefined) {
                result = val;
            } else {
                result = Math.pow(result, val);
            }
        }
        return result;
    }
    GenericOperations.log = function(...args) {
        let result = undefined;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            if (val <= 0.0) {
                return undefined;
            }
            if (result === undefined) {
                result = Math.log(val);
            } else {
                result /= Math.log(val);
            }
        }
        return result;
    }
    GenericOperations.log10 = function(...args) {
        let result = undefined;
        for (const val of args) {
            if (!isFinite(val)) {
                return undefined;
            }
            if (val <= 0.0) {
                return undefined;
            }
            if (result === undefined) {
                result = Math.log10(val);
            } else {
                result /= Math.log10(val);
            }
        }
        return result;
    }
    GenericOperations.calibration = function(value, ...coefficients) {
        if (!isFinite(value)) {
            return value;
        }
        if (coefficients.length === 0) {
            return undefined;
        }

        let result = 0.0;
        let accumulator = 1.0;
        for (const c of coefficients) {
            if (!isFinite(c)) {
                return undefined;
            }
            result += c * accumulator;
            accumulator *= value;
        }
        return result;
    }

    GenericOperations.SingleOutput = class extends DataSocket.RecordDispatch {
        constructor(dataName, operation, output, ...inputs) {
            super(dataName);
            this.inputFields = inputs;

            if (output === undefined) {
                output = this.inputFields[inputs.length-1];
            }
            this.outputField = output;

            if (!operation) {
                operation = GenericOperations.difference;
            }
            this.operation = operation;
            this.before = [];
            this.after = [];

            this._inputValues = [];
        }

        processRecord(record, epoch) {
            this._inputValues.length = 0;
            for (const fieldName of this.inputFields) {
                let values = record.get(fieldName);
                if (!values) {
                    values = [];
                }
                this._inputValues.push(values);
            }

            let outputValues = record.get(this.outputField);
            if (!outputValues) {
                outputValues = [];
                for (let i=0; i<epoch.length; i++) {
                    outputValues.push(undefined);
                }
                record.set(this.outputField, outputValues);
            }

            const args = Array.from(this.before);
            for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                args.length = this.before.length;
                for (const fieldValues of this._inputValues) {
                    args.push(fieldValues[timeIndex]);
                }
                args.push(...this.after);
                outputValues[timeIndex] = this.operation(...args);
            }
        }
    }

    GenericOperations.ApplyToFields = class extends DataSocket.RecordDispatch {
        constructor(dataName, fieldOperations, ...inputs) {
            super(dataName);
            this.inputFields = inputs;

            this.fieldOperations = fieldOperations;

            this.before = [];
            this.after = [];

            this._inputValues = [];
        }

        processRecord(record, epoch) {
            this._inputValues.length = 0;
            for (const fieldName of this.inputFields) {
                let values = record.get(fieldName);
                if (!values) {
                    values = [];
                }
                this._inputValues.push(values);
            }

            for (const fieldName of Object.keys(this.fieldOperations)) {
                const operation = this.fieldOperations[fieldName];

                let outputValues = record.get(fieldName);
                if (!outputValues) {
                    outputValues = [];
                    for (let i=0; i<epoch.length; i++) {
                        outputValues.push(undefined);
                    }
                    record.set(fieldName, outputValues);
                }

                const args = Array.from(this.before);
                for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                    args.length = this.before.length;
                    args.push(outputValues[timeIndex]);

                    for (const fieldValues of this._inputValues) {
                        args.push(fieldValues[timeIndex]);
                    }
                    args.push(...this.after);
                    outputValues[timeIndex] = operation(...args);
                }
            }
        }
    }
})();