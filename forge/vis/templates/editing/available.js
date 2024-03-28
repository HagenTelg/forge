const EditDirectiveAvailable = (function() {
    let readyCallbacks = [];

    const Available = {
        available: [],

        KnownAvailable: class {
            constructor() {
            }

            matches(selection) { return false; }

            matchesExactly(selection) { return false; }

            summaryText() {}

            selectionText() {}

            instrumentTypeText() { return undefined; }

            manufacturerText() { return undefined; }

            modelText() { return undefined; }

            serialNumberText() { return undefined; }

            wavelengthText() { return undefined; }

            titleText() { return undefined; }

            selection() {}

            instrumentID() {}

            variableID() {}
        },

        findMatching: function(selection) {
            let result = [];
            this.available.forEach((available) => {
                if (!available.matches(selection)) {
                    return;
                }
                result.push(available);
            });
            return result;
        },

        ready(callback) {
            if (readyCallbacks !== undefined) {
                readyCallbacks.push(callback);
            } else {
                callback();
            }
        }
    };

    class AvailableCPD3Variable extends Available.KnownAvailable {
        constructor(available) {
            super();
            this.variable = available.variable;
        }

        matches(selection) {
            if (selection.type !== 'cpd3_variable') {
                return false;
            }

            const match = (new RegExp(selection.variable)).exec(this.variable);
            if (!match) {
                return false;
            }
            if (match.index !== 0) {
                return false;
            }

            return true;
        }

        matchesExactly(selection) {
            return selection.variable === this.variable;
        }

        summaryText() { return this.instrumentID(); }

        selectionText() { return this.variable; }

        selection() {
            return {
                type: 'cpd3_variable',
                variable: this.variable,
            };
        }

        instrumentID() { return this.variable.split('_', 2)[1]; }

        variableID() { return this.variable.split('_', 2)[0]; }
    }

    class AvailableVariable extends Available.KnownAvailable {
        constructor(available, description) {
            super();
            this._available = available;
            const parts = available.variable_id.split('_', 2);
            if (parts.length > 1) {
                this._instrument_id = parts[1];
                this._variable_id = parts[0];
            } else {
                this._instrument_id = available.instrument_id;
                this._variable_id = available.variable_id;
            }
        }

        matches(selection) {
            if (selection.instrument_id !== undefined) {
                if (selection.instrument_id !== this._instrument_id) {
                    return false;
                }
            }

            return selection.variable_id === this._variable_id;
        }

        matchesExactly(selection) {
            return selection.variable_id === this._available.variable_id &&
                selection.instrument_id === this._available.instrument_id;
        }

        summaryText() { return this.instrumentID(); }

        selectionText() { return this._variable_id  + "_" + this._instrument_id; }

        instrumentTypeText() { return this._available.instrument; }

        manufacturerText() { return this._available.manufacturer; }

        modelText() { return this._available.model; }

        serialNumberText() {
            const sn = this._available.serial_number;
            if (!sn) {
                return undefined;
            }
            return "#" + sn;
        }

        titleText() { return this._available.description; }

        selection() {
            return {
                variable_id: this._available.variable_id,
                instrument_id: this._available.instrument_id,
                _summary: this.selectionText(),
            };
        }

        instrumentID() { return this._instrument_id; }

        variableID() { return this._variable_id; }
    }

    class AvailableWavelengthVariable extends AvailableVariable {
        constructor(available, available_wavelengths, wavelength_idx) {
            super(available);
            this._available_wavelengths = available_wavelengths;
            this._wavelength_idx = wavelength_idx;
        }

        selection() {
            let selection = super.selection();
            selection.wavelength = this._available_wavelengths[this._wavelength_idx];
            return selection
        }

        matchesExactly(selection) {
            return selection.variable_id === this._available.variable_id &&
                selection.instrument_id === this._available.instrument_id &&
                selection.wavelength === this._available_wavelengths[this._wavelength_idx];
        }

        selectionText() {
            function wavelengthSuffix(wl) {
                if (wl < 400) {
                    return undefined;
                } else if (wl < 500) {
                    return "B"
                } else if (wl < 600) {
                    return "G"
                } else if (wl < 750) {
                    return "R"
                }
                return undefined;
            }

            const wavelength = this._available_wavelengths[this._wavelength_idx];
            let suffix = undefined;
            if (this._available_wavelengths.length <= 3) {
                suffix = wavelengthSuffix(wavelength);
            }
            if (suffix !== undefined) {
                for (let i=0; i<this._available_wavelengths.length; i++) {
                    if (i === this._wavelength_idx) {
                        continue;
                    }
                    const check = wavelengthSuffix(this._available_wavelengths[i]);
                    if (check === suffix) {
                        suffix = undefined;
                        break;
                    }
                }
            }

            if (suffix === undefined) {
                if (this._available_wavelengths.length > 1) {
                    suffix = (this._wavelength_idx+1).toString();
                } else {
                    suffix = "";
                }
            }
            return this._variable_id + suffix + "_" + this._instrument_id;
        }

        wavelengthText() { return this._available_wavelengths[this._wavelength_idx].toFixed(0) + " nm"; }
    }

    $(document).ready(function() {
        const AvailableStream = class extends DataSocket.Stream {
            constructor() {
                super('{{ mode_name }}-available');
            }

            endOfData() {
                if (readyCallbacks === undefined) {
                    return;
                }
                readyCallbacks.forEach((cb) => { cb(); });
                readyCallbacks = undefined;
            }

            incomingDataContent(content) {
                if (content.type === 'cpd3_variable') {
                    EditDirectiveAvailable.available.push(new AvailableCPD3Variable(content));
                } else if (content.type === 'variable_id') {
                    const wavelengths = content.wavelengths;
                    if (wavelengths) {
                        wavelengths.forEach((wavelength, index) => {
                            EditDirectiveAvailable.available.push(new AvailableWavelengthVariable(
                                content, wavelengths, index
                            ));
                        });
                    } else {
                        EditDirectiveAvailable.available.push(new AvailableVariable(content));
                    }
                }
            }
        };

        (new AvailableStream()).beginStream();
    });

    return Available;
})();