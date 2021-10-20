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

            selection() {}

            instrument() {}
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

    class AvailableVariable extends Available.KnownAvailable {
        constructor(available) {
            super();
            this.variable = available.variable;
        }

        matches(selection) {
            if (selection.type !== 'variable') {
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
            if (selection.type !== 'variable') {
                return false;
            }
            return selection.variable === this.variable;
        }

        summaryText() { return this.instrument(); }

        selectionText() { return this.variable; }

        selection() {
            return {
                type: 'variable',
                variable: this.variable,
            };
        }

        instrument() { return this.variable.split('_', 2)[1]; }
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
                if (content.type === 'variable') {
                    EditDirectiveAvailable.available.push(new AvailableVariable(content));
                }
            }
        };

        (new AvailableStream()).beginStream();
    });

    return Available;
})();