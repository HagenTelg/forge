var ReplotController = (function() {
    return class {
        constructor(div, data, layout, config) {
            this.div = div;
            this.data = data;
            this.layout = layout;
            this.config = config;

            this._queuedReplot = undefined;
            this._needDataErase = false;
            this._queueDelay = 500;

            this._nextErase = Date.now() + 1000;
            this._checkErase = undefined;

            this.handlers = [];
        }

        replot(immediate) {
            if (this._queuedReplot) {
                if (!immediate) {
                    return;
                }
                clearTimeout(this._queuedReplot);
                this._queuedReplot = undefined;
            }

            let delay = this._queueDelay;
            if (immediate) {
                delay = 0;
            }
            const performDataErase = immediate;

            this._queuedReplot = setTimeout(() => {
                this._queuedReplot = undefined;

                this.handlers.forEach((gen) => {
                    const changed = gen();
                    if (changed) {
                        this._needDataErase = true;
                        this.layout.datarevision++;
                    }
                });

                let doErase = performDataErase && this._needDataErase;
                if (!doErase && this._needDataErase) {
                    const now = Date.now();
                    const remaining = this._nextErase - now;
                    if (remaining <= 0) {
                        doErase = true;
                        if (this._checkErase) {
                            clearTimeout(this._checkErase);
                            this._checkErase = undefined;
                        }
                    } else {
                        if (!this._checkErase) {
                            this._checkErase = setTimeout(() => {
                                this._checkErase = undefined;
                                if (this._queuedReplot) {
                                    return;
                                }
                                this.replot(true);
                            }, remaining+1);
                        }
                    }
                }

                let replotTime = 0;

                // Plotly misbehaves for sequential redraws resulting in "phantom" data (see SPO optical, all of 2022)
                if (doErase) {
                    this._needDataErase = false;
                    this._nextErase = Date.now() + this._queueDelay;

                    Plotly.react(this.div, [], this.layout, this.config);
                    this.layout.datarevision++;
                    const begin = Date.now();
                    Plotly.react(this.div, this.data, this.layout, this.config);
                    replotTime = Date.now() - begin;
                } else {
                    const begin = Date.now();
                    Plotly.react(this.div, this.data, this.layout, this.config);
                    replotTime = Date.now() - begin;
                }

                this._queueDelay = replotTime;
                if (this._queueDelay < 500) {
                    this._queueDelay = 500;
                } else if (this._queueDelay > 2000) {
                    this._queueDelay = 2000;
                }
            }, delay);
        }
    };
})();