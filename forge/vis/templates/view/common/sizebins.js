var SizeBins = {};
(function() {
    SizeBins.AverageBins = class {
        constructor(data, xaxis, yaxis) {
            this.xaxis = xaxis;
            this.yaxis = yaxis;

            this.data = data;
            this._meanIndex = this.data.length;
            this.data.push({
                x: [ ],
                y: [ ],
                mode: 'lines',
                xaxis: this.xaxis,
                yaxis: this.yaxis,
                showlegend: false,
                hovertemplate: "%{y:.2f}",
                name: "Mean",
                line: {
                    width: 2,
                    color: '#000',
                },
            });

            this._startIndex = this.data.length;
        }

        _getAverageFitPoint(index) {
            const fit = this.data[this._meanIndex];
            while (fit.x.length <= index) {
                fit.x.push(Number.NaN);
                fit.y.push(Number.NaN);
            }
            return fit;
        }

        _getAverageBin(index) {
            const dataIndex = this._startIndex + index;
            while (this.data.length <= dataIndex) {
                this.data.push({
                    x0: undefined,
                    y: [],
                    type: 'box',
                    showlegend: false,
                    xaxis: this.xaxis,
                    yaxis: this.yaxis,
                    boxpoints: false,
                    orientation: 'v',
                    line: {
                        color: '#000',
                        width: 1,
                    },
                    fillcolor: 'rgba(0, 0, 0, 0)',
                });
            }
            return this.data[dataIndex];
        }

        setDiameter(index, diameter) {
            this._getAverageFitPoint(index).x[index] = diameter;

            const averageBin = this._getAverageBin(index);
            const changed = (averageBin.x0 !==  diameter);
            averageBin.x0 = diameter;
            averageBin.name = diameter.toFixed(3) + "μm";
            return changed;
        }

        updateBin(index) {
            const averageBin = this._getAverageBin(index);

            function fitMean(y) {
                let sum = 0.0;
                let count = 0;
                for (const v of y) {
                    sum += v;
                    count++;
                }
                if (count === 0) {
                    return Number.NaN;
                }
                return sum / count;
            }

            function quantile(sorted, q) {
                if (sorted.length === 0) {
                    return undefined;
                }
                if (sorted.length === 1) {
                    return sorted[0];
                }
                let center = q * (sorted.length - 1);
                const upper = Math.ceil(center);
                const lower = Math.floor(center);
                if (upper === lower) {
                    return sorted[lower];
                }

                center -= lower;
                return sorted[upper] * (center) + sorted[lower] * (1.0 - center);
            }

            const sorted = averageBin.y.slice();
            for (let i=0; i<sorted.length; ) {
                if (!isFinite(sorted[i])) {
                    sorted.splice(i, 1);
                } else {
                    i++;
                }
            }
            sorted.sort((a, b) => {
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
                return 0;
            });

            if (sorted.length === 0) {
                this._getAverageFitPoint(index).y[index] = undefined;
                averageBin.visible = false;
                return;
            }

            averageBin.visible = true;
            averageBin.median = [quantile(sorted, 0.5)];
            averageBin.q1 = [quantile(sorted, 0.25)];
            averageBin.q3 = [quantile(sorted, 0.75)];
            averageBin.lowerfence = [quantile(sorted, 0.05)];
            averageBin.upperfence = [quantile(sorted, 0.95)];

            this._getAverageFitPoint(index).y[index] = fitMean(sorted);
        }

        addPoint(index, v, epoch_ms) {
            if (!isFinite(v)) {
                return;
            }

            const averageBin = this._getAverageBin(index);
            averageBin.y.push(v);
        }

        recalculateBins() {
            for (let i=this._startIndex; i<this.data.length; i++) {
                this.updateBin(i - this._startIndex);
            }
        }
    }

    SizeBins.RealtimeAverageBins = class extends SizeBins.AverageBins {
        constructor(data, xaxis, yaxis) {
            super(data, xaxis, yaxis);

            this._dataTimes = [];
        }

        _getDataTime(index) {
            while (this._dataTimes.length <= index) {
                this._dataTimes.push([]);
            }
            return this._dataTimes[index];
        }

        addPoint(index, v, epoch_ms) {
            const averageBin = this._getAverageBin(index);
            averageBin.y.push(v);

            const times = this._getDataTime(index);
            times.push(epoch_ms);

            TimeSelect.setIntervalBounds();
            let discardCutoff = TimeSelect.start_ms;
            if (TimeSelect.isZoomed()) {
                discardCutoff = Math.min(discardCutoff, TimeSelect.zoom_start_ms);
            }

            let countDiscard = 0;
            for (; countDiscard<times.length; countDiscard++) {
                if (times[countDiscard] >= discardCutoff) {
                    break;
                }
            }
            if (countDiscard > 0) {
                times.slice(0, countDiscard);
                averageBin.y.slice(0, countDiscard);
            }
        }
    }
})();