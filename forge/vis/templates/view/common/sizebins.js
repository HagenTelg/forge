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

        updateFit(index, averageBin) {
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

            this._getAverageFitPoint(index).y[index] = fitMean(averageBin.y);
        }

        addPoint(index, v, epoch_ms) {
            if (!isFinite(v)) {
                return undefined;
            }

            const averageBin = this._getAverageBin(index);
            averageBin.y.push(v);
            this.updateFit(index, averageBin);
            return averageBin;
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
            const averageBin = super.addPoint(index, v, epoch_ms);
            if (!averageBin) {
                return;
            }

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
            if (countDiscard <= 0) {
                return;
            }

            times.slice(0, countDiscard);
            averageBin.y.slice(0, countDiscard);
            this.updateFit(index, averageBin);
        }
    }
})();