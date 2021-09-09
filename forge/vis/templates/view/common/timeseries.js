var TimeSeriesCommon = {};
(function() {
    TimeSeriesCommon.getXAxis = function() {
        const result = {
            title: "UTC",
            type: 'date',
            zeroline: false,
            autorange: false,
            range: [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)],
        };

        if (localStorage.getItem('forge-settings-time-format') === 'doy') {
            result.hoverformat = "%Y:%-j.%k";
            result.tickformat = "%-j.%k\n %Y";
        }

        return result;
    }

    const emptyShapes = [];

    TimeSeriesCommon.updateShapes = function() { }

    const selectedTimeHighlights = [];
    TimeSeriesCommon.getTimeHighlights = function() { return selectedTimeHighlights; }
    TimeSelect.onHighlight( (start_ms, end_ms) => {
        selectedTimeHighlights.length = 0;
        if (start_ms === undefined && end_ms === undefined) {
            TimeSeriesCommon.updateShapes();
            return;
        }
        if (!start_ms) {
            start_ms = TimeSelect.start_ms;
        }
        if (!end_ms) {
            end_ms = TimeSelect.end_ms;
        }

        if (start_ms === end_ms) {
            selectedTimeHighlights.push({
                type: 'line',
                xref: 'x',
                yref: 'paper',
                x0: DataSocket.toPlotTime(start_ms),
                y0: 0,
                x1: DataSocket.toPlotTime(end_ms),
                y1: 1,
                opacity: 0.6,
                line: {
                    width: 1,
                    color: '#000000',
                }
            });
        } else {
            selectedTimeHighlights.push({
                type: 'rect',
                xref: 'x',
                yref: 'paper',
                x0: DataSocket.toPlotTime(start_ms),
                y0: 0,
                x1: DataSocket.toPlotTime(end_ms),
                y1: 1,
                fillcolor: '#d3d3d3',
                opacity: 0.2,
                line: {
                    width: 0
                }
            });
        }

        TimeSeriesCommon.updateShapes();
    });

    const contaminationShapes = new Map();
    let queuedContaminationUpdate = undefined;
    function updateContaminationDisplay(immediate) {
        if (!queuedContaminationUpdate) {
            if (!immediate) {
                return;
            }
            clearTimeout(queuedContaminationUpdate);
            queuedContaminationUpdate = undefined;
        }

        let delay = 500;
        if (immediate) {
            delay = 0;
        }

        queuedContaminationUpdate = setTimeout(() => {
            TimeSeriesCommon.updateShapes();
        }, delay);
    }
    TimeSeriesCommon.clearContamination = function() {
        contaminationShapes.clear();
    }
    TimeSeriesCommon.installContamination = function(key, record, yref) {
        DataSocket.addLoadedRecord(record, (dataName) => { return new Contamination.DataStream(dataName); },
            (start_ms, end_ms, flags) => {
                let target = contaminationShapes.get(key);
                if (target === undefined) {
                    target = [];
                    contaminationShapes.set(key, target);
                }
                target.push({
                    type: 'line',
                    layer: 'below',
                    xref: 'x',
                    yref: yref + ' domain',
                    x0: DataSocket.toPlotTime(start_ms),
                    x1: DataSocket.toPlotTime(end_ms),
                    y0: 0.05,
                    y1: 0.05,
                    fillcolor: '#000000',
                    opacity: 0.9,
                    line: {
                        width: 5,
                    },
                });

                updateContaminationDisplay();
            }, () => {
                updateContaminationDisplay(true);
            });

        return (() => {
            const shapes = contaminationShapes.get(key);
            if (shapes === undefined) {
                return emptyShapes;
            }
            return shapes;
        });
    }

    TimeSeriesCommon.installZoomHandler = function(div) {
        div.on('plotly_relayout', function(data) {
            const start_time = data['xaxis.range[0]'];
            const end_time = data['xaxis.range[1]'];
            if (!start_time || !end_time) {
                return;
            }

            const start_ms = DataSocket.fromPlotTime(start_time);
            const end_ms = DataSocket.fromPlotTime(end_time);
            if (start_ms === TimeSelect.start_ms && end_ms === TimeSelect.end_ms) {
                TimeSelect.zoom(undefined, undefined);
                return;
            }

            TimeSelect.zoom(start_ms, end_ms);
        });
    }

    TimeSeriesCommon.installSpikeToggleHandler = function(div) {
        div.on('plotly_relayout', function(data) {
            const showspikes = data['xaxis.showspikes'];
            if (showspikes === undefined) {
                return;
            }
            if (showspikes) {
                Plotly.relayout(div, {
                    hovermode: 'closest',
                });
            } else {
                Plotly.relayout(div, {
                    hovermode: 'x',
                });
            }
        });
    }

    TimeSeriesCommon.Traces = class {
        constructor(div, data, layout, config) {
            this.div = div;
            this.data = data;
            this.layout = layout;
            this.config = config;
            this._queuedDisplayUpdate = undefined;
        }

        updateDisplay(immediate) {
            if (this._queuedDisplayUpdate) {
                if (!immediate) {
                    return;
                }
                clearTimeout(this._queuedDisplayUpdate);
                this._queuedDisplayUpdate = undefined;
            }

            let delay = 500;
            if (immediate) {
                delay = 0;
            }

            this._queuedDisplayUpdate = setTimeout(() => {
                this._queuedDisplayUpdate = undefined;
                this.layout.datarevision++;
                Plotly.react(this.div, this.data, this.layout, this.config);
            }, delay);
        }

        extendData(traceIndex, times, values) {
            const data = this.data[traceIndex];
            for (let i=0; i<times.length; i++) {
                data.x.push(times[i]);
                data.y.push(values[i]);
            }
            this.updateDisplay();
        }
        updateTimeBounds() {
            Plotly.relayout(this.div, {
                'xaxis.range': [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)],
                'xaxis.autorange': false,
            })
        }
    }
})();