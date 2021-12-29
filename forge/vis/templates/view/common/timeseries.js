var TimeSeriesCommon = {};
(function() {
    TimeSeriesCommon.getXAxis = function() {
        const result = {
            title: "UTC",
            type: 'date',
            hoverformat: '%Y-%m-%d %H:%M:%S',
            tickformat: '%H:%M\n %Y-%m-%d',
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
                    opacity: 0.9,
                    line: {
                        width: 5,
                        color: '#000000',
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

    let hideContaminatedData = false;
    TimeSeriesCommon.addContaminationToggleButton = function(traces) {
        const button = document.createElement('button');
        document.getElementById('control_bar').appendChild(button);
        button.classList.add('mdi', 'mdi-filter');
        button.classList.add('contamination-toggle');
        button.classList.add('hidden');
        button.title = 'Toggle displaying contaminated data';
        $(button).click(function(event) {
            event.preventDefault();
            hideContaminatedData = !hideContaminatedData;
            $(this).toggleClass('mdi-filter mdi-filter-off');
            traces.updateDisplay(true);
        });
    }

    TimeSeriesCommon.addSymbolToggleButton = function(traces) {
        const button = document.createElement('button');
        document.getElementById('control_bar').appendChild(button);
        button.classList.add('mdi', 'mdi-chart-line-variant');
        button.title = 'Toggle data point symbol display';
        $(button).click(function(event) {
            event.preventDefault();
            $(this).toggleClass('mdi-chart-bubble mdi-chart-line-variant');
            traces.data.forEach((trace) => {
                if (trace.mode === 'lines') {
                    trace.mode = 'lines+markers';
                } else if (trace.mode === 'lines+markers') {
                    trace.mode = 'lines';
                }
            });
            traces.updateDisplay(true);
        });
    }

    const AVERAGE_NONE = 0;
    const AVERAGE_HOUR = 1;
    const AVERAGE_DAY = 2;
    const AVERAGE_MONTH = 3;
    let averagingMode = AVERAGE_NONE;
    TimeSeriesCommon.addAveragingButton = function(traces) {
        const button = document.createElement('button');
        document.getElementById('control_bar').appendChild(button);
        button.classList.add('mdi', 'mdi-sine-wave');
        button.title = 'Cycle data averaging modes (current: no averaging)';
        $(button).click(function(event) {
            event.preventDefault();
            switch (averagingMode) {
            case AVERAGE_NONE:
                if (!hideContaminatedData) {
                    $('button.contamination-toggle').click();
                }
                averagingMode = AVERAGE_HOUR;
                button.classList.remove('mdi', 'mdi-sine-wave');
                button.classList.add('character-icon');
                button.textContent = "H";
                button.title = 'Cycle data averaging modes (current: one hour average)';
                break;
            case AVERAGE_HOUR:
                averagingMode = AVERAGE_DAY;
                button.classList.remove('mdi', 'mdi-sine-wave');
                button.classList.add('character-icon');
                button.textContent = "D";
                button.title = 'Cycle data averaging modes (current: one day average)';
                break;
            case AVERAGE_DAY:
                averagingMode = AVERAGE_MONTH;
                button.classList.remove('mdi', 'mdi-sine-wave');
                button.classList.add('character-icon');
                button.textContent = "M";
                button.title = 'Cycle data averaging modes (current: monthly average)';
                break;
            default:
                averagingMode = AVERAGE_NONE;
                button.classList.add('mdi', 'mdi-sine-wave');
                button.classList.remove('character-icon');
                button.textContent = "";
                button.title = 'Cycle data averaging modes (current: no averaging)';
                break;
            }
            traces.updateDisplay(true);
        });
    }

    TimeSeriesCommon.installZoomHandler = function(div, realtime) {
        div.on('plotly_relayout', function(data) {
            const start_time = data['xaxis.range[0]'];
            const end_time = data['xaxis.range[1]'];
            if (!start_time || !end_time) {
                return;
            }

            const start_ms = DataSocket.fromPlotTime(start_time);
            const end_ms = DataSocket.fromPlotTime(end_time);
            if (realtime) {
                if ((end_ms - start_ms + 1001) >= TimeSelect.interval_ms) {
                    TimeSelect.zoom(undefined, undefined);
                    return;
                }
            } else {
                if (start_ms === TimeSelect.start_ms && end_ms === TimeSelect.end_ms) {
                    TimeSelect.zoom(undefined, undefined);
                    return;
                }
            }

            TimeSelect.zoom(start_ms, end_ms);
        });
        TimeSelect.applyZoom = function(start_ms, end_ms) {
            Plotly.relayout(div, {
                'xaxis.range[0]': DataSocket.toPlotTime(start_ms),
                'xaxis.range[1]': DataSocket.toPlotTime(end_ms),
            });
        }
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

    class DataFilter {
        constructor(traces, contaminationRecord) {
            this.epoch_ms = [];
            this.x = undefined;
            this.y = undefined;

            this.contaminationSegments = [];
            if (contaminationRecord && contaminationRecord !== '') {
                DataSocket.addLoadedRecord(contaminationRecord,
                    (dataName) => {
                        return new Contamination.DataStream(dataName);
                    }, (start_ms, end_ms) => {
                        if (this.contaminationSegments.length === 0) {
                            $('button.contamination-toggle.hidden').removeClass('hidden');
                        }
                        this.contaminationSegments.push({
                            start_ms: start_ms,
                            end_ms: end_ms,
                        });
                        traces.updateDisplay();
                    }, () => {
                        traces.updateDisplay(true);
                    }
                );
            }
        }

        extendData(times, values, epoch) {
            for (let i=0; i<epoch.length; i++) {
                if (this.x) {
                    this.x.push(times[i]);
                }
                if (this.y) {
                    this.y.push(values[i]);
                }
                this.epoch_ms.push(epoch[i]);
            }
        }

        clearData() {
            this.epoch_ms.length = 0;
            this.y = undefined;
            this.x = undefined;
            this.contaminationSegments.length = 0;
            $('button.contamination-toggle').addClass('hidden');
        }
        
        needToApplyContamination() {
            return hideContaminatedData && this.contaminationSegments.length !== 0;
        }

        needToApplyAveraging() {
            return averagingMode !== AVERAGE_NONE;
        }

        filterDataContamination(data) {
            data.y.length = 0;
            let segmentIndex = 0;
            let i;
            for (i=0; i<this.epoch_ms.length; i++) {
                const epoch_ms = this.epoch_ms[i];

                for (; segmentIndex < this.contaminationSegments.length; segmentIndex++) {
                    const segment = this.contaminationSegments[segmentIndex];
                    if (epoch_ms >= segment.end_ms) {
                        continue;
                    }
                    break;
                }
                if (segmentIndex >= this.contaminationSegments.length) {
                    break;
                }

                const segment = this.contaminationSegments[segmentIndex];
                if (epoch_ms < segment.start_ms) {
                    data.y.push(this.y[i]);
                } else {
                    data.y.push(undefined);
                }
            }

            for (; i<this.epoch_ms.length; i++) {
                data.y.push(this.y[i]);
            }
        }

        averageData(data) {
            if (this.epoch_ms.length === 0) {
                return;
            }

            function toAverageBegin(epoch_ms) {
                switch (averagingMode) {
                case AVERAGE_HOUR:
                    return Math.floor(epoch_ms / (60 * 60 * 1000)) * (60 * 60 * 1000);
                case AVERAGE_DAY:
                    return Math.floor(epoch_ms / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
                case AVERAGE_MONTH:
                    let date = new Date(Math.floor(epoch_ms));
                    date.setUTCMilliseconds(0);
                    date.setUTCSeconds(0);
                    date.setUTCMinutes(0);
                    date.setUTCHours(0);
                    date.setUTCDate(1);
                    return date.getTime();
                default:
                    throw 'Unsupported averaging mode';
                }
            }
            function toAverageEnd(epoch_ms) {
                switch (averagingMode) {
                case AVERAGE_HOUR:
                    return epoch_ms + (60 * 60 * 1000);
                case AVERAGE_DAY:
                    return epoch_ms + (24 * 60 * 60 * 1000);
                case AVERAGE_MONTH:
                    let date = new Date(Math.floor(epoch_ms));
                    date.setUTCMilliseconds(0);
                    date.setUTCSeconds(0);
                    date.setUTCMinutes(0);
                    date.setUTCHours(0);
                    date.setUTCDate(1);
                    if (date.getUTCMonth() === 11) {
                        date.setUTCFullYear(date.getUTCFullYear() + 1);
                        date.setUTCMonth(0);
                    } else {
                        date.setUTCMonth(date.getUTCMonth() + 1);
                    }
                    return date.getTime();
                default:
                    throw 'Unsupported averaging mode';
                }
            }

            let outputX = [];
            let outputY = [];

            let averageBegin = toAverageBegin(this.epoch_ms[0]);
            let averageEnd = toAverageEnd(averageBegin);
            outputX.push(DataSocket.toPlotTime(averageBegin));

            let sumY = 0;
            let countY = 0;
            for (let i=0; i<this.epoch_ms.length; i++) {
                if (this.epoch_ms[i] < averageEnd) {
                    if (isFinite(data.y[i])) {
                        sumY += data.y[i];
                        countY++;
                    }
                    continue;
                }
                if (countY > 0) {
                    outputY.push(sumY / countY);
                } else {
                    outputY.push(undefined);
                }
                sumY = 0;
                countY = 0;

                averageBegin = averageEnd;
                averageEnd = toAverageEnd(averageBegin);
                while (averageEnd <= this.epoch_ms[i]) {
                    outputX.push(DataSocket.toPlotTime(averageBegin));
                    outputY.push(undefined);

                    averageBegin = averageEnd;
                    averageEnd = toAverageEnd(averageBegin);
                }

                outputX.push(DataSocket.toPlotTime(averageBegin));
                if (isFinite(data.y[i])) {
                    sumY += data.y[i];
                    countY++;
                }
            }
            if (countY > 0) {
                outputY.push(sumY / countY);
            } else {
                outputY.push(undefined);
            }

            data.x = outputX;
            data.y = outputY;
        }

        apply(data) {
            const applyContamination = this.needToApplyContamination();
            const applyAveraging = this.needToApplyAveraging();

            if (!applyContamination && !applyAveraging) {
                if (this.y) {
                    data.y = this.y;
                    this.y = undefined;
                }
                if (this.x) {
                    data.x = this.x;
                    this.x = undefined;
                }
                return;
            }
            if (!this.y) {
                this.y = data.y.slice();
            }

            data.y.length = 0;
            for (let i=0; i<this.y.length; i++) {
                data.y.push(this.y[i]);
            }

            if (applyContamination) {
                if (this.x) {
                    data.x.length = 0;
                    for (let i=0; i<this.x.length; i++) {
                        data.x.push(this.x[i]);
                    }
                }

                this.filterDataContamination(data);
            }

            if (applyAveraging) {
                if (!this.x) {
                    this.x = data.x.slice();
                }
                this.averageData(data);
            }
        }

        discardBefore(data, cutoff) {
            let countDiscard = 0;
            for (; countDiscard<this.epoch_ms.length; countDiscard++) {
                if (this.epoch_ms[countDiscard] >= cutoff) {
                    break;
                }
            }
            if (countDiscard <= 0) {
                return;
            }

            this.epoch_ms.splice(0, countDiscard);
            if (this.x) {
                this.x.splice(0, countDiscard);
            } else {
                data.x.splice(0, countDiscard);
            }
            if (this.y) {
                this.y.splice(0, countDiscard);
            } else {
                data.y.splice(0, countDiscard);
            }
        }
    }

    TimeSeriesCommon.Traces = class {
        constructor(div, data, layout, config) {
            this.div = div;
            this.data = data;
            this.layout = layout;
            this.config = config;
            this._queuedDisplayUpdate = undefined;
            this._dataFilters = new Map();
        }

        applyUpdate() {
            this._dataFilters.forEach((handler, traceIndex) => {
                handler.apply(this.data[traceIndex]);
            });

            this.layout.datarevision++;
            Plotly.react(this.div, this.data, this.layout, this.config);
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
                this.applyUpdate();
            }, delay);
        }

        applyDataFilter(traceIndex, contaminationRecord) {
            this._dataFilters.set(traceIndex, new DataFilter(this, contaminationRecord));
        }

        extendData(traceIndex, times, values, epoch) {
            const data = this.data[traceIndex];
            const filter = this._dataFilters.get(traceIndex);
            if (filter) {
                filter.extendData(times, values, epoch);
            }
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
            });
        }

        clearAllData() {
            this.data.forEach((data, traceIndex) => {
                if (data.x) {
                    data.x.length = 0;
                }
                if (data.y) {
                    data.y.length = 0;
                }
                if (data.z) {
                    data.z.length = 0;
                }

                const filter = this._dataFilters.get(traceIndex);
                if (filter) {
                    filter.clearData();
                }
            });
        }
    }

    TimeSeriesCommon.RealtimeTraces = class extends TimeSeriesCommon.Traces {
        constructor(div, data, layout, config) {
            super(div, data, layout, config);
        }

        updateTimeBounds() {
            TimeSelect.setIntervalBounds();
            super.updateTimeBounds();
        }

        extendData(traceIndex, times, values, epoch) {
            super.extendData(traceIndex, times, values, epoch);

            TimeSelect.setIntervalBounds();
            let discardCutoff = TimeSelect.start_ms;
            if (!TimeSelect.isZoomed()) {
                this.layout.xaxis.range = [DataSocket.toPlotTime(TimeSelect.start_ms),
                    DataSocket.toPlotTime(TimeSelect.end_ms)];
            } else {
                discardCutoff = Math.min(discardCutoff, TimeSelect.zoom_start_ms);
            }

            const data = this.data[traceIndex];
            const filter = this._dataFilters.get(traceIndex);
            if (filter) {
                 filter.discardBefore(data, discardCutoff);
            } else {
                let countDiscard = 0;
                for (; countDiscard<data.x.length; countDiscard++) {
                    const pointTime = DataSocket.fromPlotTime(data.x[countDiscard]);
                    if (pointTime >= discardCutoff) {
                        break;
                    }
                }

                if (countDiscard > 0) {
                    if (data.x) {
                        data.x.splice(0, countDiscard);
                    }
                    if (data.y) {
                        data.y.splice(0, countDiscard);
                    }
                    if (data.z) {
                        data.z.splice(0, countDiscard);
                    }
                }
            }

            this.updateDisplay();
        }

        applyUpdate() {
            super.applyUpdate();
            TimeSeriesCommon.updateShapes();
        }
    }
})();