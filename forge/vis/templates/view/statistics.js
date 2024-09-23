DataSocket.resetLoadedRecords();

let time_series_epoch = [];
let bin_data_epoch = [];
let bin_data_month = [];
let bin_data_values = [];
let bin_data_updated = false;

function resetBinTraces() {
    for (const trace of box_hidden_traces) {
        trace.x.length = 0;
        trace.y.length = 0;
        trace.hoverlabel.bgcolor.length = 0;
        if (trace.text) {
            trace.text.length = 0;
        }
    }
}
replotController.handlers.push(() => {
    resetBinTraces();
    const updated = bin_data_updated;
    bin_data_updated = false;
    return updated;
});

class QuantileTracker {
    constructor() {
        this.values = [];
        this._unsorted = false;
    }

    reset() {
        this.values.length = 0;
        this._unsorted = false;
    }

    add(value) {
        if (!isFinite(value)) {
            return;
        }
        this.values.push(value);
        this._unsorted = true;
    }

    addAll(values, start, end) {
        if (start === undefined) {
            start = 0;
        }
        if (end === undefined) {
            end = bins.length;
        }
        for (let i=start; i<end; i++) {
            if (!isFinite(values[i])) {
                continue;
            }
            this.values.push(values[i]);
        }
        this._unsorted = true;
    }

    quantile(q) {
        if (this._unsorted) {
            this.values.sort((a, b) => {
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
                return 0;
            });
            this._unsorted = false;
        }

        if (this.values.length === 0) {
            return undefined;
        }
        if (this.values.length === 1) {
            return this.values[0];
        }
        if (q <= 0) {
            return this.values[0];
        }
        if (q >= 1) {
            return this.values[this.values.length - 1];
        }

        let center = q * (this.values.length - 1);
        const upper = Math.ceil(center);
        const lower = Math.floor(center);
        if (upper === lower) {
            return this.values[lower];
        }

        center -= lower;
        return this.values[upper] * (center) + this.values[lower] * (1.0 - center);
    }
}

class BinSelection {
    constructor(trace, boxes, offset) {
        this._trace = trace;
        this._boxes = boxes;
        this._offset = offset;

        this._start_selection = undefined;
        this._end_selection = undefined;
        this._plot_updated = false;

        this._months = [];
        for (let i=0; i<12; i++) {
            this._months.push(new QuantileTracker());
        }
        this._total = new QuantileTracker();

        replotController.handlers.push(() => {
            return this._replot();
        });
    }

    _selectedData(times, inclusive) {
        function lower_bound(target) {
            let low = 0;
            let high = times.length;

            while (low < high) {
                let mid = Math.floor((low + high) / 2);

                if (times[mid] < target) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        const begin_index = this._start_selection === undefined ? 0 : lower_bound(this._start_selection);
        const end_index = this._end_selection === undefined ? times.length : lower_bound(this._end_selection);
        if (inclusive && end_index < times.length && times[end_index] <= this._end_selection) {
            return [begin_index, end_index+1];
        }
        return [begin_index, end_index];
    }

    _addBox(tracker, center, name) {
        if (tracker.values.length === 0) {
            return;
        }

        const median = tracker.quantile(0.5);
        const q1 = tracker.quantile(0.25);
        const q3 = tracker.quantile(0.75);
        const lower = tracker.quantile(0.05);
        const upper = tracker.quantile(0.95);

        this._boxes.x.push(center);
        this._boxes.median.push(median);
        this._boxes.q1.push(q1);
        this._boxes.q3.push(q3);
        this._boxes.lowerfence.push(lower);
        this._boxes.upperfence.push(upper);

        box_hover_q05.x.push(center);
        box_hover_q05.y.push(lower);
        box_hover_q05.hoverlabel.bgcolor.push(this._boxes.line.color);

        box_hover_q25.x.push(center);
        box_hover_q25.y.push(q1);
        box_hover_q25.hoverlabel.bgcolor.push(this._boxes.line.color);

        box_hover_q50.x.push(center);
        box_hover_q50.y.push(median);
        box_hover_q50.hoverlabel.bgcolor.push(this._boxes.line.color);
        if (tracker.values.length === 1) {
            box_hover_q50.text.push(name + " Median (1 Day)")
        } else {
            box_hover_q50.text.push(name + " Median (" + (tracker.values.length.toString()) + " Days)")
        }

        box_hover_q75.x.push(center);
        box_hover_q75.y.push(q3);
        box_hover_q75.hoverlabel.bgcolor.push(this._boxes.line.color);

        box_hover_q95.x.push(center);
        box_hover_q95.y.push(upper);
        box_hover_q95.hoverlabel.bgcolor.push(this._boxes.line.color);
    }

    _replot() {
        this.reset();

        const [trace_start, trace_end] = this._selectedData(time_series_epoch, true);
        for (let i=trace_start; i<trace_end; i++) {
            this._trace.x.push(data_mean_all.x[i]);
            this._trace.y.push(data_mean_all.y[i]);
        }

        const [bin_start, bin_end] = this._selectedData(bin_data_epoch);
        this._total.addAll(bin_data_values, bin_start, bin_end);
        for (let i=bin_start; i<bin_end; i++) {
            this._months[bin_data_month[i]].add(bin_data_values[i]);
        }

        const months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ];
        for (let i=0; i<12; i++) {
            this._addBox(this._months[i], i+1 + this._offset, months[i]);
        }
        this._addBox(this._total, 13 + this._offset, "Total");

        const updated = this._plot_updated;
        this._plot_updated = false;
        return updated;
    }

    reset() {
        this._trace.x.length = 0;
        this._trace.y.length = 0;

        this._boxes.x.length = 0;
        this._boxes.median.length = 0;
        this._boxes.q1.length = 0;
        this._boxes.q3.length = 0;
        this._boxes.lowerfence.length = 0;
        this._boxes.upperfence.length = 0;

        this._total.reset();
        for (const tracker of this._months) {
            tracker.reset();
        }
    }
    
    get start_selection() {
        return this._start_selection;
    }
    set start_selection(value) {
        if (this._start_selection === value) {
            return;
        }
        this._start_selection = value;
        this._plot_updated = true;
    }
    
    get end_selection() {
        return this._end_selection;
    }
    set end_selection(value) {
        if (this._end_selection === value) {
            return;
        }
        this._end_selection = value;
        this._plot_updated = true;
    }
}
let selected_1 = new BinSelection(data_mean_1, data_box_1, -0.15);
let selected_2 = new BinSelection(data_mean_2, data_box_2, 0.15);

// End padding, so things don't shift during drag
shapeHandler.generators.push(() => {
    if (time_series_epoch.length === 0) {
        return [];
    }
    const x0 = DataSocket.toPlotTime(time_series_epoch[0]);
    const x1 = DataSocket.toPlotTime(time_series_epoch[time_series_epoch.length-1]+1);
    return [{
        type: 'line',
        layer: 'below',
        xref: 'x2',
        yref: 'y2 domain',
        x0: x0,
        x1: x0,
        y0: 0,
        y1: 1,
        line: {
            width: 5,
            color: 'transparent',
        },
    }, {
        type: 'line',
        layer: 'below',
        xref: 'x2',
        yref: 'y2 domain',
        x0: x1,
        x1: x1,
        y0: 0,
        y1: 1,
        line: {
            width: 5,
            color: 'transparent',
        },
    }];
});

class TimeDrag {
    constructor(selection, y0, y1, color) {
        this._selection = selection;
        this._y0 = Math.min(y0, y1);
        this._y1 = Math.max(y0, y1);
        this._color = color;

        this._in_drag = undefined;
        this._drag_center_offset = undefined;

        shapeHandler.generators.push(() => {
            return this._shapes();
        });
    }

    _shapes() {
        let shapes = [];
        if (time_series_epoch.length === 0) {
            return shapes;
        }

        let begin_time = this._selection.start_selection;
        if (begin_time === undefined) {
            begin_time = time_series_epoch[0];
        }
        let end_time = this._selection.end_selection;
        if (end_time === undefined) {
            end_time = time_series_epoch[time_series_epoch.length-1];
        }

        shapes.push({
            type: 'rect',
            layer: 'between',
            xref: 'x2',
            yref: 'y2 domain',
            x0: DataSocket.toPlotTime(begin_time),
            x1: DataSocket.toPlotTime(end_time),
            y0: this._y0,
            y1: this._y1,
            line: {
                width: 0,
                color: 'transparent',
            },
            fillcolor: this._color,
        });

        let middle_y = (this._y0 + this._y1) * 0.5;
        let span_y = Math.abs(this._y0 - this._y1);
        let handle_y0 = middle_y - span_y * 0.3;
        let handle_y1 = middle_y + span_y * 0.3;
        shapes.push({
            type: 'line',
            layer: 'between',
            xref: 'x2',
            yref: 'y2 domain',
            x0: DataSocket.toPlotTime(begin_time),
            x1: DataSocket.toPlotTime(begin_time),
            y0: handle_y0,
            y1: handle_y1,
            line: {
                width: 5,
                color: 'rgba(0,0,0,0.6)',
            },
        });
        shapes.push({
            type: 'line',
            layer: 'between',
            xref: 'x2',
            yref: 'y2 domain',
            x0: DataSocket.toPlotTime(end_time),
            x1: DataSocket.toPlotTime(end_time),
            y0: handle_y0,
            y1: handle_y1,
            line: {
                width: 5,
                color: 'rgba(0,0,0,0.6)',
            },
        });

        return shapes;
    }

    _mouseTarget(x, y) {
        if (y < this._y0 || y > this._y1) {
            return undefined;
        }

        let start = this._selection.start_selection;
        let end = this._selection.end_selection;
        if (start === undefined && time_series_epoch.length > 0) {
            start = time_series_epoch[0];
        }
        if (end === undefined && time_series_epoch.length > 0) {
            end = time_series_epoch[time_series_epoch.length-1];
        }

        let handle_width = 0;
        if (start !== undefined && end !== undefined) {
            handle_width = (end - start) * 0.05;
        }
        handle_width = Math.max(handle_width, 31 * 24 * 60 * 60 * 1000);

        if (start !== undefined && x < start - handle_width) {
            return undefined;
        }
        if (end !== undefined && x > end + handle_width) {
            return undefined;
        }
        if (start !== undefined && x <= start + handle_width) {
            return -1;
        }
        if (end !== undefined && x >= end - handle_width) {
            return 1;
        }
        return 0;
    }

    cursorShape(x, y) {
        if (this._in_drag !== undefined) {
            if (this._in_drag === 0) {
                return "ew-resize";
            }
            return "col-resize";
        }

        const target = this._mouseTarget(x, y);
        if (target === undefined) {
            return undefined;
        }
        if (target === 0) {
            return "ew-resize";
        }
        return "col-resize";
    }

    dragMove(x, y) {
        if (this._in_drag === undefined) {
            return;
        }
        if (time_series_epoch.length === 0) {
            return;
        }

        function roundToMonth(time) {
            let date = (new Date(time));
            date.setUTCMilliseconds(0);
            date.setUTCSeconds(0);
            date.setUTCMinutes(0);
            date.setUTCHours(0);
            date.setUTCDate(1);
            const before = date.getTime();
            if (date.getUTCMonth() === 11) {
                date.setUTCFullYear(date.getUTCFullYear() + 1);
                date.setUTCMonth(0);
            } else {
                date.setUTCMonth(date.getUTCMonth() + 1);
            }
            const after = date.getTime();

            if (Math.abs(before - time) < Math.abs(after - time)) {
                return before;
            } else {
                return after;
            }
        }
        function toMonthCount(time) {
            let date = (new Date(time));
            return date.getUTCFullYear() * 12 + date.getUTCMonth();
        }
        function fromMonthCount(n) {
            return Date.UTC(Math.floor(n / 12), n % 12);
        }

        let new_start = this._selection.start_selection;
        let new_end = this._selection.end_selection;
        if (new_start === undefined) {
            new_start = time_series_epoch[0];
        }
        if (new_end === undefined) {
            new_end = time_series_epoch[time_series_epoch.length - 1];
        }

        if (this._in_drag === -1) {
            new_start = roundToMonth(x);
            new_start = Math.max(new_start, time_series_epoch[0]);
            if (new_start > new_end) {
                const t = new_end;
                new_end = new_start;
                new_start = t;
                this._in_drag = 1;
                new_end = Math.min(new_end, time_series_epoch[time_series_epoch.length-1]);
            }
        } else if (this._in_drag === 1) {
            new_end = roundToMonth(x);
            new_end = Math.min(new_end, time_series_epoch[time_series_epoch.length-1]);
            if (new_end < new_start) {
                const t = new_end;
                new_end = new_start;
                new_start = t;
                this._in_drag = -1;
                new_start = Math.max(new_start, time_series_epoch[0]);
            }
        } else {
            let span_months = toMonthCount(new_end) - toMonthCount(new_start);
            if (span_months < 1) {
                span_months = 1;
            }
            new_start = roundToMonth(x - this._drag_center_offset);
            new_end = fromMonthCount(toMonthCount(new_start) + span_months);
            if (new_start < time_series_epoch[0]) {
                new_start = time_series_epoch[0];
                this._drag_center_offset = x - new_start;
            }
            if (new_end < new_start) {
                new_end = fromMonthCount(toMonthCount(new_start) + 1);
            }
            new_end = Math.min(new_end, time_series_epoch[time_series_epoch.length-1]);
        }

        this._selection.start_selection = new_start
        this._selection.end_selection = new_end;
        replotController.replot(true);
    }

    dragStart(x, y) {
        const target = this._mouseTarget(x, y);
        if (target === undefined) {
            return false;
        }
        this._in_drag = target;
        if (target === 0) {
            this._drag_center_offset = 0;
            if (this._selection.start_selection !== undefined) {
                this._drag_center_offset = x - this._selection.start_selection;
            }
        }
        this.dragMove(x, y);
        return true;
    }

    dragEnd() {
        this._in_drag = undefined;
    }
}
let drag_1 = new TimeDrag(selected_1, 1.0, 0.85, 'rgba(0,0,0,0.15)');
let drag_2 = new TimeDrag(selected_2, 0.15, 0.0, 'rgba(0,128,255,0.15)');

plot.then(() => {
    const drag_controllers = [drag_1, drag_2];
    const drag_layer = replotController.div.getElementsByClassName('draglayer')[0];

    function toDragCoordinates(x, y, allow_out_of_bounds) {
        const time_series_area = drag_layer.getElementsByClassName("x2y2")[0]
            .getElementsByClassName("nsewdrag")[0]
            .getBoundingClientRect();
        if (x < time_series_area.left ||
            x > time_series_area.right ||
            y < time_series_area.top ||
            y > time_series_area.bottom) {
            if (!allow_out_of_bounds) {
                return undefined;
            }
        }
        const fl = replotController.div._fullLayout;
        const x_data = fl.xaxis2.p2c(x - time_series_area.left);
        const y_domain = 1.0 - (y - time_series_area.top) / time_series_area.height;
        return [x_data, y_domain];
    }

    function disableZoom(d) {
        replotController.layout.xaxis2.fixedrange = d;
        replotController.layout.yaxis2.fixedrange = d;
        Plotly.relayout(replotController.div, {
            'xaxis2.fixedrange': d,
            'yaxis2.fixedrange': d,
        });
    }

    drag_layer.addEventListener('mousemove', function(event) {
        const cursor_layer = drag_layer.getElementsByClassName("x2y2")[0].getElementsByClassName("nsewdrag")[0];

        const coords = toDragCoordinates(event.clientX, event.clientY);
        if (coords !== undefined) {
            const [x, y] = coords;
            for (const d of drag_controllers) {
                const cursor = d.cursorShape(x, y);
                if (cursor !== undefined) {
                    disableZoom(true);
                    cursor_layer.style.cursor = cursor;
                    return;
                }
            }
        }

        cursor_layer.style.cursor = '';
        disableZoom(false);
    });

    function dragMove(event) {
        const coords = toDragCoordinates(event.clientX, event.clientY, true);
        if (coords === undefined) {
            return;
        }
        const [x, y] = coords;
        for (const d of drag_controllers) {
            d.dragMove(x, y);
        }
    }
    function dragStart(event) {
        const coords = toDragCoordinates(event.clientX, event.clientY);
        if (coords === undefined) {
            return false;
        }
        const [x, y] = coords;
        for (const d of drag_controllers) {
            if (d.dragStart(x, y)) {
                disableZoom(true);
                return true;
            }
        }
        return false;
    }
    function dragEnd(event) {
        for (const d of drag_controllers) {
            d.dragEnd();
        }
        document.removeEventListener('mousemove', dragMove);
        document.removeEventListener('touchmove', dragMove)
        document.removeEventListener('mouseup', dragEnd);
        document.removeEventListener('touchend', dragEnd);
    }

    document.addEventListener('mousedown', (event) => {
        if (dragStart(event)) {
            event.stopImmediatePropagation();
            document.addEventListener('mouseup', dragEnd);
            document.addEventListener('auxclick', dragEnd);
            document.addEventListener('mousemove', dragMove);
        }
    }, {passive: false});
    document.addEventListener('touchstart', (event) => {
        if (dragStart(event)) {
            event.stopImmediatePropagation();
            document.addEventListener('touchend', dragEnd);
            document.addEventListener('touchmove', dragMove);
        }
    }, {passive: false});
});

let all_month_quantiles = [];
for (let i=0; i<12; i++) {
    all_month_quantiles.push(new QuantileTracker());
}
class TotalCycleTrace {
    constructor(trace, quantile, quantile_mirror) {
        this._trace = trace;
        this._quantile = quantile;
        this._quantile_mirror = quantile_mirror;

        replotController.handlers.push(() => {
            return this._replot();
        });
    }

    _replot() {
        this._trace.y.length = 0;
        this._trace.x.length = 0;

        for (let i=0; i<time_series_epoch.length; i++) {
            this._trace.x.push(data_mean_all.x[i]);

            let month = (new Date(time_series_epoch[i])).getUTCMonth();
            this._trace.y.push(all_month_quantiles[month].quantile(this._quantile));
        }
        if (this._quantile_mirror !== undefined) {
            for (let i=time_series_epoch.length-1; i>=0; i--) {
                this._trace.x.push(data_mean_all.x[i]);

                let month = (new Date(time_series_epoch[i])).getUTCMonth();
                this._trace.y.push(all_month_quantiles[month].quantile(this._quantile_mirror));
            }
        }
    }
}
let cycle_shade = new TotalCycleTrace(data_shade, 0.25, 0.75);
let cycle_mid = new TotalCycleTrace(data_mid, 0.5);
let cycle_upper = new TotalCycleTrace(data_upper, 0.95);
let cycle_lower = new TotalCycleTrace(data_lower, 0.05);

function incomingTimeseries(plotTime, values, epoch) {
    if (values.length === 0) {
        return;
    }

    for (let i=0; i<plotTime.length; i++) {
        data_mean_all.x.push(plotTime[i]);
        data_mean_all.y.push(values[i]);
        time_series_epoch.push(epoch[i]);
    }

    replotController.replot();
}
function finishedTimeseries() {
    if (time_series_epoch.length === 0) {
        replotController.replot(true);
        return;
    }
    if (selected_1.start_selection === undefined) {
        selected_1.start_selection = time_series_epoch[0];
    }
    if (selected_1.end_selection === undefined) {
        selected_1.end_selection = time_series_epoch[time_series_epoch.length-1];
    }
    if (selected_2.end_selection === undefined) {
        selected_2.end_selection = time_series_epoch[time_series_epoch.length-1];
    }
    if (selected_2.start_selection === undefined) {
        const d = new Date(selected_2.end_selection);
        d.setUTCFullYear(d.getUTCFullYear() - 1);
        selected_2.start_selection = d.getTime();
    }

    replotController.replot(true);
}
DataSocket.addLoadedRecordField('{{ view.timeseries_record }}', 'value', incomingTimeseries,
    dataRecordLoader, finishedTimeseries);

function incomingBinData(plotTime, values, epoch) {
    for (let i=0; i<plotTime.length; i++) {
        const point_value = values[i];
        const point_epoch = epoch[i];
        const point_month = (new Date(point_epoch)).getUTCMonth();

        bin_data_values.push(point_value);
        bin_data_epoch.push(point_epoch);
        bin_data_month.push(point_month);
        all_month_quantiles[point_month].add(point_value);
    }
    bin_data_updated = true;
    replotController.replot();
}
DataSocket.addLoadedRecordField('{{ view.bins_record }}', 'value', incomingBinData,
    dataRecordLoader, () => { replotController.replot(true); });


DataSocket.onRecordReload = function() {
    time_series_epoch.length = 0;
    resetBinTraces();
    data_mean_all.x.length = 0;
    data_mean_all.y.length = 0;
    selected_1.reset();
    selected_2.reset();

    for (const tracker of all_month_quantiles) {
        tracker.reset();
    }

    bin_data_values.length = 0;
    bin_data_epoch.length = 0;
    bin_data_month.length = 0;

    replotController.replot();
};

TimeSelect.start_ms = 1;
TimeSelect.end_ms = (new Date()).getTime();
DataSocket.startLoadingRecords();
