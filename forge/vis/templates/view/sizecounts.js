shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }

TimeSeriesCommon.addSymbolToggleButton(traces);
//{% if not realtime %}
TimeSeriesCommon.addContaminationToggleButton(traces);
//{% endif %}
TimeSeriesCommon.addAveragingButton(traces);

DataSocket.resetLoadedRecords();

//{% if view.contamination %}
shapeHandler.generators.push(TimeSeriesCommon.installContamination(1,
    '{{ view.contamination }}', 'y'));
//{% endif %}


function incomingDp(plotTime, values, epoch) {
    if (values.length === 0) {
        return;
    }
    const Dp = values[values.length - 1];
    if (!Array.isArray(Dp)) {
        return;
    }

    let changed = false;
    for (let i=0; i<Dp.length; i++) {
        const d = Dp[i];
        if (!isFinite(d)) {
            return;
        }
        if (bins.setDiameter(i, d)) {
            changed = true;
        }
    }

    if (!changed) {
        return;
    }
    traces.updateDisplay();
}
DataSocket.addLoadedRecordField('{{ view.size_record }}', 'Dp', incomingDp,
    RecordProcessing.get('{{ view.size_record }}'),
    () => { traces.updateDisplay(true); });

function incomingdN(plotTime, values, epoch) {
    if (plotTime.length === 0) {
        return;
    }

    for (let timeIndex=0; timeIndex<plotTime.length; timeIndex++) {
        const dN = values[timeIndex];
        if (!Array.isArray(dN)) {
            continue;
        }
        const addTime = epoch[timeIndex];
        for (let binIndex=0; binIndex<dN.length; binIndex++) {
            bins.addPoint(binIndex, dN[binIndex], addTime);
        }
    }

    traces.updateDisplay();
}
DataSocket.addLoadedRecordField('{{ view.size_record }}', 'dN', incomingdN,
    RecordProcessing.get('{{ view.size_record }}'),
    () => { traces.updateDisplay(true); });

// {% for trace in view.traces %}
//  {% if trace.data_record and trace.data_field %}
(function(traceIndex) {
    let incomingData = (plotTime, values, epoch) => {
        traces.extendData(traceIndex, plotTime, values, epoch);
    };

    // {% if trace.script_incoming_data %}{{ '\n' }}{{ trace.script_incoming_data | safe }}{% endif %}

    traces.applyDataFilter(traceIndex, '{{ view.contamination and view.contamination or "" }}');


    DataSocket.addLoadedRecordField('{{ trace.data_record }}', '{{ trace.data_field }}',
        incomingData, RecordProcessing.get('{{ trace.data_record }}'),
        () => { traces.updateDisplay(true); });
})('{{ loop.index0 }}' * 1);
//  {% endif %}
// {% endfor %}


DataSocket.onRecordReload = function() {
    traces.clearAllData();
    TimeSeriesCommon.clearContamination();

    traces.updateTimeBounds();
    shapeHandler.update();
};
//{% if realtime %}
TimeSelect.onIntervalHeartbeat = function() {
    if (TimeSelect.isZoomed()) {
        return;
    }
    traces.updateTimeBounds();
    shapeHandler.update();
}
//{% endif %}

traces.updateTimeBounds();
shapeHandler.update();

DataSocket.startLoadingRecords();

//{% if not realtime %}
TimeSeriesCommon.installZoomHandler(div);
//{% else %}
TimeSeriesCommon.installZoomHandler(div, true);
//{% endif %}
TimeSeriesCommon.installSpikeToggleHandler(div);
