shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }


DataSocket.resetLoadedRecords();

//{% set trace_loop = namespace(index=0) %}
//{% for graph in view.graphs %}

//{% if graph.contamination and not realtime %}
shapeHandler.generators.push(TimeSeriesCommon.installContamination('{{ loop.index0 }}' * 1,
    '{{ graph.contamination }}', 'y{% if loop.index > 1 %}{{ loop.index }}{% endif %}'));
//{% endif %}

//  {% for trace in graph.traces %}
//      {% if trace.data_record and trace.data_field %}
(function(traceIndex) {
    let incomingData = (plotTime, values, epoch) => {
        traces.extendData(traceIndex, plotTime, values, epoch);
    };

    //{% if trace.script_incoming_data %}{{ '\n' }}{{ trace.script_incoming_data | safe }}{% endif %}

    //{% if graph.contamination and not realtime %}
    traces.applyDataFilter(traceIndex, '{{ graph.contamination }}');
    //{% else %}
    traces.applyDataFilter(traceIndex);
    //{% endif %}

    DataSocket.addLoadedRecordField('{{ trace.data_record }}', '{{ trace.data_field }}',
        incomingData, RecordProcessing.get('{{ trace.data_record }}'),
        () => { traces.updateDisplay(true); });
})('{{ trace_loop.index }}' * 1);
//      {% endif %}
//      {% set trace_loop.index = trace_loop.index + 1 %}
//  {% endfor %}
//{% endfor %}

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

TimeSeriesCommon.addSymbolToggleButton(traces);
TimeSeriesCommon.addHoverControlButton(traces);
//{% if not realtime %}
TimeSeriesCommon.addContaminationToggleButton(traces);
//{% endif %}
TimeSeriesCommon.addAveragingButton(traces);

div.on('plotly_clickannotation', function(data) {
    const indexMap = [
    //{% for graph in view.graphs %}{% if graph.display_title %}
        '{{ loop.index }}' * 1,
    //{% endif %}{% endfor %}
    ];
    const graphIndex = indexMap[data.index];
    if (!graphIndex) {
        return;
    }
    const url = new URL('{{ request.url_for("view_standalone", station=station, view_name=view_name) }}');
    url.searchParams.append("graph", graphIndex.toString())
    window.open(url.href, "", "noopener");
});
