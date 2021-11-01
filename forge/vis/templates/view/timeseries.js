let layout = {
    //{% if view.title %}
    title: "{{ view.title }}",
    //{% endif %}

    autosize : true,

    modebar: {
        add: ['togglespikelines'],
    },
    hovermode: 'x',

    xaxis: TimeSeriesCommon.getXAxis(),

    grid: {
        columns: 1,
        rows: '{{ view.graphs|length }}' * 1,
        pattern: 'coupled',
        ygap: 0.3,
    },

    annotations: [
        //{% for graph in view.graphs %}{% if graph.title and graph.title|length > 0 %}
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: '{{ view.axis_code(graph.axes[0]) }} domain',
            y: 1,
            x: 0.5,
            text: "{{ graph.title }}",
            showarrow: false
        },
        //{% endif %}{% endfor %}
    ],

    //{% for graph in view.graphs %}
    //  {% for axis in graph.axes %}
    "{{ view.axis_code(axis, base='yaxis') }}": {
        side: '{% if axis.side %}{{ axis.side }}{% else %}{% if loop.index%2 == 1 %}left{% else %}right{% endif %}{% endif %}',
        //{% if loop.index > 1 %}
        overlaying: '{{ view.axis_code(graph.axes[0]) }}',
        zeroline: false,
        showgrid: false,
        //{% endif %}

        //{% if axis.title %}
        title: "{{ axis.title }}",
        //{% endif %}

        //{% if axis.hover_format %}
        hoverformat: "{{ axis.hover_format }}",
        //{% endif %}

        type: '{% if axis.logarithmic %}log{% else %}linear{% endif %}',
        //{% if axis.range == 0 %}
        rangemode: 'tozero',
        //{% elif axis.range %}
        range: ['{{ axis.range[0] }}' * 1, '{{ axis.range[1] }}' * 1],
        //{% endif %}

        //{% if axis.ticks %}
        tickvals: [
            // {% for tick in axis.ticks %}
            '{{ tick }}' * 1,
            // {% endfor %}
        ],
        //{% endif %}
    },
    //  {% endfor %}
    //{% endfor %}
};

let data = [
    //{% for graph in view.graphs %}
    //  {% for trace in graph.traces %}
    {
        x: [ ],
        y: [ ],
        mode: 'lines',
        yaxis: '{{ view.axis_code(trace.axis) }}',
        name: "{{ trace.legend }}",
        hovertemplate: "{{ trace.hover_template() }}",
        line: {
            width: 1,
            //{% if trace.color %}
            color: '{{ trace.color }}',
            //{% endif %}
        },
    },
    //  {% endfor %}
    //{% endfor %}
];

let config = {
    responsive: true,
};

const div = document.getElementById('view_timeseries');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

const shapeHandler = new ShapeHandler(div);
const traces = new TimeSeriesCommon.Traces(div, data, layout, config);
shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }
TimeSeriesCommon.addContaminationToggleButton(traces);


DataSocket.resetLoadedRecords();

//{% set trace_loop = namespace(index=0) %}
//{% for graph in view.graphs %}

//{% if graph.contamination %}
shapeHandler.generators.push(TimeSeriesCommon.installContamination('{{ loop.index0 }}' * 1,
    '{{ graph.contamination }}', 'y{% if loop.index > 1 %}{{ loop.index }}{% endif %}'));
//{% endif %}

//  {% for trace in graph.traces %}
//      {% if trace.data_record and trace.data_field %}
(function(traceIndex) {
    let incomingData = (plotTime, values, epoch) => {
        traces.extendData(traceIndex, plotTime, values, epoch);
    };

    // {% if trace.script_incoming_data %}{{ '\n' }}{{ trace.script_incoming_data | safe }}{% endif %}

    //{% if graph.contamination %}
    traces.setTraceContamination(traceIndex, '{{ graph.contamination }}');
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

traces.updateTimeBounds();
shapeHandler.update();

DataSocket.startLoadingRecords();

TimeSeriesCommon.installZoomHandler(div);
TimeSeriesCommon.installSpikeToggleHandler(div);
