let layout = {
    //{% if view.title %}
    title: "{{ view.title }}",
    //{% endif %}

    autosize : true,

    xaxis: {
        title: "UTC",
        type: 'date',
        zeroline: false,
        autorange: false,
        range: [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)],
    },

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

        type: '{% if axis.logarithmic %}log{% else %}linear{% endif %}',
        //{% if axis.range == 0 %}
        rangemode: 'tozero',
        //{% elif axis.range %}
        range: ['{{ axis.range[0] }}' * 1, '{{ axis.range[1] }}' * 1],
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

if (localStorage.getItem('forge-settings-time-format') === 'doy') {
    layout.xaxis.hoverformat = "%Y:%-j %H:%M";
    layout.xaxis.tickformat = "%-j\n %Y";
}

const div = document.getElementById('view_timeseries');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

TimeSelect.onHighlight( (start_ms, end_ms) => {
    if (start_ms === undefined && end_ms === undefined) {
        Plotly.relayout(div, {
            'shapes': []
        });
        return;
    }
    if (!start_ms) {
        start_ms = TimeSelect.start_ms;
    }
    if (!end_ms) {
        end_ms = TimeSelect.end_ms;
    }

    Plotly.relayout(div, {
        'shapes': [{
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
        }]
    });
});

function extendData(traceIndex, times, values) {
    Plotly.extendTraces(div, {
        x: [times],
        y: [values]
    }, [traceIndex]);
}
function updateTimeBounds() {
    Plotly.relayout(div, {
        'xaxis.range': [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)],
        'xaxis.autorange': false,
    })
}

DataSocket.resetLoadedRecords();

//{% set trace_loop = namespace(index=0) %}
//{% for graph in view.graphs %}
//  {% for trace in graph.traces %}
//      {% if trace.data_record and trace.data_field %}
(function(traceIndex) {
    DataSocket.addLoadedRecordField('{{ trace.data_record }}', '{{ trace.data_field }}',
    (plotTime, values) => {
        extendData(traceIndex, plotTime, values);
    }, RecordProcessing.get('{{ trace.data_record }}'));
})('{{ trace_loop.index }}' * 1);
//      {% endif %}
//      {% set trace_loop.index = trace_loop.index + 1 %}
//  {% endfor %}
//{% endfor %}

DataSocket.onRecordReload = function() {
    data.forEach((trace) => {
        trace.x.length = 0;
        trace.y.length = 0;
    });

    updateTimeBounds();
};

updateTimeBounds();

DataSocket.startLoadingRecords();

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
