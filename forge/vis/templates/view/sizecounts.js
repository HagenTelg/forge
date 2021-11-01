let layout = {
    //{% if view.title %}
    title: "{{ view.title }}",
    //{% endif %}

    autosize : true,

    modebar: {
        add: 'togglespikelines',
    },
    hovermode: 'x',

    grid: {
        columns: 1,
        rows: 2,
        subplots: [
            ['xy'],
            ['x2y2'],
        ],
    },

    xaxis: TimeSeriesCommon.getXAxis(),
    xaxis2: {
        title: "D (μm)",
        hoverformat: ".3f",
        type: 'log',
    },

    yaxis: {
        side: 'left',
        title: "cm⁻³",
        hoverformat: ".1f",
        rangemode: 'tozero',
        domain: [0.3, 1],
    },
    yaxis2: {
        side: 'left',
        title: "dN (cm⁻³)",
        hoverformat: ".2f",
        rangemode: 'tozero',
        domain: [0, 0.2],
    },

    datarevision: 0,
};

let data = [
    // {% for trace in view.traces %}
    {
        x: [ ],
        y: [ ],
        mode: 'lines',
        yaxis: 'y',
        xaxis: 'x',
        name: "{{ trace.legend }}",
        hovertemplate: "%{y:.1f}",
        line: {
            width: 1,
            //{% if trace.color %}
            color: '{{ trace.color }}',
            //{% endif %}
        },
    },
    // {% endfor %}
];
const bins = new SizeBins.AverageBins(data, 'x2', 'y2');

let config = {
    responsive: true,
};

const div = document.getElementById('view_sizecounts');
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

//{% if view.contamination %}
shapeHandler.generators.push(TimeSeriesCommon.installContamination(1,
    '{{ view.contamination }}', 'y'));
//{% endif %}


function incomingDp(plotTime, values) {
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

function incomingdN(plotTime, values) {
    if (plotTime.length === 0) {
        return;
    }

    for (let timeIndex=0; timeIndex<plotTime.length; timeIndex++) {
        const dN = values[timeIndex];
        if (!Array.isArray(dN)) {
            continue;
        }
        for (let binIndex=0; binIndex<dN.length; binIndex++) {
            bins.addPoint(binIndex, dN[binIndex]);
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

    //{% if graph.contamination %}
    traces.setTraceContamination(traceIndex, '{{ graph.contamination }}');
    //{% endif %}

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

traces.updateTimeBounds();
shapeHandler.update();

DataSocket.startLoadingRecords();

TimeSeriesCommon.installZoomHandler(div);
TimeSeriesCommon.installSpikeToggleHandler(div);
