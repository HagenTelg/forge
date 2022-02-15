//{% if realtime %}
TimeSelect.setIntervalBounds();
//{% endif %}

let layout = {
    //{% if view.title %}
    title: "{{ view.title }}",
    //{% endif %}

    autosize : true,
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
        marker: {
            symbol: '{% if loop.index0 <= 52 %}{{ loop.index0 }}{% elif loop.index0 <= 52*2 %}{{loop.index0 + 200 }}{% endif %}',
        },
    },
    // {% endfor %}
];
// {% if not realtime %}
const bins = new SizeBins.AverageBins(data, 'x2', 'y2');
// {% else %}
const bins = new SizeBins.RealtimeAverageBins(data, 'x2', 'y2');
// {% endif %}

let config = {
    responsive: true,
};

const div = document.getElementById('view_sizecounts');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

const shapeHandler = new ShapeHandler(div);

// {% if not realtime %}
const TracesBase = TimeSeriesCommon.Traces;
// {% else %}
const TracesBase = TimeSeriesCommon.RealtimeTraces;
// {% endif %}


const traces = new (class extends TracesBase {
    applyUpdate() {
        bins.recalculateBins();
        super.applyUpdate();
    }
})(div, data, layout, config);
