//{% if realtime %}
TimeSelect.setIntervalBounds();
//{% endif %}

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
        rows: 3,
        ygap: 0.3,
        subplots: [
            ['xy'],
            ['x2y2'],
            ['xy3'],
        ],
    },

    legend: {
        y: 0.33,
        yanchor: 'top',
    },

    xaxis: TimeSeriesCommon.getXAxis(),
    xaxis2: {
        title: "D (μm)",
        hoverformat: ".3f",
        type: 'log',
    },

    yaxis: {
        side: 'left',
        title: "D (μm)",
        hoverformat: ".3f",
        type: 'log',
    },
    yaxis2: {
        side: 'left',
        title: "dN/dlog(Dp) (cm⁻³)",
        hoverformat: ".1f",
        rangemode: 'tozero',
    },
    yaxis3: {
        side: 'left',
        title: "Mm⁻¹",
        hoverformat: ".2f",
    },

    datarevision: 0,
};

let data = [
    {
        x: [ ],
        y: [ ],
        z: [ ],
        type: 'heatmap',
        zsmooth: 'best',
        name: 'dN/dlog(Dp)',
        zmin: 0.0,
        hovertemplate: "%{y:.3f} μm, %{z:.2f} cm⁻³",
        colorscale: 'Electric',
        reversescale: true,
        colorbar: {
            title: "dN/dlog(Dp) (cm⁻³)",
            titleside: 'right',
            len: 0.27,
            y: 1,
            yanchor: 'top',
        },
    },
];
const sizeDistributionIndex = 0;

const measuredScatteringIndex = data.length;
//{% set trace_loop = namespace(index=0) %}
//{% for wl in view.scattering_wavelengths %}
//{% if wl.measured_field %}
data.push({
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y3',
    name: "Measured ({{ wl.wavelength }}nm)",
    hovertemplate: "%{y:.2f}",
    line: {
        width: 1,
        //{% if wl.measured_color %}
        color: '{{ wl.measured_color }}',
        //{% endif %}
    },
    marker: {
        symbol: '{% if trace_loop.index <= 52 %}{{ trace_loop.index }}{% elif trace_loop.index <= 52*2 %}{{ trace_loop.index + 200 }}{% endif %}',
    },
});
//{% endif %}
//{% set trace_loop.index = trace_loop.index + 1 %}
//{% endfor %}

const calculatedScatteringIndex = data.length;
//{% for wl in view.scattering_wavelengths %}
//{% if wl.calculated_field %}
data.push({
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y3',
    name: "Calculated ({{ wl.wavelength }}nm)",
    hovertemplate: "%{y:.2f}",
    line: {
        width: 1,
        //{% if wl.calculated_color %}
        color: '{{ wl.calculated_color }}',
        //{% endif %}
    },
    marker: {
        symbol: '{% if trace_loop.index <= 52 %}{{ trace_loop.index }}{% elif trace_loop.index <= 52*2 %}{{ trace_loop.index + 200 }}{% endif %}',
    },
});
//{% endif %}
//{% set trace_loop.index = trace_loop.index + 1 %}
//{% endfor %}

// {% if not realtime %}
const bins = new SizeBins.AverageBins(data, 'x2', 'y2');
// {% else %}
const bins = new SizeBins.RealtimeAverageBins(data, 'x2', 'y2');
// {% endif %}

let config = {
    responsive: true,
};

const div = document.getElementById('view_sizedistribution');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

const shapeHandler = new ShapeHandler(div);

// {% if not realtime %}
const traces = new TimeSeriesCommon.Traces(div, data, layout, config);
// {% else %}
const traces = new TimeSeriesCommon.RealtimeTraces(div, data, layout, config);
// {% endif %}

