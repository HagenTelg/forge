let layout = {
    //{% if view.title %}
    title: { text: "{{ view.title }}" },
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

    xaxis: {
        range: [0, 14],
        zeroline: false,
        tickvals: [1, 3, 5, 7, 9, 11, 13],
        ticktext: ["JAN", "MAR", "MAY", "JUL", "SEP", "NOV", "ANN"],
        fixedrange: true,
    },
    xaxis2: {
        title: { text: "UTC" },
        type: 'date',
        tickformat: '%b\n%Y',
        zeroline: false,
        hoverformat: "%B %Y",
    },

    yaxis: {
        domain: [0.4, 1],
        side: 'left',
        //{% if view.units %}
        title: { text: "{{ view.units }}" },
        //{% endif %}
        type: '{% if view.logarithmic %}log{% else %}linear{% endif %}',
        //{% if view.range == 0 %}
        rangemode: 'tozero',
        //{% elif view.range %}
        range: ['{{ view.range[0] }}' * 1, '{{ view.range[1] }}' * 1],
        //{% endif %}
    },
    yaxis2: {
        domain: [0, 0.3],
        side: 'left',
        //{% if view.units %}
        title: { text: "{{ view.units }}" },
        //{% endif %}
        type: '{% if view.logarithmic %}log{% else %}linear{% endif %}',
        //{% if view.range == 0 %}
        rangemode: 'tozero',
        //{% elif view.range %}
        range: ['{{ view.range[0] }}' * 1, '{{ view.range[1] }}' * 1],
        //{% endif %}
    },

    datarevision: 0,
    uirevision: 0,
};


let data = [];

let data_shade = {
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y2',
    xaxis: 'x2',
    showlegend: false,
    hoverinfo: 'skip',
    fill: 'toself',
    fillcolor: 'rgba(128, 64, 0, 0.1)',
    line: {color: 'transparent'},
};
data.push(data_shade);
let data_mid = {
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y2',
    xaxis: 'x2',
    showlegend: false,
    hoverinfo: 'skip',
    line: {
        color: 'rgba(128, 64, 0, 0.5)',
        width: 1,
    },
};
data.push(data_mid);
let data_upper = {
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y2',
    xaxis: 'x2',
    showlegend: false,
    hoverinfo: 'skip',
    line: {
        color: 'rgba(128, 64, 0, 0.2)',
        width: 1,
        dash: 'dash',
    },
};
data.push(data_upper);
let data_lower = {
    x: [ ],
    y: [ ],
    mode: 'lines',
    yaxis: 'y2',
    xaxis: 'x2',
    showlegend: false,
    hoverinfo: 'skip',
    line: {
        color: 'rgba(128, 64, 0, 0.2)',
        width: 1,
        dash: 'dash',
    },
};
data.push(data_lower);

let box_hidden_traces = [];
function boxHidden() {
    let trace = {
        x: [ ],
        y: [ ],
        mode: 'none',
        yaxis: 'y',
        xaxis: 'x',
        showlegend: false,
        hovertemplate: '%{y:.1f}<extra></extra>',
        line: { color: 'transparent' },
        hoverlabel: { bgcolor: [] },
    };
    data.push(trace);
    box_hidden_traces.push(trace);
    return trace;
}
let box_hover_q05 = boxHidden();
box_hover_q05.hovertemplate = '%{y:.1f}<extra>5%</extra>';
let box_hover_q25 = boxHidden();
box_hover_q25.hovertemplate = '%{y:.1f}<extra>25%</extra>';
let box_hover_q50 = boxHidden();
box_hover_q50.hovertemplate = '%{y:.1f}<extra>%{text}</extra>';
box_hover_q50.text = [];
let box_hover_q75 = boxHidden();
box_hover_q75.hovertemplate = '%{y:.1f}<extra>75%</extra>';
let box_hover_q95 = boxHidden();
box_hover_q95.hovertemplate = '%{y:.1f}<extra>95%</extra>';

function boxTrace(color) {
    let trace = {
        x: [],
        median: [],
        q1: [],
        q3: [],
        lowerfence: [],
        upperfence: [],
        width: 0.25,
        type: 'box',
        showlegend: false,
        hoverinfo : 'none',
        xaxis: 'x',
        yaxis: 'y',
        boxpoints: false,
        orientation: 'v',
        line: {
            color: color,
            width: 1,
        },
        fillcolor: 'rgba(0, 0, 0, 0)',
    };
    data.push(trace);
    return trace;
}
let data_box_1 = boxTrace("#000");
let data_box_2 = boxTrace("#07f");

function meanData(width, color) {
    let trace = {
        x: [ ],
        y: [ ],
        mode: 'lines',
        yaxis: 'y2',
        xaxis: 'x2',
        showlegend: false,
        line: {
            width: width,
            color: color,
        },
    };
    data.push(trace);
    return trace;
}
let data_mean_all = meanData(1, "#333");
data_mean_all.line.dash = 'dot';
data_mean_all.hovertemplate = "%{y:.1f}<extra></extra>"
let data_mean_1 = meanData(3, "#000");
data_mean_1.hoverinfo = 'skip';
let data_mean_2 = meanData(1, "#07f");
data_mean_2.hoverinfo = 'skip';

const data_bins_begin = data.length;

let config = {
    responsive: true,
};

const div = document.getElementById('view_statistics');
const plot = Plotly.newPlot(div, data, layout, config);

const replotController = new ReplotController(div, data, layout, config);
const shapeHandler = new ShapeHandler(replotController);
