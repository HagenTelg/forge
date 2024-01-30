let layout = {
    title: "BSRN QC Flagging",

    autosize : true,
    hovermode: 'x',
    xaxis: TimeSeriesCommon.getXAxis(),

    grid: {
        columns: 1,
        rows: 6,
        pattern: 'coupled',
        ygap: 0.3,
    },

    annotations: [
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y domain',
            y: 1,
            x: 0.5,
            text: "Global SWdn (Rdg)",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y2 domain',
            y: 1,
            x: 0.5,
            text: "Diffuse SW (Rdf)",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y3 domain',
            y: 1,
            x: 0.5,
            text: "Direct Normal (Rdn)",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y4 domain',
            y: 1,
            x: 0.5,
            text: "SWup (Rug)",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y5 domain',
            y: 1,
            x: 0.5,
            text: "LWdn (Rdi)",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y6 domain',
            y: 1,
            x: 0.5,
            text: "LWup (Rui)",
            showarrow: false,
        },
    ],

    yaxis: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        showgrid: false,
    },
    yaxis2: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        showgrid: false,
    },
    yaxis3: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        showgrid: false,
    },
    yaxis4: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        showgrid: false,
    },
    yaxis5: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        gridwidth: 0,
    },
    yaxis6: {
        side: 'left',
        type: 'category',
        zeroline: false,
        ticks: '',
        showgrid: false,
    },
};

let data = [];

let config = {
    responsive: true,
};

const div = document.getElementById('view_timeseries');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

const replotController = new ReplotController(div, data, layout, config);
const shapeHandler = new ShapeHandler(replotController);
