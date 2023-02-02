const latitude = '{{ latitude }}' * 1.0;
const longitude = '{{ longitude }}' * 1.0;
const solarTime = new Solar.Time(latitude, longitude);

let layout = {
    title: "Solar Position",

    autosize : true,
    hovermode: 'x',
    xaxis: TimeSeriesCommon.getXAxis(),

    grid: {
        columns: 1,
        rows: 2,
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
            text: "Elevation Angle",
            showarrow: false,
        },
        {
            xanchor: 'center',
            yanchor: 'bottom',
            xref: 'paper',
            yref: 'y2 domain',
            y: 1,
            x: 0.5,
            text: "Azimuth Angle",
            showarrow: false,
        },
    ],

    shapes: [],

    yaxis: {
        side: 'left',
        title: "degrees",
    },
    yaxis2: {
        side: 'left',
        title: "degrees",
        range: [0, 360],
        zeroline: false,
        tickvals: [0, 90, 180, 270, 360],
    },

    datarevision: 0,
};

let data = [
    {
        x: [ ],
        y: [ ],
        mode: 'lines',
        yaxis: 'y',
        name: "Elevation",
        hovertemplate: "%{y:.1f}",
        line: {
            width: 1,
        },
    },
    {
        x: [ ],
        y: [ ],
        mode: 'lines',
        yaxis: 'y2',
        name: "Azimuth",
        hovertemplate: "%{y:.0f}",
        line: {
            width: 1,
        },
    },
];

let config = {
    responsive: true,
};

const div = document.getElementById('view_timeseries');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);
