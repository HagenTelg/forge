const latitude = '{{ view.latitude }}' * 1.0;
const longitude = '{{ view.longitude }}' * 1.0;
const solarTime = new Solar.Time(latitude, longitude);

let layout = {
    title: "Solar Position",

    autosize : true,
    hovermode: 'x',
    xaxis: TimeSeriesCommon.getXAxis(),

    modebar: {
        add: ['togglespikelines'],
    },

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

DataSocket.resetLoadedRecords();

DataSocket.onRecordReload = function() {
    layout.datarevision++;

    data.forEach((trace) => {
        trace.x.length = 0;
        trace.y.length = 0;
    });
    layout.xaxis.range = [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)];

    function applyWrap(value) {
        if (value < 180.0) {
            return value + 360.0;
        } else {
            return value - 360.0;
        }
    }

    const elevation = data[0];
    const azimuth = data[1];
    let priorAzimuthAngle = undefined;
    for (let epoch_ms = Math.floor(TimeSelect.start_ms / 60000) * 60000;
            epoch_ms <= TimeSelect.end_ms; epoch_ms += 60000) {
        solarTime.setTime(epoch_ms);
        const time = DataSocket.toPlotTime(epoch_ms);

        elevation.x.push(time);
        elevation.y.push(solarTime.position.elevation);

        const azimuthAngle = solarTime.position.azimuth;
        if (!isFinite(priorAzimuthAngle) || !isFinite(azimuthAngle)) {
            azimuth.x.push(time);
            azimuth.y.push(azimuthAngle);
            priorAzimuthAngle = azimuthAngle;
            continue;
        }

        const currentAzimuthWrapped = applyWrap(azimuthAngle);
        if (Math.abs(priorAzimuthAngle - azimuthAngle) <= Math.abs(priorAzimuthAngle - currentAzimuthWrapped)) {
            azimuth.x.push(time);
            azimuth.y.push(azimuthAngle);
            priorAzimuthAngle = azimuthAngle;
            continue;
        }

        const priorAzimuthWrapped = applyWrap(priorAzimuthAngle);
        const priorTime = DataSocket.toPlotTime(epoch_ms - 60000);

        azimuth.x.push(priorTime);
        azimuth.y.push(currentAzimuthWrapped);

        azimuth.x.push(priorTime);
        azimuth.y.push(undefined);

        azimuth.x.push(priorTime);
        azimuth.y.push(priorAzimuthWrapped);

        azimuth.x.push(time);
        azimuth.y.push(azimuthAngle);
        priorAzimuthAngle = azimuthAngle;
    }

    const shapes = layout.shapes;
    for (let reference=TimeSelect.start_ms - 86400000;
         reference <= TimeSelect.end_ms + 86400000; reference += 86400000) {

        solarTime.setTime(reference);

        const noon = solarTime.day.noon;
        if (isFinite(noon)) {
            shapes.push({
                type: 'line',
                layer: 'below',
                xref: 'x',
                yref: 'paper',
                x0: DataSocket.toPlotTime(noon),
                y0: 0,
                x1: DataSocket.toPlotTime(noon),
                y1: 1,
                opacity: 0.9,
                line: {
                    width: 1,
                    color: '#000000',
                }
            });
        }
    }

    Plotly.react(div, data, layout, config);
};

DataSocket.onRecordReload();

TimeSeriesCommon.installZoomHandler(div);
TimeSeriesCommon.installSpikeToggleHandler(div);
