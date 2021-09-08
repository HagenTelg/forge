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
});
//{% endif %}
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
});
//{% endif %}
//{% endfor %}

const bins = new SizeBins.AverageBins(data, 'x2', 'y2');

let config = {
    responsive: true,
};

const div = document.getElementById('view_sizedistribution');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    div.classList.add('scroll');
}

Plotly.newPlot(div, data, layout, config);

const shapeHandler = new ShapeHandler(div);
const traces = new TimeSeriesCommon.Traces(div, data, layout, config);
shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }


DataSocket.resetLoadedRecords();

//{% if view.contamination %}
shapeHandler.generators.push(TimeSeriesCommon.installContamination(3,
    '{{ view.contamination }}', 'y3'));
//{% endif %}

function sizeDistributionProcessing(dataName) {
    const outputWavelengths = new Map();
//{% for wl in view.scattering_wavelengths %}
    outputWavelengths.set('{{ wl.wavelength }}' * 1.0, {
        Bs: '{{ wl.calculated_field }}',
    });
//{% endfor %}
    return new Mie.OpticalDispatch(dataName, outputWavelengths);
}

function prepareDistributionBins(binCount) {
    const distributionData = data[sizeDistributionIndex];
    const distributionX = distributionData.x;
    const distributionY = distributionData.y;
    const distributionZ = distributionData.z;

    while (distributionY.length < binCount) {
        distributionY.push(Number.NaN);
        distributionZ.push([]);
        const extendZ = distributionZ[distributionZ.length-1];
        while (extendZ.length < distributionX.length) {
            extendZ.push(Number.NaN);
        }
    }
}

function incomingDp(plotTime, values) {
    if (values.length === 0) {
        return;
    }
    const Dp = values[values.length - 1];
    if (!Array.isArray(Dp)) {
        return;
    }

    const distributionData = data[sizeDistributionIndex];
    let changed = false;
    prepareDistributionBins(Dp.length);
    for (let i=0; i<Dp.length; i++) {
        const d = Dp[i];
        if (!isFinite(d)) {
            return;
        }
        if (distributionData.y[i] === d) {
            continue;
        }
        changed = true;

        distributionData.y[i] = d;
        bins.setDiameter(i, d);
    }

    if (!changed) {
        return;
    }
    traces.updateDisplay();
}
DataSocket.addLoadedRecordField('{{ view.size_record }}', 'Dp',
    incomingDp, sizeDistributionProcessing,
    () => { traces.updateDisplay(true); });

function incomingdNdlogDp(plotTime, values) {
    if (plotTime.length === 0) {
        return;
    }

    const distributionData = data[sizeDistributionIndex];
    const distributionX = distributionData.x;
    const distributionY = distributionData.y;
    const distributionZ = distributionData.z;
    for (let timeIndex=0; timeIndex<plotTime.length; timeIndex++) {
        const dNdlogDp = values[timeIndex];
        if (!Array.isArray(dNdlogDp)) {
            continue;
        }
        prepareDistributionBins(dNdlogDp.length);

        distributionX.push(plotTime[timeIndex]);
        for (let binIndex=0; binIndex<distributionY.length; binIndex++) {
            const v = dNdlogDp[binIndex];
            if (!isFinite(v)) {
                distributionZ[binIndex].push(Number.NaN);
                continue;
            }
            bins.addPoint(binIndex, v);
            distributionZ[binIndex].push(v);
        }
    }

    traces.updateDisplay();
}
DataSocket.addLoadedRecordField('{{ view.size_record }}', 'dNdlogDp',
    incomingdNdlogDp, sizeDistributionProcessing,
    () => { traces.updateDisplay(true); });

//{% for wl in view.scattering_wavelengths %}
(function(traceIndex) {
    DataSocket.addLoadedRecordField('{{ view.measured_record }}', '{{ wl.measured_field }}', (plotTime, values) => {
        traces.extendData(traceIndex, plotTime, values);
    }, undefined, () => { traces.updateDisplay(true); });
})('{{ loop.index0 }}' * 1 + measuredScatteringIndex);

(function(traceIndex) {
    DataSocket.addLoadedRecordField('{{ view.size_record }}', '{{ wl.calculated_field }}', (plotTime, values) => {
        traces.extendData(traceIndex, plotTime, values);
    }, sizeDistributionProcessing, () => { traces.updateDisplay(true); });
})('{{ loop.index0 }}' * 1 + calculatedScatteringIndex);
//{% endfor %}


DataSocket.onRecordReload = function() {
    data.forEach((trace) => {
        if (trace.x) {
            trace.x.length = 0;
        }
        if (trace.y) {
            trace.y.length = 0;
        }
        if (trace.z) {
            trace.z.length = 0;
        }
    });
    TimeSeriesCommon.clearContamination();

    traces.updateTimeBounds();
    shapeHandler.update();
};

traces.updateTimeBounds();
shapeHandler.update();

DataSocket.startLoadingRecords();

TimeSeriesCommon.installZoomHandler(div);
TimeSeriesCommon.installSpikeToggleHandler(div);
