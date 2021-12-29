shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }


TimeSeriesCommon.addSymbolToggleButton(traces);


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
        const extendZ = [];
        distributionZ.push(extendZ);
        while (extendZ.length < distributionX.length) {
            extendZ.push(Number.NaN);
        }
    }
}

function incomingDp(plotTime, values, epoch) {
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

function incomingdNdlogDp(plotTime, values, epoch) {
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
        const addTime = epoch[timeIndex];
        for (let binIndex=0; binIndex<distributionY.length; binIndex++) {
            const v = dNdlogDp[binIndex];
            if (!isFinite(v)) {
                distributionZ[binIndex].push(Number.NaN);
                continue;
            }
            bins.addPoint(binIndex, v, addTime);
            distributionZ[binIndex].push(v);
        }
    }

    //{% if realtime %}
    (function() {
        TimeSelect.setIntervalBounds();
        let discardCutoff = TimeSelect.start_ms;
        if (!TimeSelect.isZoomed()) {
            layout.xaxis.range = [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)];
        } else {
            discardCutoff = Math.min(discardCutoff, TimeSelect.zoom_start_ms);
        }

        let countDiscard = 0;
        for (; countDiscard<distributionX.length; countDiscard++) {
            const pointTime = DataSocket.fromPlotTime(distributionX[countDiscard]);
            if (pointTime >= discardCutoff) {
                break;
            }
        }

        if (countDiscard > 0) {
            distributionX.splice(0, countDiscard);
            for (let binIndex=0; binIndex<distributionY.length; binIndex++) {
                distributionZ[binIndex].splice(0, countDiscard);
            }
        }
    })();
    //{% endif %}

    traces.updateDisplay();
}
DataSocket.addLoadedRecordField('{{ view.size_record }}', 'dNdlogDp',
    incomingdNdlogDp, sizeDistributionProcessing,
    () => { traces.updateDisplay(true); });

//{% for wl in view.scattering_wavelengths %}
(function(traceIndex) {
    DataSocket.addLoadedRecordField('{{ view.measured_record }}', '{{ wl.measured_field }}', (plotTime, values, epoch) => {
        traces.extendData(traceIndex, plotTime, values, epoch);
    }, undefined, () => { traces.updateDisplay(true); });
})('{{ loop.index0 }}' * 1 + measuredScatteringIndex);

(function(traceIndex) {
    DataSocket.addLoadedRecordField('{{ view.size_record }}', '{{ wl.calculated_field }}', (plotTime, values, epoch) => {
        traces.extendData(traceIndex, plotTime, values, epoch);
    }, sizeDistributionProcessing, () => { traces.updateDisplay(true); });
})('{{ loop.index0 }}' * 1 + calculatedScatteringIndex);
//{% endfor %}


DataSocket.onRecordReload = function() {
    traces.clearAllData();
    TimeSeriesCommon.clearContamination();

    traces.updateTimeBounds();
    shapeHandler.update();
};
//{% if realtime %}
TimeSelect.onIntervalHeartbeat = function() {
    if (TimeSelect.isZoomed()) {
        return;
    }
    traces.updateTimeBounds();
    shapeHandler.update();
}
//{% endif %}

traces.updateTimeBounds();
shapeHandler.update();

DataSocket.startLoadingRecords();

//{% if not realtime %}
TimeSeriesCommon.installZoomHandler(div);
//{% else %}
TimeSeriesCommon.installZoomHandler(div, true);
//{% endif %}
TimeSeriesCommon.installSpikeToggleHandler(div);
