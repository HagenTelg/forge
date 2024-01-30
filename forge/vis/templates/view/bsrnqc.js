const latitude = '{{ latitude }}' * 1.0;
const longitude = '{{ longitude }}' * 1.0;
const solarTime = new Solar.Time(latitude, longitude);

shapeHandler.generators.push(TimeSeriesCommon.getTimeHighlights);
TimeSeriesCommon.updateShapes = function() { shapeHandler.update(); }

shapeHandler.generators.push(() => {
    const shapes = [];

    let priorSunset = undefined;
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

        const sunset = solarTime.day.sunset;
        const sunrise = solarTime.day.sunrise;
        if (isFinite(priorSunset) && isFinite(sunrise)) {
            for (let i=1; i<=6; i++) {
                let y = 'y';
                if (i > 1) {
                    y = y + i.toString();
                }
                shapes.push({
                    type: 'rect',
                    xref: 'x',
                    yref: y,
                    x0: DataSocket.toPlotTime(priorSunset),
                    x1: DataSocket.toPlotTime(sunrise),
                    y0: 0,
                    y1: 1,
                    opacity: 0.1,
                    fillcolor: '#400080',
                    line: {
                        width: 0,
                    },
                });
            }
        }

        priorSunset = sunset;
    }

    return shapes;
});

DataSocket.resetLoadedRecords();


let data_physically_possible_Rdg = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y',
    name: "Physically Possible Limit (Rdg)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rdg);
let data_extremely_rare_Rdg = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y',
    name: "Extremely Rare Limit (Rdg)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rdg);
let data_global_sum_sw_ratio = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y',
    name: "Ratio of Global Over Sum SW",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_global_sum_sw_ratio);

let data_physically_possible_Rdf = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y2',
    name: "Physically Possible Limit (Rdf)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rdf);
let data_extremely_rare_Rdf = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y2',
    name: "Extremely Rare Limit (Rdf)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rdf);
let data_diffuse_ratio = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y2',
    name: "Diffuse Ratio",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_diffuse_ratio);

let data_physically_possible_Rdn = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y3',
    name: "Physically Possible Limit (Rdn)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rdn);
let data_extremely_rare_Rdn = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y3',
    name: "Extremely Rare Limit (Rdn)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rdn);

let data_physically_possible_Rug = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y4',
    name: "Physically Possible Limit (Rug)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rug);
let data_extremely_rare_Rug = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y4',
    name: "Extremely Rare Limit (Rug)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rug);
let data_swup_comparison = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y4',
    name: "SWup Comparison",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_swup_comparison);

let data_physically_possible_Rdi = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y5',
    name: "Physically Possible Limit (Rdi)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rdi);
let data_extremely_rare_Rdi = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y5',
    name: "Extremely Rare Limit (Rdi)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rdi);
let data_air_temperature_Rdi = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y5',
    name: "LWdn to Air Temperature (Rdi)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_air_temperature_Rdi);
let data_Rdi_to_Rui = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y5',
    name: "LWdn to LWup Comparison (Rdi/Rui)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_Rdi_to_Rui);

let data_physically_possible_Rui = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y6',
    name: "Physically Possible Limit (Rui)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_physically_possible_Rui);
let data_extremely_rare_Rui = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y6',
    name: "Extremely Rare Limit (Rui)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_extremely_rare_Rui);
let data_air_temperature_Rui = {
    x: [ ],
    y: [ ],
    mode: 'markers',
    yaxis: 'y6',
    name: "LWup to Air Temperature (Rui)",
    hovertext: [],
    marker: {
        symbol: [],
        color: [],
        size: 10,
    }
};
data.push(data_air_temperature_Rui);


class Calculator extends DataSocket.RecordStream {
    constructor(dataName) {
        super(dataName);
    }

    attach() {}

    _deriveValues(record, times) {
        let SZA = record.get("zsa");
        if (!SZA) SZA = [];
        const u0 = [];
        for (let i=0; i<times.length; i++) {
            let v = SZA[i];
            if (!isFinite(v)) {
                u0.push(undefined);
                continue;
            }
            if (v > 90) {
                u0.push(0);
            } else {
                u0.push(Math.cos(SZA * Math.PI/180.0));
            }
        }
        record.set("u0", u0);

        let S0 = record.get("S0");
        if (!S0) S0 = [];
        let AU = record.get("AU");
        if (!AU) AU = [];
        const Sa = [];
        for (let i=0; i<times.length; i++) {
            let v_s0 = S0[i];
            let v_au = AU[i];
            if (!isFinite(v_s0) || !isFinite(v_au) || v_au <= 0.0) {
                Sa.push(undefined);
                continue;
            }
            Sa.push(v_s0 / Math.pow(v_au, 2));
        }
        record.set("Sa", Sa);
    }

    processRecord(record, epoch, plotTime) {
        this._deriveValues(record, epoch);

        function generate(destination, category, calculate, ...inputs) {
            const inputValues = [];
            for (const fieldName of inputs) {
                let values = record.get(fieldName);
                if (!values) {
                    values = [];
                }
                inputValues.push(values);
            }

            const args = [];
            for (let timeIndex=0; timeIndex<plotTime.length; timeIndex++) {
                destination.x.push(plotTime[timeIndex]);

                args.length = 0;
                for (const fieldValues of inputValues) {
                    let value = fieldValues[timeIndex];
                    if (!isFinite(value)) {
                        break;
                    }
                    args.push(value);
                }
                if (args.length !== inputValues.length) {
                    destination.y.push(undefined);
                    destination.marker.symbol.push(undefined);
                    destination.marker.color.push(undefined)
                    continue;
                }

                const result = calculate(...args);
                if (result > 0) {
                    destination.y.push(category);
                    destination.marker.symbol.push("triangle-up")
                    destination.marker.color.push("rgb(255, 0, 0)")
                    destination.hovertext.push("Over Max")
                } else if (result < 0) {
                    destination.y.push(category);
                    destination.marker.symbol.push("triangle-down")
                    destination.marker.color.push("rgb(0, 255, 255)")
                    destination.hovertext.push("Under Min")
                } else {
                    destination.y.push(undefined);
                    destination.marker.symbol.push(undefined);
                    destination.marker.color.push(undefined);
                    destination.hovertext.push(undefined);
                }
            }
        }

        generate(data_physically_possible_Rdg, "Physical", (Rdg, u0, Sa) => {
            if (Rdg < -4) {
                return -1;
            } else if (Rdg > Sa * 1.5 * Math.pow(u0, 1.2) + 100) {
                return 1;
            }
        }, "Rdg", "u0", "Sa");
        generate(data_extremely_rare_Rdg, "Extreme", (Rdg, u0, Sa) => {
            if (Rdg < -2) {
                return -1;
            } else if (Rdg > Sa * 1.2 * Math.pow(u0, 1.2) + 50) {
                return 1;
            }
        }, "Rdg", "u0", "Sa");
        generate(data_global_sum_sw_ratio, "Ratio", (Rdg, Rdf, Rdn, SZA, u0) => {
            if (SZA < 93) {
                return undefined;
            }
            const sum_sw = Rdf + Rdn * u0;
            if (sum_sw < 50) {
                return undefined;
            }
            const ratio = Rdg / sum_sw;
            if (SZA < 75) {
                if (ratio < 0.92) {
                    return -1;
                } else if (ratio > 1.08) {
                    return 1;
                }
            } else {
                if (ratio < 0.85) {
                    return -1;
                } else if (ratio > 1.15) {
                    return 1;
                }
            }
        }, "Rdg", "Rdf", "Rdn", "zsa", "u0");

        generate(data_physically_possible_Rdf, "Physical", (Rdf, u0, Sa) => {
            if (Rdf < -4) {
                return -1;
            } else if (Rdf > Sa * 0.95 * Math.pow(u0, 1.2) + 50) {
                return 1;
            }
        }, "Rdf", "u0", "Sa");
        generate(data_extremely_rare_Rdf, "Extreme", (Rdf, u0, Sa) => {
            if (Rdf < -2) {
                return -1;
            } else if (Rdf > Sa * 0.75 * Math.pow(u0, 1.2) + 30) {
                return 1;
            }
        }, "Rdf", "u0", "Sa");
        generate(data_diffuse_ratio, "Ratio", (Rdf, Rdg, SZA) => {
            if (SZA < 93) {
                return undefined;
            }
            if (Rdg < 50) {
                return undefined;
            }
            const ratio = Rdf / Rdg;
            if (SZA < 75) {
                if (ratio >= 1.05) {
                    return 1;
                }
            } else {
                if (ratio >= 1.10) {
                    return 1;
                }
            }
        }, "Rdf", "Rdg", "zsa");

        generate(data_physically_possible_Rdn, "Physical", (Rdn, Sa) => {
            if (Rdn < -4) {
                return -1;
            } else if (Rdn > Sa) {
                return 1;
            }
        }, "Rdf", "Sa");
        generate(data_extremely_rare_Rdn, "Extreme", (Rdn, u0, Sa) => {
            if (Rdn < -2) {
                return -1;
            } else if (Rdn > Sa * 0.95 * Math.pow(u0, 0.2) + 10) {
                return 1;
            }
        }, "Rdn", "u0", "Sa");

        generate(data_physically_possible_Rug, "Physical", (Rug, u0, Sa) => {
            if (Rug < -4) {
                return -1;
            } else if (Rug > Sa * 1.2 * Math.pow(u0, 1.2) + 50) {
                return 1;
            }
        }, "Rug", "u0", "Sa");
        generate(data_extremely_rare_Rug, "Extreme", (Rug, u0, Sa) => {
            if (Rdf < -2) {
                return -1;
            } else if (Rdf > Sa * Math.pow(u0, 1.2) + 50) {
                return 1;
            }
        }, "Rug", "u0", "Sa");
        generate(data_swup_comparison, "Comparison", (Rug, Rdf, Rdn, SZA, u0) => {
            const sum_sw = Rdf + Rdn * u0;
            if (sum_sw < 50) {
                return undefined;
            }
            if (Rug > sum_sw) {
                return 1;
            }
        }, "Rug", "Rdf", "Rdn", "zsa", "u0");

        generate(data_physically_possible_Rdi, "Physical", (Rdi) => {
            if (Rdi < 40) {
                return -1;
            } else if (Rdi > 700) {
                return 1;
            }
        }, "Rdi");
        generate(data_extremely_rare_Rdi, "Extreme", (Rdi) => {
            if (Rdi < 60) {
                return -1;
            } else if (Rdi > 500) {
                return 1;
            }
        }, "Rdi");
        generate(data_air_temperature_Rdi, "Temperature", (Rdi, Tambient) => {
            const stephan_boltzman_constant = 4.67E-8;
            const Ta = Tambient + 273.15;
            if (Ta < 170 || Ta > 350) {
                return undefined;
            }
            const base = stephan_boltzman_constant * Math.pow(Tambient, 4);
            if (Rdi < 0.4 * base) {
                return -1;
            } else if (Rdi > base + 25) {
                return 1;
            }
        }, "Rdi", "Tambient");
        generate(data_Rdi_to_Rui, "Comparison", (Rdi, Rui) => {
            if (Rdi > Rui + 25) {
                return 1;
            } else if (Rdi < Rui - 300) {
                return -1;
            }
        }, "Rdi", "Rui");
        
        generate(data_physically_possible_Rui, "Physical", (Rui) => {
            if (Rui < 40) {
                return -1;
            } else if (Rui > 900) {
                return 1;
            }
        }, "Rui");
        generate(data_extremely_rare_Rui, "Extreme", (Rui) => {
            if (Rui < 60) {
                return -1;
            } else if (Rui > 700) {
                return 1;
            }
        }, "Rui");
        generate(data_air_temperature_Rui, "Temperature", (Rui, Tambient) => {
            const stephan_boltzman_constant = 4.67E-8;
            const Ta = Tambient + 273.15;
            if (Ta < 170 || Ta > 350) {
                return undefined;
            }
            if (Rui < stephan_boltzman_constant * Math.pow(Tambient - 15, 4)) {
                return -1;
            } else if (Rui > stephan_boltzman_constant * Math.pow(Tambient + 25, 4)) {
                return 1;
            }
        }, "Rui", "Tambient");

        replotController.replot();
    }

    incomingData(fieldName, plotTime, values, epoch) {}

    endOfData() {
        super.endOfData();
        replotController.replot(true);
    }
}
DataSocket.addLoadedRecord("{{ record }}", (dataName) => { return new Calculator(dataName); });

//{% if contamination %}
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y');
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y2');
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y3');
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y4');
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y5');
TimeSeriesCommon.installContamination(shapeHandler, '{{ contamination }}', 'y6');
//{% endif %}


DataSocket.onRecordReload = function() {
    data.forEach((data, traceIndex) => {
        if (data.x) {
            data.x.length = 0;
        }
        if (data.y) {
            data.y.length = 0;
        }
        if (data.marker.symbol) {
            data.marker.symbol.length = 0;
        }
        if (data.marker.color) {
            data.marker.color.length = 0;
        }
    })

    TimeSeriesCommon.clearContamination();

    Plotly.relayout(div, {
        'xaxis.range': [DataSocket.toPlotTime(TimeSelect.start_ms), DataSocket.toPlotTime(TimeSelect.end_ms)],
        'xaxis.autorange': false,
    });

    shapeHandler.update();
};

DataSocket.startLoadingRecords();

TimeSeriesCommon.installZoomHandler(div);

shapeHandler.update();