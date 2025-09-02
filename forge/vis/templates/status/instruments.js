$('#instrumentsummary-ok').click(function(event) {
    event.preventDefault();
    hideModal();
});

function formatInstrumentDisplay(content) {
    let displayInfo = "";
    if (content.manufacturer) {
        if (displayInfo) displayInfo += " ";
        displayInfo += content.manufacturer;
    }
    if (content.model) {
        if (displayInfo) displayInfo += " ";
        displayInfo += content.model;
    }
    if (content.serial_number) {
        if (displayInfo) displayInfo += " ";
        displayInfo += "#" + content.serial_number;
    }
    if (content.instrument_code) {
        if (displayInfo) {
            displayInfo += " (" + content.instrument_code + ")";
        } else {
            displayInfo += content.instrument_code;
        }
    }
    return displayInfo;
}

const instrumentSegments = new Map();
function showPlot() {
    const div = document.getElementById('instrumentsummary_plot');
    div.style.display = '';

    let displayStart = undefined;
    let displayEnd = undefined;
    instrumentSegments.forEach((segments) => {
        if (displayStart === undefined || segments[0].start_epoch_ms < displayStart) {
            displayStart = segments[0].start_epoch_ms;
        }
        if (displayEnd === undefined || segments[segments.length-1].end_epoch_ms > displayEnd) {
            displayEnd = segments[segments.length-1].end_epoch_ms;
        }
    });

    const data = [];
    const layout = {
        autosize : true,
        showlegend: false,
        barmode: 'stack',
        hoverdistance: -1,

        height: 25 * instrumentSegments.size + 100,
        margin: {
            t: 20,
            b: 60,
            l: 80,
            r: 10,
        },

        xaxis: (function() {
            const result = {
                title: { text: "UTC" },
                type: 'date',
                hoverformat: '%Y-%m-%d %H:%M:%S',
                tickformat: '%H:%M\n %Y-%m-%d',
                zeroline: false,
                autorange: false,
                range: [ DataSocket.toPlotTime(displayStart), DataSocket.toPlotTime(displayEnd) ],
            };

            if (localStorage.getItem('forge-settings-time-format') === 'doy') {
                result.hoverformat = "%Y:%-j.%k";
                result.tickformat = "%-j.%k\n %Y";
            }

            return result;
        })(),

        yaxis: {
            zeroline: false,
            range: [1, 0],
            tickvals: [],
            ticktext: [],
            ticklabelstandoff: 5,
        },

        shapes: [],
    };
    const config = {
        responsive: true,
    };

    const instrumentHoverTrace = {
        type: 'scatter',
        x: [],
        y: [],
        opacity: 0,
        mode: 'none',
        hoverinfo: 'text',
        hovertext: [],
    };
    data.push(instrumentHoverTrace);

    Array.from(instrumentSegments.keys()).sort().forEach((instrument_id) => {
        const segments = instrumentSegments.get(instrument_id);
        let y_center = 0.5 + layout.yaxis.tickvals.length;
        layout.yaxis.tickvals.push(y_center);
        layout.yaxis.ticktext.push(instrument_id);

        for (const segment of segments) {
            layout.shapes.push({
                type: 'rect',
                xref: 'x',
                yref: 'y',
                x0: DataSocket.toPlotTime(segment.start_epoch_ms),
                y0: y_center - 0.4,
                x1: DataSocket.toPlotTime(segment.end_epoch_ms),
                y1: y_center + 0.4,
                fillcolor: '#777',
                line: {
                    color: '#000',
                    width: 1,
                },
            });

            instrumentHoverTrace.x.push(DataSocket.toPlotTime((segment.end_epoch_ms + segment.start_epoch_ms) / 2));
            instrumentHoverTrace.y.push(y_center);
            let hoverText = TimeParse.toDisplayTime(segment.start_epoch_ms) + " - " + TimeParse.toDisplayTime(segment.end_epoch_ms);
            hoverText += "<br>" + formatInstrumentDisplay(segment);
            instrumentHoverTrace.hovertext.push(hoverText);
        }
    });
    layout.yaxis.range = [layout.yaxis.tickvals[layout.yaxis.tickvals.length-1] + 0.5, 0.0];

    Plotly.newPlot(div, data, layout, config);
}

let incomingInstrumentsStream = undefined;
const InstrumentsStream = class extends DataSocket.Stream {
    constructor() {
        super('{{ mode_name }}-instruments');
        instrumentSegments.clear();
    }

    endOfData() {
        incomingInstrumentsStream = undefined;

        document.getElementById('instrumentsummary_loading').style.display = 'none';

        const details = document.getElementById('instrumentsummary_details');
        while (details.firstChild) {
            details.removeChild(details.firstChild);
        }

        if (instrumentSegments.size === 0) {
            details.textContent = 'No instruments recorded.';
            return;
        }
        showPlot();

        function addSummaryLine(text) {
            const span = document.createElement('span');
            span.textContent = text;
            details.appendChild(span);
        }

        let latestInstrumentSeen = undefined;
        instrumentSegments.forEach((segments) => {
            if (latestInstrumentSeen === undefined || segments[segments.length-1].end_epoch_ms > latestInstrumentSeen) {
                latestInstrumentSeen = segments[segments.length-1].end_epoch_ms;
            }
        });

        Array.from(instrumentSegments.keys()).sort().forEach((instrument_id) => {
            const segments = instrumentSegments.get(instrument_id);
            const latest = segments[segments.length-1];
            if (latest.end_epoch_ms < latestInstrumentSeen - 2 * 24 * 60 * 60 * 1000) {
                return;
            }

            addSummaryLine(instrument_id + ": " + formatInstrumentDisplay(latest));
        });
    }

    incomingDataContent(content) {
        let target = instrumentSegments.get(content.instrument_id);
        if (target === undefined) {
            target = new Array();
            instrumentSegments.set(content.instrument_id, target);
        }
        if (target.length > 0 && target[target.length-1].end_epoch_ms + 2 * 24 * 60 * 60 * 1000 >= content.start_epoch_ms) {
            const compare = target[target.length-1];
            if (target.instrument_code === compare.instrument_code &&
                target.manufacturer === compare.manufacturer &&
                target.model === compare.model &&
                target.serial_number === compare.serial_number) {
                compare.end_epoch_ms = content.end_epoch_ms;
                return;
            }
        }
        target.push(content);
    }
};

$(document).ready(function() {
    document.getElementById('instrumentsummary_plot').style.display = 'none';
    incomingInstrumentsStream = new InstrumentsStream();
    incomingInstrumentsStream.beginStream();
});

const modalWindow = document.getElementById('modal-container');
const originalHide = modalWindow.onmodalhide;
modalWindow.onmodalhide = function() {
    if (incomingInstrumentsStream) {
        incomingInstrumentsStream.stopStream();
        incomingInstrumentsStream = undefined;
    }
    if (originalHide) {
        originalHide();
    }
};