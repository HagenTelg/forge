$('#passedsummary-ok').click(function(event) {
    event.preventDefault();
    hideModal();
});

const outputPassedSegments = new Array();
function overlayPassed(segment) {
    function intersects(a_start, a_end, b_start, b_end) {
        if (a_start >= b_end)
            return false;
        if (b_start >= a_end)
            return false;
        return true;
    }
    function before(search_start) {
        let existing_index = 0
        let end_index = outputPassedSegments.length;
        while (existing_index < end_index) {
            const mid = Math.trunc((existing_index + end_index) / 2);
            if (outputPassedSegments[mid].start_epoch_ms < search_start) {
                existing_index = mid + 1;
            } else {
                end_index = mid;
            }
        }
        return existing_index;
    }
    function findBeforeStart(search_start) {
        return Math.max(0, before(search_start) - 1);
    }
    function subtract(sub_start, sub_end) {
        let existing_index = findBeforeStart(sub_start);

        while (existing_index < outputPassedSegments.length) {
            const inspect_start = outputPassedSegments[existing_index].start_epoch_ms;
            const inspect_end = outputPassedSegments[existing_index].end_epoch_ms;
            if (!intersects(sub_start, sub_end, inspect_start, inspect_end)) {
                if (inspect_start >= sub_end) {
                    break;
                }
                existing_index += 1;
                continue;
            }
            if (inspect_start >= sub_start && inspect_end <= sub_end) {
                outputPassedSegments.splice(existing_index, 1);
                continue;
            }
            if (sub_start > inspect_start) {
                outputPassedSegments[existing_index].end_epoch_ms = sub_start;
                if (sub_end >= inspect_end) {
                    existing_index += 1;
                    continue;
                }
                const pass_time_epoch_ms = outputPassedSegments[existing_index].pass_time_epoch_ms;
                const comment = outputPassedSegments[existing_index].comment;
                outputPassedSegments.splice(existing_index+1, 0, {
                    start_epoch_ms: sub_end,
                    end_epoch_ms: inspect_end,
                    pass_time_epoch_ms: pass_time_epoch_ms,
                    comment: comment,
                });
                existing_index += 2
                continue;
            }

            outputPassedSegments[existing_index].start_epoch_ms = sub_end;
            existing_index += 1;
        }
    }

    subtract(segment.start_epoch_ms, segment.end_epoch_ms);
    const idx = before(segment.start_epoch_ms);
    outputPassedSegments.splice(idx, 0, segment);
}

function showPlot() {
    const div = document.getElementById('passedsummary_plot');
    div.style.display = '';

    const data = [];
    const layout = {
        autosize : true,
        showlegend: false,
        barmode: 'stack',
        hoverdistance: -1,

        height: 150,
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
                range: [
                    DataSocket.toPlotTime(outputPassedSegments[0].start_epoch_ms),
                    DataSocket.toPlotTime(outputPassedSegments[outputPassedSegments.length-1].end_epoch_ms)
                ],
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
            tickvals: [0.5],
            ticktext: ["Passed"],
            ticklabelstandoff: 5,
        },

        shapes: [],
    };
    const config = {
        responsive: true,
    };

    const passedHoverTrace = {
        type: 'scatter',
        x: [],
        y: [],
        opacity: 0,
        mode: 'none',
        hoverinfo: 'text',
        hovertext: [],
    };
    data.push(passedHoverTrace);

    const gapSegments = [];
    let priorPassedEnd = 0;
    for (const segment of outputPassedSegments) {
        if (priorPassedEnd + 1000 < segment.start_epoch_ms) {
            if (priorPassedEnd > 0) {
                gapSegments.push([priorPassedEnd, segment.start_epoch_ms]);
            }
            priorPassedEnd = segment.start_epoch_ms;
        }

        layout.shapes.push({
            type: 'rect',
            xref: 'x',
            yref: 'y',
            x0: DataSocket.toPlotTime(priorPassedEnd),
            y0: 0.1,
            x1: DataSocket.toPlotTime(segment.end_epoch_ms),
            y1: 0.9,
            fillcolor: '#777',
            line: {
                color: '#000',
                width: 1,
            },
        });
        priorPassedEnd = segment.end_epoch_ms;

        passedHoverTrace.x.push(DataSocket.toPlotTime((segment.end_epoch_ms + segment.start_epoch_ms) / 2));
        passedHoverTrace.y.push(0.5);
        let hoverText = TimeParse.toDisplayTime(segment.start_epoch_ms) + " - " + TimeParse.toDisplayTime(segment.end_epoch_ms);
        if (segment.comment) {
            hoverText += "<br>" + segment.comment;
        }
        passedHoverTrace.hovertext.push(hoverText);
    }

    if (gapSegments.length > 0) {
        layout.yaxis.range = [1.25, 0];
        layout.yaxis.tickvals.push(1.1);
        layout.yaxis.ticktext.push("Gap");

        const gapHoverTrace = {
            type: 'scatter',
            x: [],
            y: [],
            opacity: 0,
            mode: 'none',
            hoverinfo: 'text',
            hovertext: [],
        };
        data.push(gapHoverTrace);
        for (const segment of gapSegments) {
            layout.shapes.push({
                type: 'rect',
                xref: 'x',
                yref: 'y',
                x0: DataSocket.toPlotTime(segment[0]),
                y0: 1.05,
                x1: DataSocket.toPlotTime(segment[1]),
                y1: 1.15,
                fillcolor: '#A00',
                line: {
                    color: '#A00',
                    width: 2,
                },
            });

            gapHoverTrace.x.push(DataSocket.toPlotTime((segment[0] + segment[1]) / 2));
            gapHoverTrace.y.push(1.1);

            let hoverText = "Gap at " + TimeParse.toDisplayTime(segment[0]) + " - " + TimeParse.toDisplayTime(segment[1]);
            gapHoverTrace.hovertext.push(hoverText);
        }
    }

    Plotly.newPlot(div, data, layout, config);
}

let incomingPassedStream = undefined;
const PassedStream = class extends DataSocket.Stream {
    constructor() {
        super('{{ mode_name }}-passed');
        this._segments = new Array();
    }

    endOfData() {
        incomingPassedStream = undefined;

        this._segments.sort((a, b) => {
            if (a.pass_time_epoch_ms < b.pass_time_epoch_ms) {
                return -1;
            } else if (a.pass_time_epoch_ms > b.pass_time_epoch_ms) {
                return 1;
            } else {
                return 0;
            }
        });

        outputPassedSegments.length = 0;
        for (const s of this._segments) {
            overlayPassed(s);
        }

        document.getElementById('passedsummary_loading').style.display = 'none';

        const details = document.getElementById('passedsummary_details');
        while (details.firstChild) {
            details.removeChild(details.firstChild);
        }

        if (outputPassedSegments.length === 0) {
            details.textContent = 'No data passed yet.';
            return;
        }

        showPlot();

        function addSummaryLine(text) {
            const span = document.createElement('span');
            span.textContent = text;
            details.appendChild(span);
        }

        const latestPassed = outputPassedSegments[outputPassedSegments.length - 1];
        let endPassDisplay = "Latest passed: " + TimeParse.toDisplayTime(latestPassed.end_epoch_ms, '', ' ');
        const daysAgo = Math.floor(
            ((new Date()).getTime() - latestPassed.end_epoch_ms) /
            (24 * 60 * 60 * 1000)
        );
        if (daysAgo > 1) {
            endPassDisplay += " (" + daysAgo + " days behind)";
        }
        addSummaryLine(endPassDisplay);

        addSummaryLine("Passed at: " + TimeParse.toDisplayTime(latestPassed.pass_time_epoch_ms, '', ' '));
    }

    incomingDataContent(content) {
        this._segments.push(content);
    }
};

$(document).ready(function() {
    document.getElementById('passedsummary_plot').style.display = 'none';
    incomingPassedStream = new PassedStream();
    incomingPassedStream.beginStream();
});

const modalWindow = document.getElementById('modal-container');
const originalHide = modalWindow.onmodalhide;
modalWindow.onmodalhide = function() {
    if (incomingPassedStream) {
        incomingPassedStream.stopStream();
        incomingPassedStream = undefined;
    }
    exportSocket.close();
    if (originalHide) {
        originalHide();
    }
};