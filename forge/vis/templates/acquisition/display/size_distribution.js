// {{ '\n' }}{% include 'global/calculations/size_distribution.js' %}

const layout = {
    autosize: false,
    width: 800,
    height: 300,

    hovermode: 'x unified',
    showlegend: false,

    xaxis: {
        title: "D (μm)",
        hoverformat: ".3f",
        type: 'log',
        automargin: true,
    },

    yaxis2: {
        side: 'left',
        title: "dN/dlog(Dp) (cm⁻³)",
        hoverformat: ".1f",
        rangemode: 'tozero',
        automargin: true,
        overlaying: 'y',
    },
    yaxis: {
        side: 'right',
        title: "dN (cm⁻³)",
        hoverformat: ".1f",
        rangemode: 'tozero',
        automargin: true,
        zeroline: false,
        showgrid: false,
    },

    bargap: 0,
    margin: {
        t: 10,
        b: 10,
        l: 10,
        r: 10,
    },

    datarevision: 0,
};

const plot_dNdlogDp = {
    x: [ ],
    y: [ ],
    type: 'scatter',
    mode: 'lines',
    yaxis: 'y2',
    name: "dN/dlog(Dp)",
    hovertemplate: "%{y:.3f} cm⁻³",
    line: {
        width: 2,
        color: '#00f',
    },
};
const plot_dN = {
    x: [ ],
    y: [ ],
    width: [ ],
    type: 'bar',
    yaxis: 'y',
    name: "dN",
    hovertemplate: "%{y:.1f} cm⁻³",
    opacity: 0.5,
    marker: {
        color: '#000',
        line: {
            width: 0,
            opacity: 0,
        },
    }
};

const data = [
    plot_dN,
    plot_dNdlogDp,
];

const plotDiv = document.getElementById('{{uid}}_size_distribution');
Plotly.newPlot(plotDiv, data, layout);


let activeReplot = undefined;
function replot() {
    if (activeReplot) {
        return;
    }

    function applyReplot() {
        activeReplot = undefined;


        plot_dNdlogDp.x = plot_dN.x;
        plot_dNdlogDp.y.length = 0;
        for (let binIndex=0; binIndex<Math.max(plot_dNdlogDp.x.length, plot_dN.y.length); binIndex++) {
            const t = denormalizationFactor(plot_dNdlogDp.x, binIndex);
            const denormalized = plot_dN.y[binIndex];

            if (!isFinite(t) || t <= 0.0 || !isFinite(denormalized)) {
                plot_dNdlogDp.y.push(undefined);
            } else {
                plot_dNdlogDp.y.push(denormalized / t);
            }
        }

        plot_dN.width.length = 0;
        if (plot_dN.x.length === 1) {
            plot_dN.width.push(plot_dN.x[0]);
        } else {
            for (let binIndex=0; binIndex<plot_dN.x.length; binIndex++) {
                if (binIndex === 0) {
                    plot_dN.width.push(plot_dN.x[1] - plot_dN.x[0]);
                } else if (binIndex === plot_dN.x.length-1) {
                    plot_dN.width.push(plot_dN.x[plot_dN.x.length-1] - plot_dN.x[plot_dN.x.length-2]);
                } else {
                    const lower = plot_dN.x[binIndex - 1];
                    const upper = plot_dN.x[binIndex + 1];
                    plot_dN.width.push((upper - lower) / 2);
                }
            }
        }

        layout.datarevision += 1;
        Plotly.react(plotDiv, data, layout);
    }
    activeReplot = setTimeout(applyReplot, 100);
}

context.addSourceTarget((value) => {
    if (!value || !value.length) {
        return;
    }
    plot_dN.x.length = 0;
    for (let i=0; i<value.length; i++)  {
        const v = value[i];
        if (v === null || !isFinite(v) || v < 0.0) {
            continue;
        }
        while (i >= plot_dN.x.length) {
            plot_dN.x.push(undefined);
        }
        plot_dN.x[i] = v;
    }

    replot();
}, context.source, 'Dp');
context.addSourceTarget((value) => {
    if (!value || !value.length) {
        return;
    }
    plot_dN.y.length = 0;
    for (let i=0; i<value.length; i++)  {
        const v = value[i];
        if (v === null || !isFinite(v) || v < 0.0) {
            continue;
        }
        while (i >= plot_dN.y.length) {
            plot_dN.y.push(undefined);
        }
        plot_dN.y[i] = v;
    }

    replot();
}, context.source, 'dN');