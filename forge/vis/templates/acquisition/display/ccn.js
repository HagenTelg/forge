// {{ '\n' }}{% include 'global/calculations/size_distribution.js' %}

const layout = {
    autosize: false,
    width: 800,
    height: 300,

    hovermode: 'x unified',
    showlegend: false,

    xaxis: {
        title: { text: "Bin Index" },
        hoverformat: ".0f",
        automargin: true,
    },

    yaxis: {
        side: 'left',
        title: { text: "dN (cm⁻³)" },
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

const plot_dN = {
    x: [ ],
    y: [ ],
    type: 'bar',
    yaxis: 'y',
    name: "dN",
    hovertemplate: "%{y:.1f} cm⁻³",
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
];

const plotDiv = document.getElementById('{{uid}}_bins');
Plotly.newPlot(plotDiv, data, layout);


let activeReplot = undefined;
function replot() {
    if (activeReplot) {
        return;
    }

    function applyReplot() {
        activeReplot = undefined;

        layout.datarevision += 1;
        Plotly.react(plotDiv, data, layout);
    }
    activeReplot = setTimeout(applyReplot, 100);
}

context.addSourceTarget((value) => {
    if (!value || !value.length) {
        return;
    }
    plot_dN.y.length = 0;
    plot_dN.x.length = 0;
    for (let i=0; i < value.length; i++)  {
        const v = value[i];
        if (v === null || !isFinite(v) || v < 0.0) {
            continue;
        }
        while (i >= plot_dN.y.length) {
            plot_dN.y.push(undefined);
            plot_dN.x.push(i+1);
        }
        plot_dN.y[i] = v;
    }

    replot();
}, context.source, 'dN');