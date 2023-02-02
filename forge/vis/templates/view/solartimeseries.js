const latitude = '{{ latitude }}' * 1.0;
const longitude = '{{ longitude }}' * 1.0;
const solarTime = new Solar.Time(latitude, longitude);

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
            //{% for graph in view.graphs %}
            shapes.push({
                type: 'rect',
                xref: 'x',
                yref: 'y{% if loop.index > 1 %}{{ loop.index }}{% endif %} domain',
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
            //{% endfor %}
        }

        priorSunset = sunset;
    }

    return shapes;
});

shapeHandler.update();