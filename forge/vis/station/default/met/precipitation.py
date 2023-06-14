import typing
from forge.vis.view.timeseries import TimeSeries


class Precipitation(TimeSeries):
    ACCUMULATE_INCOMING = r"""(function() {
const plotIncomingData = incomingData;
let priorInterval = undefined;
let priorEpoch = undefined;
let total = 0.0;
incomingData = (plotTime, values, epoch) => {
    const output = values.slice();
    for (let i=0; i < epoch.length; i++) {
        const interval = Math.floor(epoch[i] / (24 * 60 * 60 * 1000));
        if (interval != priorInterval) {
            priorInterval = interval;
            total = 0.0;
        }
        
        let elapsed = 60 * 1000;
        if (isFinite(priorEpoch)) {
            elapsed = epoch[i] - priorEpoch;
        }
        priorEpoch = epoch[i];
        
        if (isFinite(values[i])) {
            const perMS = values[i] / (60 * 60 * 1000);
            total += perMS * elapsed;
        }
        output[i] = total;
    }
    plotIncomingData(plotTime, output, epoch);
};
})();"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

        precipitation = TimeSeries.Graph()
        precipitation.title = "Precipitation"
        self.graphs.append(precipitation)

        mmh = TimeSeries.Axis()
        mmh.title = "mm/h"
        mmh.range = 0
        mmh.format_code = '.2f'
        precipitation.axes.append(mmh)

        mm = TimeSeries.Axis()
        mm.title = "mm"
        mm.format_code = '.2f'
        mm.range = 0
        precipitation.axes.append(mm)

        rate = TimeSeries.Trace(mmh)
        rate.legend = "Rate"
        rate.data_record = f'{mode}-precipitation'
        rate.data_field = 'precipitation'
        precipitation.traces.append(rate)

        total = TimeSeries.Trace(mm)
        total.legend = "Total"
        total.data_record = f'{mode}-precipitation'
        total.data_field = 'precipitation'
        total.script_incoming_data = self.ACCUMULATE_INCOMING
        precipitation.traces.append(total)
