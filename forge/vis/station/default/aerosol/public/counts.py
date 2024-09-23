import typing
from forge.vis.view.timeseries import PublicTimeSeries


class PublicCountsShort(PublicTimeSeries):
    class STP(PublicTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('stp')
            self.script = r"""(function(dataName) {
    return new STP.CorrectOpticalDispatch(dataName, 'N', 'T', 'P', 12.0, NOMINAL_PRESSURE);
})"""

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        cnc = PublicTimeSeries.Graph()
        cnc.title = "Condensation Nucleus Concentration"
        self.graphs.append(cnc)

        cm_3 = PublicTimeSeries.Axis()
        cm_3.title = "Concentration (cm⁻³)"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = PublicTimeSeries.Trace(cm_3)
        n_cnc.legend = "CPC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'N'
        cnc.traces.append(n_cnc)
        self.processing[n_cnc.data_record] = self.STP()


        cpc_flow = PublicTimeSeries.Graph()
        cpc_flow.title = "CPC Flow"
        self.graphs.append(cpc_flow)

        lpm_cpc = PublicTimeSeries.Axis()
        lpm_cpc.title = "CPC (lpm)"
        lpm_cpc.format_code = '.3f'
        cpc_flow.axes.append(lpm_cpc)

        lpm_drier = PublicTimeSeries.Axis()
        lpm_drier.title = "Drier (lpm)"
        lpm_drier.format_code = '.3f'
        cpc_flow.axes.append(lpm_drier)

        sample = PublicTimeSeries.Trace(lpm_cpc)
        sample.legend = "Outlet"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Qsample'
        cpc_flow.traces.append(sample)

        drier = PublicTimeSeries.Trace(lpm_drier)
        drier.legend = "Drier"
        drier.data_record = f'{mode}-cpcstatus'
        drier.data_field = 'Qdrier'
        cpc_flow.traces.append(drier)


class PublicCountsLong(PublicCountsShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

