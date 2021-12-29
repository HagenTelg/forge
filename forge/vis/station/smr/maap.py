import typing
from forge.vis.view.timeseries import TimeSeries


class MAAP5012Optical(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "MAAP"

        mass = TimeSeries.Graph()
        mass.title = "Equivalent Black Carbon"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        mass.axes.append(ugm3)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(ugm3)
            maap.legend = f"MAAP EBC ({size[0]})"
            maap.data_record = f'{mode}-maap-{size[1]}'
            maap.data_field = 'X'
            mass.traces.append(maap)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            aethalometer = TimeSeries.Trace(ugm3)
            aethalometer.legend = f"Aethalometer 660 nm ({size[0]})"
            aethalometer.data_record = f'{mode}-aethalometer-{size[1]}'
            aethalometer.data_field = 'X5'
            mass.traces.append(aethalometer)


        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        absorption.axes.append(Mm_1)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(Mm_1)
            maap.legend = f"MAAP Absorption ({size[0]})"
            maap.data_record = f'{mode}-maap-{size[1]}'
            maap.data_field = 'Ba'
            absorption.traces.append(maap)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            aethalometer = TimeSeries.Trace(Mm_1)
            aethalometer.legend = f"Aethalometer 660 nm ({size[0]})"
            aethalometer.data_record = f'{mode}-aethalometer-{size[1]}'
            aethalometer.data_field = 'Ba5'
            absorption.traces.append(aethalometer)


class EditingMAAP5012(TimeSeries):
    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "MAAP"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        raw.axes.append(ugm3)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        raw.axes.append(Mm_1)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(ugm3)
            maap.legend = f"Raw MAAP EBC ({size[0]})"
            maap.data_record = f'{profile}-raw-maap-{size[1]}'
            maap.data_field = 'X'
            raw.traces.append(maap)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(Mm_1)
            maap.legend = f"Raw MAAP Absorption ({size[0]})"
            maap.data_record = f'{profile}-raw-maap-{size[1]}'
            maap.data_field = 'Ba'
            raw.traces.append(maap)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        edited.axes.append(ugm3)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        edited.axes.append(Mm_1)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(ugm3)
            maap.legend = f"Edited MAAP EBC ({size[0]})"
            maap.data_record = f'{profile}-editing-maap-{size[1]}'
            maap.data_field = 'X'
            edited.traces.append(maap)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            maap = TimeSeries.Trace(Mm_1)
            maap.legend = f"Edited MAAP Absorption ({size[0]})"
            maap.data_record = f'{profile}-editing-maap-{size[1]}'
            maap.data_field = 'Ba'
            edited.traces.append(maap)
