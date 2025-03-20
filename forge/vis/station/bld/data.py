import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("bld"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    class RealtimeData(RealtimeTranslator.Data):
        PUBLIC_DATA = {
            'public-realtime-ozone': 'ozone-realtime-ozone'
        }

        def get(self, key, default=None) -> typing.Callable[[str, int, int, typing.Callable], typing.Optional[DataStream]]:
            key = self.PUBLIC_DATA.get(key, key)
            return super().get(key, default)


    station_profile_data['public'] = {
        'realtime': RealtimeData('public', {
            'ozone': {
                RealtimeTranslator.Key('X_G81'): 'ozone',
            },
        }),
    }



    station_profile_data['ozone']['raw']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X1_G82'): 'no2',
            Name(station, 'raw', 'X2_G82'): 'no',
            Name(station, 'raw', 'X3_G82'): 'nox',
        }, send
    )
    station_profile_data['ozone']['realtime']['nox'] = {
        RealtimeTranslator.Key('X1_G82'): 'no2',
        RealtimeTranslator.Key('X2_G82'): 'no',
        RealtimeTranslator.Key('X3_G82'): 'nox',
    }
    station_profile_data['ozone']['editing']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X1_G82'): 'no2',
            Name(station, 'clean', 'X2_G82'): 'no',
            Name(station, 'clean', 'X3_G82'): 'nox',
        }, send
    )
    station_profile_data['ozone']['clean']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X1_G82'): 'no2',
            Name(station, 'clean', 'X2_G82'): 'no',
            Name(station, 'clean', 'X3_G82'): 'nox',

        }, send
    )
    station_profile_data['ozone']['avgh']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X1_G82'): 'no2',
            Name(station, 'avgh', 'X2_G82'): 'no',
            Name(station, 'avgh', 'X3_G82'): 'nox',
        }, send
    )

    station_profile_data['ozone']['raw']['noxstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_G82'): 'Tmanifold',
            Name(station, 'raw', 'T2_G82'): 'Toven',
            Name(station, 'raw', 'T3_G82'): 'Tbox',
            Name(station, 'raw', 'P_G82'): 'Psample',
        }, send
    )
    station_profile_data['ozone']['realtime']['noxstatus'] = {
        RealtimeTranslator.Key('T1_G82'): 'Tmanifold',
        RealtimeTranslator.Key('T2_G82'): 'Toven',
        RealtimeTranslator.Key('T3_G82'): 'Tbox',
        RealtimeTranslator.Key('P_G82'): 'Psample',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, ozone_data, ozone_public, radiation_data, data_get, \
        DataStream, DataRecord, RealtimeRecord, \
        Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(ozone_data)
    data_records.update(ozone_public)
    data_records.update(radiation_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"ozone-{archive}-nox"] = DataRecord({
            "no2": [Selection(variable_name="nitrogen_dioxide_mixing_ratio", exclude_tags={"secondary"})],
            "no": [Selection(variable_name="nitrogen_monoxide_mixing_ratio", exclude_tags={"secondary"})],
            "nox": [Selection(variable_name="nox_mixing_ratio", exclude_tags={"secondary"})],
        })
    data_records[f"ozone-realtime-nox"] = RealtimeRecord({
        "no2": [RealtimeSelection("XNO2", variable_name="nitrogen_dioxide_mixing_ratio", exclude_tags={"secondary"})],
        "no": [RealtimeSelection("XNO", variable_name="nitrogen_monoxide_mixing_ratio", exclude_tags={"secondary"})],
        "nox": [RealtimeSelection("XNOx", variable_name="nox_mixing_ratio", exclude_tags={"secondary"})],
    })

    data_records[f"ozone-raw-noxstatus"] = DataRecord({
        "Tmanifold": [Selection(variable_name="manifold_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Toven": [Selection(variable_name="oven_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Tbox": [Selection(variable_name="box_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="pressure", instrument_code="teledynen500", exclude_tags={"secondary"})],
    })
    data_records[f"ozone-realtime-noxstatus"] = RealtimeRecord({
        "Tmanifold": [RealtimeSelection("Tmanifold", variable_name="manifold_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Toven": [RealtimeSelection("Toven", variable_name="oven_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Tbox": [RealtimeSelection("Tbox", variable_name="box_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("Psample", variable_name="pressure", instrument_code="teledynen500", exclude_tags={"secondary"})],
    })

    data_records[f"public-realtime-ozone"] = RealtimeRecord({
        "ozone": [RealtimeSelection("X", standard_name="mole_fraction_of_ozone_in_air",
                                    require_tags={"ozone"}, exclude_tags={"secondary"})],
    })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)