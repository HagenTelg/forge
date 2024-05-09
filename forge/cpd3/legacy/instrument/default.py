import typing
from netCDF4 import Dataset

if typing.TYPE_CHECKING:
    from ..raw.analyze import Instrument as RawInstrument


def convert_raw(source: "RawInstrument", station: str, instrument_id: str,
                file_start: float, file_end: float, root: Dataset) -> bool:
    if source.source.cpd3_component == "acquire_2b_ozone205" or source.source.forge_instrument == "tech2b205":
        from .tech2b205 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_ad_cpcmagic200" or source.source.forge_instrument == "admagic200cpc":
        from .admagic200cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_ad_cpcmagic250" or source.source.forge_instrument == "admagic250cpc":
        from .admagic250cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_bmi_cpc1710" or source.source.forge_instrument == "bmi1710cpc":
        from .bmi1710cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_bmi_cpc1720" or source.source.forge_instrument == "bmi1720cpc":
        from .bmi1720cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_csd_pops" or source.source.forge_instrument == "csdpops":
        from .csdpops import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_dmt_ccn" or source.source.forge_instrument == "dmtccn":
        from .dmtccn import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_ecotech_nephaurora" or source.source.forge_instrument == "ecotechnephelometer":
        from .ecotechnephelometer import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_gill_windsonic" or source.source.forge_instrument == "gillwindsonic":
        from .gillwindsonic import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_gmd_clap3w" or source.source.forge_instrument == "clap":
        from .clap import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_gmd_cpcpulse" or source.source.forge_instrument == "tsi3760cpc":
        from .tsi3760cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_grimm_opc110x" or source.source.forge_instrument == "grimm110xopc":
        from .grimm110xopc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_magee_aethalometer33" or source.source.forge_instrument == "mageeae33":
        from .mageeae33 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_magee_aethalometer162131" or source.source.forge_instrument == "mageeae31":
        from .mageeae31 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_purpleair_pa2" or source.source.forge_instrument == "purpleair":
        from .purpleair import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_rmy_wind86xxx" or source.source.forge_instrument == "rmy86xxx":
        from .rmy86xxx import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_teledyne_t640" or source.source.forge_instrument == "teledynet640":
        from .teledynet640 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_thermo_maap5012" or source.source.forge_instrument == "thermomaap":
        from .thermomaap import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_thermo_ozone49" or source.source.forge_instrument == "thermo49":
        from .thermo49 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_thermo_ozone49iq" or source.source.forge_instrument == "thermo49iq":
        from .thermo49iq import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_cpc3010" or source.source.forge_instrument == "tsi3010cpc":
        from .tsi3010cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_cpc302x" or source.source.forge_instrument == "tsi302xcpc":
        from .tsi302xcpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_cpc377x" or source.source.forge_instrument == "tsi377xcpc":
        from .tsi377xcpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_cpc3781" or source.source.forge_instrument == "tsi3781cpc":
        from .tsi3781cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_cpc3783" or source.source.forge_instrument == "tsi3783cpc":
        from .tsi3781cpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_mfm4xxx" or source.source.forge_instrument == "tsimfm":
        from .tsimfm import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_tsi_neph3563" or source.source.forge_instrument == "tsi3563nephelometer":
        from .tsi3563nephelometer import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_vaisala_pwdx2" or source.source.forge_instrument == "vaisalapwdx2":
        from .vaisalapwdx2 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_vaisala_wmt700" or source.source.forge_instrument == "vaisalawmt700":
        from .vaisalawmt700 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.cpd3_component == "acquire_vaisala_wxt5xx" or source.source.forge_instrument == "vaisalawxt5xx":
        from .vaisalawxt5xx import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.forge_instrument == "bmitap":
        from .bmitap import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.forge_instrument == "teledynen500":
        from .teledynen500 import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    if source.source.forge_instrument == "tsi375xcpc":
        from .tsi375xcpc import Converter
        return Converter(station, instrument_id, file_start, file_end, root).run()
    return False
