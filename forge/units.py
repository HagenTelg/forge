import typing


ZERO_C_IN_K = 273.15
ONE_ATM_IN_HPA = 1013.25


def flow_ccm_to_lpm(ccm: float) -> float:
    return ccm / 1000.0


def flow_ccs_to_lpm(ccs: float) -> float:
    return flow_ccm_to_lpm(ccs * 60.0)


def flow_m3s_to_lpm(m3s: float) -> float:
    return m3s * (1000 * 60.0)


def flow_lpm_to_ccm(lpm: float) -> float:
    return lpm * 1000.0


def flow_lpm_to_ccs(lpm: float) -> float:
    return flow_lpm_to_ccm(lpm) / 60.0


def flow_lpm_to_m3s(lpm: float) -> float:
    return lpm / (60.0 * 1000.0)


def temperature_k_to_c(k: float) -> float:
    return k - ZERO_C_IN_K


def pressure_Pa_to_hPa(p: float) -> float:
    return p / 100.0


def pressure_kPa_to_hPa(p: float) -> float:
    return p * 10.0


def pressure_mmHg_to_hPa(p: float) -> float:
    return p * 1.33322387415


def mass_ng_to_ug(m: float) -> float:
    return m / 1000.0


def concentration_ppm_to_ppb(x: float) -> float:
    return x * 1000.0
