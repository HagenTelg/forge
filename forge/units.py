import typing


ZERO_C_IN_K = 273.15
ONE_ATM_IN_HPA = 1013.25


def flow_ccm_to_lpm(ccm: float) -> float:
    return ccm / 1000.0


def flow_ccs_to_lpm(ccs: float) -> float:
    return flow_ccm_to_lpm(ccs * 60.0)


def flow_lpm_to_ccs(lpm: float) -> float:
    return lpm * 1000.0 / 60.0


def flow_lpm_to_m3s(lpm: float) -> float:
    return lpm / (60.0 * 1000.0)


def temperature_k_to_c(k: float) -> float:
    return k - ZERO_C_IN_K
