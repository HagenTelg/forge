import typing


def flow_ccm_to_lpm(ccm: float) -> float:
    return ccm / 1000.0


def flow_ccs_to_lpm(ccs: float) -> float:
    return flow_ccm_to_lpm(ccs * 60.0)


def flow_lpm_to_ccs(lpm: float) -> float:
    return lpm * 1000.0 / 60.0
