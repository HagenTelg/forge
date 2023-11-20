import typing


def surface_pressure(station: str) -> float:
    from forge.processing.station.lookup import station_data
    h = station_data(station, 'site', 'altitude')(station)
    if not h:
        return 1013.25
    # Barometric formula calculation
    L = 0.00976  # Temperature lapse rate, K/m
    T0 = 288.16  # Sea level standard temperature, K
    g = 9.80665  # Gravitational acceleration, m/s2
    M = 0.02896968  # Molar mass of dry air, kg/mol
    R0 = 8.314462618  # Universal gas constant, J/(mol·K)
    return 1013.25 * (1 - L * h / T0) ** ((g * M) / (R0 * L))
