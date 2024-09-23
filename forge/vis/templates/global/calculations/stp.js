function calculateAirDensity(temperature, pressure) {
    if (!isFinite(pressure) || pressure < 10.0 || pressure > 2000.0) {
        return undefined;
    }
    if (!isFinite(temperature)) {
        return undefined;
    }
    if (temperature < 150.0) {
        temperature += 273.15;
    }
    if (temperature < 100.0 || temperature > 350.0) {
        return undefined;
    }
    return (pressure / 1013.25) * (273.15 / temperature);
}

function correctOpticalSTP(value, temperature, pressure) {
    if (!isFinite(value)) {
        return undefined;
    }
    const density = calculateAirDensity(temperature, pressure);
    if (density === undefined) {
        return undefined;
    }
    return value / density;
}

