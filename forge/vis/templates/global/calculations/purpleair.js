const purpleAirScatteringEfficiency = 0.015;

function calculatePurpleAirScattering(A, B) {
    if (!isFinite(A)) {
        if (!isFinite(B)) {
            return undefined;
        }
        return B * purpleAirScatteringEfficiency;
    }
    if (!isFinite(B)) {
        return A * purpleAirScatteringEfficiency;
    }

    return (A + B) / 2.0 * purpleAirScatteringEfficiency;
}