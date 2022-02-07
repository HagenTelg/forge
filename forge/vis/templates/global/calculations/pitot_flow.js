const stackPitotArea = (function() {
    let d = 1.75 * 25.4;
    d /= 1000.0;
    d /= 2.0;
    return d * d * Math.PI;
})();

function calculatePitotFlow(dP, A, t, p) {
    if (!isFinite(dP) || dP < 0.0) {
        return undefined;
    }
    if (!isFinite(A) || A <= 0.0) {
        return undefined;
    }
    if (!isFinite(t) || t <= 0.0) {
        return undefined;
    }
    if (!isFinite(p) || p <= 0.0) {
        return undefined;
    }

    let density = 1.2922;   /* kg/m^3 at 0C */
    density *= (273.15 / t) * (p / 1013.25);

    const v = Math.sqrt(2.0 * (dP * 100.0) / density);   /* m/s */
    let Q = v * A;   /* m^3/s */

    Q *= 60000.0;   /* lpm */
    Q *= (p / 1013.25) * (273.15 / t);  /* To mass flow */

    return Q;
}
