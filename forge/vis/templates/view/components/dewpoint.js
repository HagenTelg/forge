var Dewpoint = {};
(function() {

    /*
     * See http://www.decatur.de/javascript/dew/dew.js
     * Saturation Vapor Pressure formula for range -100..0 Deg. C.
     * This is taken from
     *   ITS-90 Formulations for Vapor Pressure, Frostpoint Temperature,
     *   Dewpoint Temperature, and Enhancement Factors in the Range 100 to +100 C
     * by Bob Hardy
     * as published in "The Proceedings of the Third International Symposium on Humidity & Moisture",
     * Teddington, London, England, April 1998
     */
    function svpIce(t) {
        if (t <= 0.0) {
            return undefined;
        }
        return Math.exp(-5.8666426e3 / t +
            2.232870244e1 +
            (1.39387003e-2 + (-3.4262402e-5 + (2.7040955e-8 * t)) * t) * t +
            6.7063522e-1 * Math.log(t));
    }

    /*
     * See http://www.decatur.de/javascript/dew/dew.js
     * Saturation Vapor Pressure formula for range 273..678 Deg. K.
     * This is taken from the
     *   Release on the IAPWS Industrial Formulation 1997
     *   for the Thermodynamic Properties of Water and Steam
     * by IAPWS (International Association for the Properties of Water and Steam),
     * Erlangen, Germany, September 1997.
     *
     * This is Equation (30) in Section 8.1 "The Saturation-Pressure Equation (Basic Equation)"
     */
    function svpWater(t) {
        if (t <= 0.0) {
            return undefined;
        }
        const th = t - 0.23855557567849 / (t - 0.65017534844798e3);
        const A = (th + 0.11670521452767e4) * th - 0.72421316703206e6;
        const B = (-0.17073846940092e2 * th + 0.12020824702470e5) * th - 0.32325550322333e7;
        const C = (0.14915108613530e2 * th - 0.48232657361591e4) * th + 0.40511340542057e6;

        let p = 2.0 * C / (-B + Math.sqrt(B * B - 4 * A * C));
        p *= p;
        p *= p;
        return p * 1e6;
    }

    function svp(t, forceWater) {
        if (!forceWater && t < 273.15)
            return svpIce(t);
        return svpWater(t);
    }

    function svpSolve(svpTarget, t, forceWater) {
        return newtonRaphson(svpTarget, (x) => { return svp(x, forceWater); }, t, t + 1.0);
    }

    Dewpoint.TD = function(t, rh, forceWater) {
        if (!isFinite(t) || !isFinite(rh)) {
            return undefined;
        }
        if (rh <= 0.0 || rh > 100.0 || t < -100.0 || t > 400.0) {
            return undefined;
        }

        t += 273.15;

        let svpTarget = svp(t, forceWater);
        if (!isFinite(svpTarget)) {
            return undefined;
        }
        svpTarget *= (rh / 100.0);

        let result = svpSolve(svpTarget, t, forceWater);
        if (!isFinite(result)) {
            return undefined;
        }
        return result - 273.15;
    }

    Dewpoint.RH = function(t, td, forceWater) {
        if (!isFinite(t) || !isFinite(td)) {
            return undefined;
        }
        if (t < -100.0 || t > 400.0 || td < -100.0 || td > 400.0) {
            return undefined;
        }
        t += 273.15;
        td += 273.15;

        const svpT = svp(t, forceWater);
        if (!isFinite(svpT) || svpT === 0.0) {
            return undefined;
        }
        const svpTD = svp(td, forceWater);
        if (!isFinite(svpTD)) {
            return undefined;
        }
        return (svpTD / svpT) * 100.0;
    }

    Dewpoint.T = function(rh, td, forceWater) {
        if (!isFinite(rh) || !isFinite(td)) {
            return undefined;
        }
        if (rh <= 0.0 || rh > 100.0 || td < -100 || td > 400.0) {
            return undefined;
        }
        td += 273.15;

        let svpTarget = svp(td, forceWater);
        if (!isFinite(yTarget)) {
            return undefined;
        }
        svpTarget /= (rh / 100.0);

        return svpSolve(svpTarget, td, forceWater);
    }

    const recordFieldMatch = /^(TD|T|U)(.+)$/;
    Dewpoint.CalculateDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName) {
            super(dataName);
            this.forceWater = false;
        }

        processRecord(record, epoch) {
            const parameters = new Map();

            record.forEach((fieldData, fieldName) => {
                const matched = recordFieldMatch.exec(fieldName);
                if (!matched) {
                    return;
                }

                let parameterData = parameters.get(matched[2]);
                if (!parameterData) {
                    parameterData = {};
                    parameters.set(matched[2], parameterData);
                }

                parameterData[matched[1]] = fieldData;
            });

            function getParameter(parameterData, parameterName, parameterType) {
                let fieldData = parameterData[parameterType];
                if (fieldData) {
                    return fieldData;
                }

                fieldData = [];
                for (let i=0; i<epoch.length; i++) {
                    fieldData.push(undefined);
                }

                parameterData[parameterType] = fieldData;
                record.set(parameterType + parameterName, fieldData);
                return fieldData;
            }

            parameters.forEach((parameterData, parameterName) => {
                const T = getParameter(parameterData, parameterName, 'T');
                const TD = getParameter(parameterData, parameterName, 'TD');
                const U = getParameter(parameterData, parameterName, 'U');

                for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                    const Tvalue = T[timeIndex];
                    const TDvalue = TD[timeIndex];
                    const Uvalue = U[timeIndex];

                    if (!isFinite(TDvalue)) {
                        TD[timeIndex] = Dewpoint.TD(Tvalue, Uvalue, this.forceWater);
                    } else if (!isFinite(Uvalue)) {
                        U[timeIndex] = Dewpoint.RH(Tvalue, TDvalue, this.forceWater);
                    } else if (!isFinite(Tvalue)) {
                        T[timeIndex] = Dewpoint.T(Uvalue, TDvalue, this.forceWater);
                    }
                }
            });
        }
    }
})();