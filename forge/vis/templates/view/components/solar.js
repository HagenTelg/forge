var Solar = {};
(function() {
    /*
     * https://www.esrl.noaa.gov/gmd/grad/solcalc/
     */

    function toRadians(degrees)
    { return degrees * (Math.PI / 180.0); }

    function toDegrees(radians)
    { return radians * (180.0 / Math.PI); }

    function toJulianCentury(julianDay)
    { return (julianDay - 2451545.0) / 36525.0; }

    function calculateGeomMeanLongSun(julianCentury) {
        let L0 = 280.46646 + julianCentury * (36000.76983 + julianCentury * (0.0003032));
        L0 -= Math.floor(L0 / 360.0) * 360.0;
        return toRadians(L0);
    }

    function calculateGeomMeanAnomalySun(julianCentury)
    { return toRadians(357.52911 + julianCentury * (35999.05029 - 0.0001537 * julianCentury)); }

    function calculateEccentricityEarthOrbit(julianCentury)
    { return 0.016708634 - julianCentury * (0.000042037 + 0.0000001267 * julianCentury); }

    function calculateSunEqOfCenter(julianCentury, meanAnomalySun) {
        const sinm = Math.sin(meanAnomalySun);
        const sin2m = Math.sin(meanAnomalySun * 2.0);
        const sin3m = Math.sin(meanAnomalySun * 3.0);
        const C = sinm * (1.914602 - julianCentury * (0.004817 + 0.000014 * julianCentury)) +
            sin2m * (0.019993 - 0.000101 * julianCentury) +
            sin3m * 0.000289;
        return toRadians(C);
    }

    function calculateSunTrueLong(geomMeanLongSun, sunEqOfCenter)
    { return geomMeanLongSun + sunEqOfCenter; }

    /*function calculateSunTrueAnomaly(geomMeanLongSun, sunEqOfCenter)
    { return geomMeanLongSun + sunEqOfCenter; }

    function calculateSunRadVector(sunTrueAnomaly, eccentricityEarthOrbit)
    {
        return (1.000001018 * (1 - eccentricityEarthOrbit * eccentricityEarthOrbit)) /
            (1 + eccentricityEarthOrbit * Math.cos(sunTrueAnomaly));
    }*/
    
    function calculateSunApparentLong(julianCentury, sunTrueLong) {
        const o = toDegrees(sunTrueLong);
        const omega = 125.04 - 1934.136 * julianCentury;
        const lambda = o - 0.00569 - 0.00478 * Math.sin(toRadians(omega));
        return toRadians(lambda);
    }
    
    /*function calculateMeanObliquityOfEcliptic(julianCentury) {
        const seconds = 21.448 - julianCentury * (46.8150 + julianCentury * (0.00059 - julianCentury * (0.001813)));
        const e0 = 23.0 + (26.0 + (seconds / 60.0)) / 60.0;
        return toRadians(e0);
    }*/
    
    function calculateObliquityCorrection(julianCentury) {
        const seconds = 21.448 - julianCentury * (46.8150 + julianCentury * (0.00059 - julianCentury * (0.001813)));
        const e0 = 23.0 + (26.0 + (seconds / 60.0)) / 60.0;
        const omega = 125.04 - 1934.136 * julianCentury;
        const e = e0 + 0.00256 * Math.cos(toRadians(omega));
        return toRadians(e);
    }

    /*function calcSunRtAscension(obliquityCorrection, sunApparentLong) {
        const tananum = (Math.cos(obliquityCorrection) * Math.sin(sunApparentLong));
        const tanadenom = (Math.cos(sunApparentLong));
        return Math.atan2(tananum, tanadenom);
    }*/

    function calculateSunDeclination(obliquityCorrection, sunApparentLong) {
        const sint = Math.sin(obliquityCorrection) * Math.sin(sunApparentLong);
        return Math.asin(sint);
    }
    
    function calculateEquationOfTime(JulianCenturyOReccentricityEarthOrbit,
                                     obliquityCorrection, geomMeanLongSun, geomMeanAnomalySun) {
        let eccentricityEarthOrbit = JulianCenturyOReccentricityEarthOrbit;
        if (!isFinite(obliquityCorrection)) {
            const julianCentury = JulianCenturyOReccentricityEarthOrbit;
            eccentricityEarthOrbit = calculateEccentricityEarthOrbit(julianCentury);
            obliquityCorrection = calculateObliquityCorrection(julianCentury);
            geomMeanLongSun = calculateGeomMeanLongSun(julianCentury);
            geomMeanAnomalySun = calculateGeomMeanAnomalySun(julianCentury);
        }

        let y = Math.tan(obliquityCorrection / 2.0);
        y *= y;
    
        const sin2l0 = Math.sin(2.0 * geomMeanLongSun);
        const sinm = Math.sin(geomMeanAnomalySun);
        const cos2l0 = Math.cos(2.0 * geomMeanLongSun);
        const sin4l0 = Math.sin(4.0 * geomMeanLongSun);
        const sin2m = Math.sin(2.0 * geomMeanAnomalySun);
    
        const Etime = y * sin2l0 -
            2.0 * eccentricityEarthOrbit * sinm +
            4.0 * eccentricityEarthOrbit * y * sinm * cos2l0 -
            0.5 * y * y * sin4l0 -
            1.25 * eccentricityEarthOrbit * eccentricityEarthOrbit * sin2m;
        return toDegrees(Etime) * 4.0;
    }

    function calculateHourAngleSunrise(latitudeRadians, sunDeclination) {
        const HAarg = (Math.cos(toRadians(90.833)) / (Math.cos(latitudeRadians) * Math.cos(sunDeclination)) - 
            Math.tan(latitudeRadians) * Math.tan(sunDeclination));
        if (HAarg < -1.0 || HAarg > 1.0) {
            return undefined;
        }
        return Math.acos(HAarg);
    }
    
    function calculateSolarTime(minutes, equationOfTime, longitude) {
        const solarTimeFix = equationOfTime + 4.0 * longitude;
        let trueSolarTime = minutes + solarTimeFix;
        trueSolarTime -= Math.floor(minutes / 1440.0) * 1440.0;
        return trueSolarTime;
    }
    
    function calculateAzimuthElevation(sunDeclination, trueSolarTime, latitudeRadians)
    {
        let hourAngle = trueSolarTime / 4.0 - 180.0;
        if (hourAngle < -180.0) {
            hourAngle += 360.0;
        }
        const haRad = toRadians(hourAngle);
        let csz = Math.sin(latitudeRadians) * Math.sin(sunDeclination) + 
            Math.cos(latitudeRadians) * Math.cos(sunDeclination) * Math.cos(haRad);
        if (csz > 1.0) {
            csz = 1.0;
        } else if (csz < -1.0) {
            csz = -1.0;
        }
        let zenith = Math.acos(csz);
        const azDenom = Math.cos(latitudeRadians) * Math.sin(zenith);
        let azimuth;
        if (Math.abs(azDenom) > 1E-6) {
            let azRad = ((Math.sin(latitudeRadians) * Math.cos(zenith)) - Math.sin(sunDeclination)) / azDenom;
            if (azRad > 1.0) {
                azRad = 1.0;
            } else if (azRad < -1.0) {
                azRad = -1.0;
            }
            azimuth = Math.PI - Math.acos(azRad);
            if (hourAngle > 0.0)
                azimuth = -azimuth;
        } else {
            if (latitudeRadians > 0.0) {
                azimuth = Math.PI;
            } else {
                azimuth = 0.0;
            }
        }
        if (azimuth < 0.0) {
            azimuth += Math.PI * 2;
        }
    
        azimuth = toDegrees(azimuth);
        zenith = toDegrees(zenith);
    
        const exoatmElevation = 90.0 - zenith;
    
        // Atmospheric Refraction correction
        let refractionCorrection;
        if (exoatmElevation > 85.0) {
            refractionCorrection = 0.0;
        } else {
            const te = Math.tan(toRadians(exoatmElevation));
            if (exoatmElevation > 5.0) {
                refractionCorrection = 58.1 / te - 0.07 / (te * te * te) + 0.000086 / (te * te * te * te * te);
            } else if (exoatmElevation > -0.575) {
                refractionCorrection = 1735.0 + exoatmElevation *
                    (-518.2 + exoatmElevation *
                        (103.4 + exoatmElevation *
                            (-12.79 + exoatmElevation * 0.711)));
            } else {
                refractionCorrection = -20.774 / te;
            }
            refractionCorrection = refractionCorrection / 3600.0;
        }
    
        const solarZenith = zenith - refractionCorrection;
    
        return {
            'azimuth': azimuth,
            'elevation': 90.0 - solarZenith,
        };
    }
    
    function calculateSolarNoon(julianDay, longitude) {
        const tnoon = toJulianCentury(julianDay - longitude / 360.0);
        let eqTime = calculateEquationOfTime(tnoon);
        const solNoonOffset = 720.0 - (longitude * 4) - eqTime;
        const newt = toJulianCentury(julianDay + solNoonOffset / 1440.0);
        eqTime = calculateEquationOfTime(newt);
        let solNoon = 720 - (longitude * 4) - eqTime;
        solNoon -= Math.floor(solNoon / 1440.0) * 1440.0;
        return solNoon;
    }
    
    function calculateSunRiseSetTime(equationOfTime, hourAngle, longitude) {
        const delta = longitude + toDegrees(hourAngle);
        return 720.0 - (4.0 * delta) - equationOfTime;
    }

    function julianDay(epoch_ms) {
        let date = new Date(Math.round(epoch_ms));
        date.setUTCMilliseconds(0);
        date.setUTCSeconds(0);
        date.setUTCMinutes(0);
        date.setUTCHours(0);
        let year = date.getUTCFullYear();
        let month = date.getUTCMonth() + 1;
        let day = date.getUTCDate();

        if (month > 2) {
            month -= 3;
        } else {
            year--;
            month += 9;
        }
        const c = Math.floor(year / 100);
        const ya = year - 100 * c;
        return Math.floor((146097 * c) / 4) + Math.floor((1461 * ya) / 4) +
            Math.floor((153 * month + 2) / 5) + day + 1721119;
    }


    class Position {
        constructor(solarTime) {
            if (!isFinite(solarTime._epoch_ms) ||
                    !isFinite(solarTime._longitude) ||
                    !isFinite(solarTime._longitude)) {
                this._azimuth = undefined;
                this._elevation = undefined;
                return;
            }

            const epoch = solarTime._epoch_ms / 1000;
            let julianDay = solarTime._julianDay;
            const secondsAfterMidnight = epoch - Math.floor(epoch / 86400.0) * 86400.0;
            julianDay += secondsAfterMidnight / 86400.0;
            const julianCentury = toJulianCentury(julianDay);

            const geomMeanLongSun = calculateGeomMeanLongSun(julianCentury);
            const geomMeanAnomalySun = calculateGeomMeanAnomalySun(julianCentury);
            const sunEqOfCenter = calculateSunEqOfCenter(julianCentury, geomMeanAnomalySun);
            const sunTrueLong = calculateSunTrueLong(geomMeanLongSun, sunEqOfCenter);
            const sunApparentLong = calculateSunApparentLong(julianCentury, sunTrueLong);
            const obliquityCorrection = calculateObliquityCorrection(julianCentury);
            const sunDeclination = calculateSunDeclination(obliquityCorrection, sunApparentLong);

            const eccentricityEarthOrbit = calculateEccentricityEarthOrbit(julianCentury);
            const equationOfTime = calculateEquationOfTime(eccentricityEarthOrbit, obliquityCorrection,
                geomMeanLongSun, geomMeanAnomalySun);

            const st = calculateSolarTime(secondsAfterMidnight / 60.0, equationOfTime, solarTime._longitude);

            const pos = calculateAzimuthElevation(sunDeclination, st, toRadians(solarTime._latitude));
            this._azimuth = pos.azimuth;
            this._elevation = pos.elevation;
        }

        isDark(angle) {
            if (typeof angle === 'undefined') {
                angle = 0.0;
            }
            if (!isFinite(this._elevation)) {
                return false;
            }
            if (!isFinite(angle)) {
                return false;
            }
            return this._elevation < -angle;
        }

        get dark() { return this.isDark(); }
        get azimuth() { return this._azimuth; }
        get elevation() { return this._elevation; }
    }

    class Day {
        constructor(solarTime) {
            if (!isFinite(solarTime._epoch_ms) ||
                    !isFinite(solarTime._longitude) ||
                    !isFinite(solarTime._longitude)) {
                this._julianDay = 0;
                this._longitude = undefined;
                this._timeOffset = undefined;
                this._equationOfTime = undefined;
                this._hourAngleSunrise = undefined;
                return;
            }
            this._longitude = solarTime._longitude;

            const epoch = solarTime._epoch_ms / 1000.0;
            this._timeOffset = Math.floor(epoch / 86400.0) * 86400.0;

            this._julianDay = solarTime._julianDay;
            const julianCentury = toJulianCentury(this._julianDay);

            const geomMeanLongSun = calculateGeomMeanLongSun(julianCentury);
            const geomMeanAnomalySun = calculateGeomMeanAnomalySun(julianCentury);
            const sunEqOfCenter = calculateSunEqOfCenter(julianCentury, geomMeanAnomalySun);
            const sunTrueLong = calculateSunTrueLong(geomMeanLongSun, sunEqOfCenter);
            const sunApparentLong = calculateSunApparentLong(julianCentury, sunTrueLong);
            const obliquityCorrection = calculateObliquityCorrection(julianCentury);
            const sunDeclination = calculateSunDeclination(obliquityCorrection, sunApparentLong);

            const eccentricityEarthOrbit = calculateEccentricityEarthOrbit(julianCentury);
            this._equationOfTime = calculateEquationOfTime(eccentricityEarthOrbit, obliquityCorrection,
                geomMeanLongSun, geomMeanAnomalySun);

            this._hourAngleSunrise = calculateHourAngleSunrise(toRadians(solarTime._latitude), sunDeclination);
        }

        get sunrise() {
            if (!isFinite(this._hourAngleSunrise)) {
                return undefined;
            }
            const minutes = calculateSunRiseSetTime(this._equationOfTime, this._hourAngleSunrise, this._longitude);
            return (minutes * 60.0 + this._timeOffset) * 1000.0;
        }

        get sunset() {
            if (!isFinite(this._hourAngleSunrise)) {
                return undefined;
            }
            const minutes = calculateSunRiseSetTime(this._equationOfTime, -this._hourAngleSunrise, this._longitude);
            return (minutes * 60.0 + this._timeOffset) * 1000.0;
        }

        get noon() {
            if (this._julianDay <= 0 || !isFinite(this._longitude)) {
                return undefined;
            }
            const minutes = calculateSolarNoon(this._julianDay, this._longitude);
            return (minutes * 60.0 + this._timeOffset) * 1000.0;
        }
    }

    Solar.Time = class {
        constructor(latitude, longitude, epoch_ms) {
            this._latitude = latitude;
            this._longitude = longitude;
            if (isFinite(epoch_ms)) {
                this._epoch_ms = epoch_ms;
                this._julianDay = julianDay(epoch_ms);
            }

            this._day = undefined;
            this._position = undefined;
        }

        setTime(epoch_ms) {
            if (!isFinite(epoch_ms)) {
                this._epoch_ms = undefined;
                this._julianDay = undefined;
            } else {
                this._epoch_ms = epoch_ms;
                this._julianDay = julianDay(epoch_ms);
            }
            this._day = undefined;
            this._position = undefined;
        }

        get day() {
            if (!this._day) {
                this._day = new Day(this);
            }
            return this._day;
        }

        get position() {
            if (!this._position) {
                this._position = new Position(this);
            }
            return this._position;
        }

        get noon() {
            if (!isFinite(this._epoch_ms) || !isFinite(this._longitude)) {
                return undefined;
            }

            const minutes = calculateSolarNoon(this._julianDay, this._longitude);
            const epoch = this._epoch_ms / 1000.0;
            return (minutes * 60.0 + Math.floor(epoch / 86400.0) * 86400.0) * 1000.0;
        }
    }
})();
