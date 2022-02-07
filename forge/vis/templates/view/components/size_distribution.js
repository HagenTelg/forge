var SizeDistribution = {};
(function() {

    // {{ '\n' }}{% include 'global/calculations/size_distribution.js' %}

    SizeDistribution.ConcentrationDispatch = class extends DataSocket.RecordDispatch {
        constructor(dataName, outputConcentrations) {
            super(dataName);

            this._outputConcentrations = new Map();
            if (outputConcentrations !== undefined) {
                outputConcentrations.forEach((size, fieldName) => {
                    if (typeof size === 'number') {
                        this._outputConcentrations.set(fieldName, [undefined, size]);
                    } else if (Array.isArray(size)) {
                        this._outputConcentrations.set(fieldName, size);
                    } else {
                        this._outputConcentrations.set(fieldName, [undefined, undefined]);
                    }
                });
            }

            this._diameters = [];
        }

        processRecord(record, epoch) {
            let Dp = record.get('Dp');
            if (!Dp) {
                Dp = [];
            }

            let dNdlogDp = record.get('dNdlogDp');
            if (!dNdlogDp) {
                dNdlogDp = [];
                record.set('dNdlogDp', dNdlogDp);
            }

            let dN = record.get('dN');
            if (!dN) {
                dN = [];
                record.set('dN', dN);
            }

            for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                const sizes = Dp[timeIndex];
                if (sizes && Array.isArray(sizes)) {
                    for (let binIndex=0; binIndex<sizes.length; binIndex++) {
                        const d = sizes[binIndex];
                        if (!isFinite(d)) {
                            continue;
                        }
                        this._diameters[binIndex] = d;
                    }
                }

                let dNdlogDpTime = dNdlogDp[timeIndex];
                if (!dNdlogDpTime) {
                    dNdlogDpTime = [];
                    dNdlogDp[timeIndex] = dNdlogDpTime;
                }
                let dNTime = dN[timeIndex];
                if (!dNTime) {
                    dNTime = [];
                    dN[timeIndex] = dNTime;
                }

                for (let binIndex=0; binIndex<this._diameters.length; binIndex++) {
                    const normalized = dNdlogDpTime[binIndex];
                    const denormalized = dNTime[binIndex];

                    if (!isFinite(denormalized)) {
                        if (!isFinite(normalized)) {
                            continue;
                        }
                        const t = denormalizationFactor(this._diameters, binIndex);
                        if (!isFinite(t) || t <= 0.0) {
                            continue;
                        }

                        dNTime[binIndex] = normalized * t;
                    } else if (!isFinite(normalized)) {
                        if (!isFinite(denormalized)) {
                            continue;
                        }
                        const t = denormalizationFactor(this._diameters, binIndex);
                        if (!isFinite(t) || t <= 0.0) {
                            continue;
                        }

                        dNdlogDpTime[binIndex] = denormalized / t;
                    }
                }

                this._outputConcentrations.forEach((fieldSizeRange, fieldName) => {
                    let outputValues = record.get(fieldName);
                    if (!outputValues) {
                        outputValues = [];
                        record.set(fieldName, outputValues);
                    }
                    if (isFinite(outputValues[timeIndex])) {
                        return;
                    }
                    
                    let concentration = undefined;
                    for (let binIndex=0; binIndex<this._diameters.length; binIndex++) {
                        const binConcentration = dNTime[binIndex];
                        if (!isFinite(binConcentration)) {
                            continue;
                        }

                        const diameter = this._diameters[binIndex];
                        const lowerSizeLimit = fieldSizeRange[0];
                        if (isFinite(lowerSizeLimit) && (!isFinite(diameter) || diameter < lowerSizeLimit)) {
                            continue;
                        }
                        const upperSizeLimit = fieldSizeRange[1];
                        if (isFinite(upperSizeLimit) && (!isFinite(diameter) || diameter > upperSizeLimit)) {
                            continue;
                        }
                        
                        if (concentration === undefined) {
                            concentration = binConcentration;
                        } else {
                            concentration += binConcentration;
                        }
                    }

                    if (concentration === undefined) {
                        concentration = Number.NaN;
                    }
                    outputValues[timeIndex] = concentration;
                });
            }
        }
    }
})();