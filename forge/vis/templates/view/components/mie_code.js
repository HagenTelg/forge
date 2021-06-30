var Mie = {};
(function() {
    class Complex {
        constructor(r, i) {
            if (r instanceof Complex) {
                this.r = r.r;
                this.i = r.i;
                return;
            }

            if (isFinite(r)) {
                this.r = r;
            } else {
                this.r = 0;
            }
            if (isFinite(i)) {
                this.i = i;
            } else {
                this.i = 0;
            }
        }
        clone() { return new Complex(this); }

        eadd(...args) {
            for (const v of args) {
                if (typeof v === 'number') {
                    this.r += v;
                } else {
                    this.r += v.r;
                    this.i += v.i;
                }
            }
            return this;
        }
        static add(...args) { return (new Complex(args[0])).eadd(...(args.slice(1))); }

        esub(...args) {
            for (const v of args) {
                if (typeof v === 'number') {
                    this.r -= v;
                } else {
                    this.r -= v.r;
                    this.i -= v.i;
                }
            }
            return this;
        }
        static sub(...args) { return (new Complex(args[0])).esub(...(args.slice(1))); }

        emult(...args) {
            for (const v of args) {
                if (typeof v === 'number') {
                    this.r *= v;
                    this.i *= v;
                } else {
                    const r = this.r * v.r - this.i * v.i;
                    const i = this.r * v.i + this.i * v.r;
                    this.r = r;
                    this.i = i;
                }
            }
            return this;
        }
        static mult(...args) { return (new Complex(args[0])).emult(...(args.slice(1))); }

        ediv(...args) {
            for (const v of args) {
                if (typeof v === 'number') {
                    this.r /= v;
                    this.i /= v;
                } else {
                    const r = this.r * v.r + this.i * v.i;
                    const i = this.i * v.r - this.r * v.i;
                    const d = v.r * v.r + v.i * v.i;
                    this.r = r / d;
                    this.i = i / d;
                }
            }
            return this;
        }
        static div(...args) { return (new Complex(args[0])).ediv(...(args.slice(1))); }

        abs() { return Math.sqrt(this.r * this.r + this.i * this.i); }
        mod() { return this.abs(); }
    }

    /* http://atol.ucsd.edu/scatlib/scatterlib.htm */
    class BHMie {
        constructor(ri, nAng) {
            if (!nAng) {
                nAng = 2;
            }
            if (!ri) {
                ri = new Complex(1.53, 0.001);
            }

            this.ri = ri;
            this.nAng = nAng;
            this.x = undefined;

            this.tau = []; this.tau.length = nAng;
            this.pi = []; this.pi.length = nAng;
            this.pi0 = []; this.pi0.length = nAng;
            this.pi1 = []; this.pi1.length = nAng;
            this.s1 = []; this.s1.length = nAng*2 - 1;
            this.s2 = []; this.s2.length = nAng*2 - 1;
            this.D = [];

            this.amu = [];
            for (let i=0; i<nAng; i++) {
                this.amu.push(Math.cos(i * (Math.PI / 2.0) / (nAng - 1)));
            }

            this._qsca = 0.0;
            this._gsca = 0.0;
        }

        _calculate(x) {
            if (this.x === x) {
                return;
            }
            this.x = x;

            const y = Complex.mult(x, this.ri);
            const nstop = Math.floor(x + 4.0 * Math.pow(x, 1 / 3.0) + 2.0);
            const nmx = Math.max(Math.floor(y.abs()), nstop) + 15;

            this.tau.fill(0.0);
            this.pi.fill(0.0);
            this.pi0.fill(0.0);
            this.pi1.fill(0.0);
            for (let i = 0; i < this.s1.length; i++) {
                this.s1[i] = new Complex(0, 0);
            }
            for (let i = 0; i < this.s2.length; i++) {
                this.s2[i] = new Complex(0, 0);
            }

            let psi0 = Math.cos(x);
            let psi1 = Math.sin(x);
            let chi0 = -psi1;
            let chi1 = psi0;
            let xi1 = new Complex(psi1, -chi1);

            this._qsca = 0.0;
            this._gsca = 0;
            let p = -1;

            let fn = 0;
            let psi = 0;
            let chi = 0;
            let xi = new Complex(0, 0);
            let an = new Complex(0, 0);
            let bn = new Complex(0, 0);
            let an1 = new Complex(0, 0);
            let bn1 = new Complex(0, 0);
            let anA = 0;
            let bnA = 0;
            let j;
            let k;
            let en;
            let en21
            let en2m1;

            this.D.length = nmx;
            this.D[nmx - 1] = new Complex(0.0, 0.0);
            for (let en = nmx; en > 1; en--) {
                an = Complex.div(en, y);
                this.D[en - 2] = Complex.sub(an, Complex.div(1.0, Complex.add(this.D[en - 1], an)));
            }
            for (let n = 0; n < nstop; n++) {
                en = n + 1;
                en21 = 2 * en + 1;
                en2m1 = 2 * en - 1;

                fn = en21 / (en * (en + 1));
                psi = en2m1 * psi1 / x - psi0;
                chi = en2m1 * chi1 / x - chi0;
                xi = new Complex(psi, -chi);

                if (n !== 0) {
                    an1 = an;
                    bn1 = bn;
                }
                an = Complex.div(this.D[n], this.ri)
                an.eadd(en / x);
                an.emult(psi);
                an.esub(psi1);
                an.ediv(Complex.div(this.D[n], this.ri).eadd(en / x).emult(xi).esub(xi1));

                bn = Complex.mult(this.ri, this.D[n]);
                bn.eadd(en / x);
                bn.emult(psi);
                bn.esub(psi1);
                bn.ediv(Complex.mult(this.ri, this.D[n]).eadd(en / x).emult(xi).esub(xi1));

                anA = an.abs();
                bnA = bn.abs();

                this._qsca += en21 * (anA * anA + bnA * bnA);
                this._gsca += fn * (an.r * bn.r + an.i * bn.i);

                if (n > 0) {
                    this._gsca += (((en - 1) * (en + 1)) / en) *
                        (an1.r * an.r + an1.i * an.i + bn1.r * bn.r + bn1.i * bn.i);
                }

                for (j = 0; j < this.nAng; j++) {
                    this.pi[j] = this.pi1[j];
                    this.tau[j] = en * this.amu[j] * this.pi[j] - (en + 1) * this.pi0[j];
                    this.s1[j].eadd(Complex.mult(an, this.pi[j]).eadd(Complex.mult(bn, this.tau[j])).emult(fn));
                    this.s2[j].eadd(Complex.mult(an, this.tau[j]).eadd(Complex.mult(bn, this.pi[j])).emult(fn));
                }

                p = -p;

                for (j = 0, k = this.nAng * 2 - 2; j < this.nAng - 1; j++, k--) {
                    this.s1[k].eadd(Complex.mult(an, this.pi[j]).esub(Complex.mult(bn, this.tau[j])).emult(fn * p));
                    this.s2[k].eadd(Complex.mult(bn * this.pi[j]).esub(Complex.mult(an * this.tau[j])).emult(fn * p));
                }

                psi0 = psi1;
                psi1 = psi;
                chi0 = chi1;
                chi1 = chi;
                xi1 = xi;

                for (j = 0; j < this.nAng; j++) {
                    this.pi1[j] = (en21 * this.amu[j] * this.pi[j] - (en + 1) * this.pi0[j]) / en;
                    this.pi0[j] = this.pi[j];
                }
            }
        }

        gsca(x) {
            this._calculate(x);
            return 2.0 * this._gsca / this._qsca;
        }
        qsca(x) {
            this._calculate(x);
            return (2.0 / (x * x)) * this._qsca;
        }
        qext(x) {
            this._calculate(x);
            return (4.0 / (x * x)) * this.s1[0].r
        }
        qbsc(x) {
            this._calculate(x);
            const qbsc = this.s1[2 * this.nAng - 2].abs();
            return (4.0 / (x * x)) * qbsc * qbsc;
        }
        qabs(x) {
            return this.qext(x) - this.qsca(x);
        }
    }


    function miePoint(diameter, wavelength) {
        return (Math.PI / wavelength * 1E3) * diameter;
    }
    function binScale(diameter, dN) {
        return diameter * diameter * dN * (Math.PI / 4.0);
    }

    class OpticalCalculator {
        constructor(mie) {
            this.mie = mie;
            this._cache = new Map();
        }

        _getCached(x, method) {
            let entry = this._cache.get(x);
            if (!entry) {
                entry = {};
                while (this._cache.size >= 1024) {
                    this._cache.delete(this._cache.keys().next().value);
                }
                this._cache.set(x, entry);
            }
            let result = entry[method];
            if (result === undefined) {
                result = this.mie[method](x);
                entry[method] = result;
            }
            return result;
        }

        _integrate(wavelength, Dp, dN, calculate) {
            if (!isFinite(wavelength) || wavelength <= 0.0) {
                return undefined;
            }

            let sum = undefined;
            for (let i=0; i<Dp.length; i++) {
                const diameter = Dp[i];
                if (!isFinite(diameter) || diameter <= 0.0) {
                    continue;
                }
                const x = miePoint(diameter, wavelength);

                if (!isFinite(dN[i]) || dN[i] < 0.0) {
                    continue;
                }
                const scale = binScale(diameter, dN[i]);

                const add = calculate(x, scale);
                if (sum === undefined) {
                    sum = add;
                } else {
                    sum += add;
                }
            }
            return sum;
        }

        Bs(wavelength, Dp, dN) {
            return this._integrate(wavelength, Dp, dN, (x, scale) => {
                return this._getCached(x, 'qsca') * scale;
            });
        }
        Bbs(wavelength, Dp, dN) {
            return this._integrate(wavelength, Dp, dN, (x, scale) => {
                return this._getCached(x, 'qbsc') * scale;
            });
        }
        Be(wavelength, Dp, dN) {
            return this._integrate(wavelength, Dp, dN, (x, scale) => {
                return this._getCached(x, 'qext') * scale;
            });
        }
        Ba(wavelength, Dp, dN) {
            return this._integrate(wavelength, Dp, dN, (x, scale) => {
                return this._getCached(x, 'qabs') * scale;
            });
        }
        G(wavelength, Dp, dN) {
            let G = undefined;
            const Bs = this._integrate(wavelength, Dp, dN, (x, scale) => {
                const add = this._getCached(x, 'gsca');
                if (G === undefined) {
                    G = add;
                } else {
                    G += add;
                }
                return this._getCached(x, 'qsca') * scale;
            });
            if (!isFinite(Bs) || Bs === 0.0) {
                return undefined;
            }
            if (!isFinite(G)) {
                return undefined;
            }
            return G / Bs;
        }
    }

    Mie.OpticalDispatch = class extends SizeDistribution.ConcentrationDispatch {
        constructor(dataName, wavelengths, outputConcentrations, r, i, nAng) {
            super(dataName, outputConcentrations);

            if (r === undefined) {
                r = 1.53;
            }
            if (i === undefined) {
                i = 0.001;
            }
            this._calculator = new OpticalCalculator(new BHMie(new Complex(r, i), nAng));

            this.wavelengths = wavelengths;

            this._activeDiameters = [];
        }

        processRecord(record, epoch) {
            super.processRecord(record, epoch);

            let Dp = record.get('Dp');
            if (!Dp) {
                Dp = [];
            }
            const dN = record.get('dN');
            if (!dN) {
                return;
            }

            for (let timeIndex=0; timeIndex<epoch.length; timeIndex++) {
                const sizes = Dp[timeIndex];
                if (sizes && Array.isArray(sizes)) {
                    for (let binIndex=0; binIndex<sizes.length; binIndex++) {
                        const d = sizes[binIndex];
                        if (!isFinite(d)) {
                            continue;
                        }
                        this._activeDiameters[binIndex] = d;
                    }
                }

                const dNTime = dN[timeIndex];
                if (!dNTime || !Array.isArray(dNTime)) {
                    continue;
                }

                function calculateField(fieldName, calculate) {
                    if (fieldName === undefined) {
                        return;
                    }

                    let fieldData = record.get(fieldName);
                    if (!fieldData) {
                        fieldData = [];
                        record.set(fieldName, fieldData);
                        for (let i=0; i<epoch.length; i++) {
                            fieldData.push(undefined);
                        }
                    }

                    const value = fieldData[timeIndex];
                    if (isFinite(value)) {
                        return;
                    }
                    fieldData[timeIndex] = calculate();
                }

                this.wavelengths.forEach((outputs, wavelength) => {
                    calculateField(outputs.Bs, () => {
                        return this._calculator.Bs(wavelength, this._activeDiameters, dNTime);
                    });
                    calculateField(outputs.Bbs, () => {
                        return this._calculator.Bbs(wavelength, this._activeDiameters, dNTime);
                    });
                    calculateField(outputs.Ba, () => {
                        return this._calculator.Ba(wavelength, this._activeDiameters, dNTime);
                    });
                    calculateField(outputs.Be, () => {
                        return this._calculator.Be(wavelength, this._activeDiameters, dNTime);
                    });
                    calculateField(outputs.G, () => {
                        return this._calculator.G(wavelength, this._activeDiameters, dNTime);
                    });
                });
            }
        }
    }
})();