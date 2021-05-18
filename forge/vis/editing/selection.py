import typing


class Selection:
    def __init__(self, display: str, patterns: typing.List[str]):
        self.display = display
        self.patterns = patterns


selections = [
    Selection("Scattering", [
        r'^Bb?s[BGRQ0-9]*_',
    ]),
    Selection("Absorption", [
        r'^Ba[BGRQ0-9]*_',
    ]),
    Selection("Counts", [
        r'^N[pbn]?[0-9]*_',
    ]),
    Selection("Scattering, Absorption, and Counts", [
        r'^Bb?s[BGRQ0-9]*_',
        r'^Ba[BGRQ0-9]*_',
        r'^N[pbn]?[0-9]*_',
    ]),
    Selection("Nephelometer P, T, and RH", [
        r'^[TUP][0-9]*u?_S[0-9]+$',
    ]),
    Selection("Concentration (EBC, Aethalometer)", [
        r'^X[BGRQ0-9]*c?_',
    ]),
    Selection("Wind speed and direction", [
        r'^W[SD][0-9]*_',
    ]),
    Selection("System flags", [
        r'^F1?_',
    ]),
]


class InstrumentSelection:
    def __init__(self, display: str, patterns: typing.List[str],
                 instrument: typing.Optional[str] = None,
                 require: typing.Optional[str] = None):
        self.display = display
        self.patterns = patterns
        self.instrument = instrument
        self.require = require


instrument_selections = [
    InstrumentSelection("Scattering", [
       r'^Bb?s[BGRQ0-9]*$'
    ]),
    InstrumentSelection("Absorption", [
       r'^Bac?[BGRQ0-9]*$'
    ]),
    InstrumentSelection("Counts", [
       r'^N[pbn]?[0-9]*$'
    ]),
    InstrumentSelection("Concentration", [
       r'^Xc?[BGRQ0-9]*$'
    ]),
    InstrumentSelection("Optical", [
       r'^Bb?[sae]?c?[BGRQ0-9]*',
       r'^Xc?[BGRQ0-9]*',
    ], instrument='^(S|A|E)'),
    InstrumentSelection("Conditions", [
       r'^(?:T|P|U)1?',
    ], require=r'^((P1?)|(T1?)|((Bb?[sae]?|X)[BGRQ0-9]*c?)){'),
]

