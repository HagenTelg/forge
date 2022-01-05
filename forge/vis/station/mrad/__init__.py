class Site:
    def __init__(self, instrument_code: str, radiation_code: str, name: str, include_spn1: bool = False):
        self.instrument_code = instrument_code
        self.radiation_code = radiation_code
        self.name = name
        self.include_spn1 = include_spn1
