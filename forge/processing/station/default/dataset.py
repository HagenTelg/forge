import typing


def feature_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return "point"
    if tags and 'edits' in tags:
        return "point"
    if tags and 'passed' in tags:
        return "point"
    return "timeSeries"


def source(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "surface observation"


def title(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return "Acquisition System Event Log"
    if tags and 'edits' in tags:
        return "QA/QC Mentor Edits"
    if tags and 'passed' in tags:
        return "Data Pass Record"
    if tags and 'ozone' in tags:
        return "Surface Ozone Measurements"
    if tags and 'met' in tags:
        return "Meteorological Measurements"
    return "Aerosol Extensive Properties"


def keywords(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    keywords = [
        "GCMD:DOC/NOAA/ESRL/GMD",
    ]
    if tags and 'ozone' in tags:
        keywords.append("DOC/NOAA/OAR/ESRL/GMD/OZWV")
        keywords.append("GCMD:TROPOSPHERIC OZONE")
        keywords.append("GCMD:SPECTROPHOTOMETERS")
        keywords.append("GCMD:TROPOSPHERE,GCMD:Ultraviolet Ozone Spectrometer")
        keywords.append("GCMD:OZONE MONITORS")
        keywords.append("GCMD:OZONE")
        keywords.append("GCMD:AIR QUALITY")
    elif tags and 'met' in tags:
        keywords.append("GCMD:WIND DIRECTION")
        keywords.append("GCMD:WIND SPEED")
        keywords.append("GCMD:AIR TEMPERATURE")
        keywords.append("GCMD:RELATIVE HUMIDITY")
        keywords.append("GCMD:DEW POINT TEMPERATURE")
        keywords.append("GCMD:LIQUID SURFACE PRECIPITATION RATE")
    elif tags and 'eventlog' in tags:
        pass
    elif tags and 'edits' in tags:
        pass
    elif tags and 'passed' in tags:
        pass
    else:
        keywords.append("GCMD:AEROSOL MONITOR")
        if tags and 'scattering' in tags:
            keywords.append("GCMD:NEPHELOMETERS")
            keywords.append("GCMD:SCATTERING")
            keywords.append("GCMD:AEROSOL FORWARD SCATTER")
            keywords.append("GCMD:AEROSOL BACKSCATTER")
        if tags and ('cpc' in tags or 'cn' in tags or 'cnc' in tags):
            keywords.append("GCMD:CNC")
        if tags and 'absorption' in tags:
            keywords.append("GCMD:AEROSOL ABSORPTION")
    return ",".join(keywords)


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return None
    if tags and 'edits' in tags:
        return None
    if tags and 'passed' in tags:
        return None
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return "10.7289/V57P8WBF"
    if tags and ('met' in tags and 'aerosol' not in tags):
        return None
    if tags and ('radiation' in tags and 'aerosol' not in tags):
        return None
    return None


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def summary(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return "This dataset represents a log of events that occurred on the acquisition system."
    elif tags and 'edits' in tags:
        return "This dataset contains logical edits made to the data by the station mentor in the process of " \
               "performing QA/QC to produce the final data for analysis."
    elif tags and 'passed' in tags:
        return "This dataset represents a record of when QA/QC was completed on data and it was marked as ready " \
               "for analysis."
    if tags and 'ozone' in tags:
        return "This data set contains continuous UV Photometric data of surface level ozone collected and processed " \
               "by the National Oceanic and Atmospheric Administration, Global Monitoring Division."
    if tags and 'met' in tags:
        return "This dataset represents a range of time of continuous meteorological measurements.  The exact " \
               "contents depends on instrument availability but include wind speed and direction, ambient " \
               "temperature, ambient relative humidity, ambient pressure and/or precipitation rate."
    return "This dataset represents a range of time of continuous aerosol optical and/or microphysical measurements.  "\
           "The exact contents depends on instrument availability but include aerosol light scattering coefficients, "\
           "aerosol light absorption coefficients, and/or particle number concentration."


def references(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return None
    if tags and 'edits' in tags:
        return None
    if tags and 'passed' in tags:
        return None
    if tags and 'ozone' in tags:
        return """WMO/GAW Report No. 209, Guidelines for Continuous Measurements of Ozone in the Troposphere, 2013, available at https://www.wmo.int/pages/prog/arep/gaw/documents/Final_GAW_209_web.pdf
Oltmans, S. J. and Lefohn, A. S. and Scheel, H. E. and Harris, J. M. and Levy, H. and Galbally, I. E. and Brunke, E.-G. and Meyer, C. P. and Lathrop, J. A. and Johnson, B. J. and Shadwick, D. S. and Cuevas, E. and Schmidlin, F. J. and Tarasick, D. W. and Claude, H. and Kerr, J. B. and Uchino, O. and Mohnen, V., Trends of ozone in the troposphere, in Geophysical Research Letters 1998, Issue 1944-8007, Vol 25, Num 2, pg 139-142, http://dx.doi.org/10.1029/97GL03505, available at http://onlinelibrary.wiley.com/doi/10.1029/97GL03505/abstract"""
    if tags and ('met' in tags and 'aerosol' not in tags):
        return """Mefford, T.K., M. Bieniulis, B. Halter, and J. Peterson, Meteorological Measurements, in CMDL Summary Report 1994 - 1995, No. 23, 1996, pg. 17.
Herbert, G., M. Bieniulis, T. Mefford, and K. Thaut, Acquisition and Data Management Division, in CMDL Summary Report 1993, No. 22, 1994, pg. 57
Herbert, G.A., J. Harris, M. Bieniulis, and J. McCutcheon, Acquisition and Data Management, in CMDL Summary Report 1989, No. 18, 1990, Pg. 50.
Herbert, G.A., E.R. Green, G.L. Koenig, and K.W. Thaut, Monitoring instrumentation for the continuous measurement and quality assurance of meteorological observations, NOAA Tech. Memo. ERL ARL-148, 44 pp, 1986.
Herbert, G.A., E.R. Green, J.M. Harris, G.L. Koenig, S.J. Roughton, and K.W. Thaut, Control and Monitoring Instrumentation for the Continuous Measurement of Atmospheric CO2 and Meteorological Variables, J. of Atmos. and Oceanic Tech., 3, 414-421, 1986."""
    return """Anderson, T. L., et al., J. Atmos. Oceanic Tech. 13, 967-986, 1996
Anderson, T. L. and Ogren, J. A., Aerosol Sci. Technol., 29, 57-69, 1998
Bond, T. C., et al., Aerosol Sci. Technol., 30, 582-600, doi 10.1080/027868299304435, 1999
Ogren, J. A., Aerosol Sci. Technol., 44, 589-591, doi:10.1080/02786826.2010.482111, 2010
WMO/GAW Report No. 227, Aerosol Measurement Procedures, Guidelines and Recommendations, 2nd Edition, 2016."""


def acknowledgement(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Request acknowledgement details from data creator"


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "325 Broadway\nBoulder, CO, USA, 80305"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "group"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return "Ozone and Water Vapor (OZWV)"
    if tags and ('met' in tags and 'aerosol' not in tags and 'radiation' not in tags):
        return "GML Observatory Operations (OBOP)"
    return "Global Radiation and Aerosols Division (GRAD)"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return "gml.ozwv@noaa.gov"
    if tags and ('met' in tags and 'aerosol' not in tags and 'radiation' not in tags):
        return "gmd.met@noaa.gov"
    return "gml.grad@noaa.gov"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "National Oceanic and Atmospheric Administration/Global Monitoring Laboratory (NOAA/GML)"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return "https://gml.noaa.gov/ozwv/"
    if tags and ('met' in tags and 'aerosol' not in tags and 'radiation' not in tags):
        return "https://gml.noaa.gov/obop/"
    return "https://gml.noaa.gov/grad/"

