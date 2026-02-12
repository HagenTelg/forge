## Forge Processing for radiation
# Adding a site:
    1. Add site.py to forge/processing/station/{station} (copy one that exists).
    2. Add station codes to const.py
    3. Make changes to forge/vis/station/{station} mode.py & view.py

## Notable Changes
- Surfrad stations will use GAW codes if one exists. In the future I would like to create aliases to codes SURFRAD uses.


## ToDo
- [ ] Add SURFRAD stations that overlap with aeronet
- [ ] Add standalone SURFRAD stations
- [ ] Add SOLRAD