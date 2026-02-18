## Forge Processing for radiation
# Adding a site:
    1. Add site.py to forge/processing/station/{station} (copy one that exists).
    2. Add station codes to const.py
    3. Make changes to forge/vis/station/{station} mode.py & view.py
    4. Update systemmd scripts

## Notable Changes
- Surfrad stations will use GAW codes if one exists. In the future I would like to create aliases to codes SURFRAD uses.


## ToDo

# List of sites to add to vis/station
- Might need to add met to stations.

- [ ] Need to add historical edits (surfmod.dat) to scripts.
- Systemmd needs to be updated.
- update radiationfiles/tracker.py