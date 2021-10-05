# Forge Data System

This is the NOAA-GML "aerosol" data system.  It is the successor to "CPD3" and its "CPX3" subsystem.  Currently, only the visualization and interactive QA/QC (CPX3) component is implemented.  Instead of working as a local application with data made available through a synchronization process, it works as a web application [hosted on the NOAA-GML web server](https://gml.noaa.gov/aero/dataview/) or run locally with a direct attachment to the data source.

Access to data is restricted behind a request and approval system that requires an internal operator to manually grant access before any data for a station can be viewed by a user.  However, the GML hosted web server is configured to allow for unauthenticated access to a simple example that does not show real data.  This example can be viewed [here](https://gml.noaa.gov/aero/dataview/station/nil/example-basic).


## Getting Started

For local development or testing, running the system in a Python virtual environment is the simplest approach.  An example configuration is also provided to run a local web server for the basic example.  To get this ready, the following sequence of commands can be used:

```shell
git clone https://gitlab.com/derek.hageman/forge.git forge
cd forge
python3 -m venv venv
. venv/bin/activate
pip3 install -e .
cd forge/vis
cp example-settings.toml settings.toml
```

To start the local web server, run `forge-vis` with virtual environment created above.  To activate the virtual environment (e.g. in a new shell), source `venv/bin/activate` from within the repository directory ("forge" above).  So a full start sequence looks like:

```shell
cd forge
. venv/bin/activate
cd forge/vis
forge-vis
```

At this point, you should see a start-up log that looks something like:

```
INFO:     Started server process [105911]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

After which you can open a web browser and go to `http://127.0.0.1:8000/station/nil/example-basic`


## Station Plot Configuration

The configuration for station specific plots is stored in Python modules under `forge/vis/station/`.  The  general layout is that `mode.py` specifies the visible modes (e.g. "aerosol" or "ozone") as well as mapping plot keys to human-readable names.  From there `view.py` maps the plot keys into actual instances of the plots.  These instances are usually classes that generate the templated HTML that displays the plot.  However, in most cases simply using the normal base class to specify plots in terms of data records and variables mapping to traces is sufficient.  The data references by the plots is provided by `data.py` and the exported data files by `export.py`.  So in general those two need to be updated if the station is using unconventional data sources (e.g. non-standard instrument codes).

All stations derive from "default" in the station directory, so if one of the above override files is not found the corresponding default one is used.  In most cases, a station starts with the basic setup defined in the default and only changes specific parts of it.  This process usually means "detaching" the configuration then adding, removing, or replacing parts of the detached configuration.