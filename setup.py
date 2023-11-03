#!/usr/bin/python3

import os
from setuptools import setup, find_packages
from forge import const

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as readme:
    LONG_DESCRIPTION = readme.read()

with open(os.path.join(here, 'requirements.txt')) as requirements_txt:
    REQUIRES = requirements_txt.read().splitlines()

setup(
    name='forge',
    version=const.__version__,
    license='GPL3',
    author='Derek Hageman',
    author_email='derek.hageman@noaa.gov',
    description="A data management and visualization system",
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIRES,
    tests_require=['pytest', 'requests', 'pytest-asyncio'],
    python_requires='>=3.6,<4.0',
    test_suite="tests",
    entry_points={"console_scripts": [
        "forge-vis = forge.vis.__main__:main",
        "forge-vis-access = forge.vis.access.cli.__main__:main",
        "forge-vis-export-control = forge.vis.export.controller.server:main",
        "forge-vis-realtime-control = forge.vis.realtime.controller.server:main",
        "forge-vis-acquisition-control = forge.vis.acquisition.controller.server:main",

        "forge-acquisition-uplink-local = forge.acquisition.uplink.local:main",
        "forge-acquisition-uplink-remote = forge.acquisition.uplink.remote:main",
        "forge-acquisition-uplink-incoming = forge.acquisition.uplink.incoming.__main__:main",
        "forge-acquisition-serial-multiplexer = forge.acquisition.serial.multiplexer.__main__:main",
        "forge-acquisition-serial-eavesdropper = forge.acquisition.serial.multiplexer.eavesdropper:main",
        "forge-acquisition-serial-control = forge.acquisition.serial.multiplexer.control:main",
        "forge-acquisition-console = forge.acquisition.console.__main__:main",
        "forge-acquisition-bus-server = forge.acquisition.bus.server.__main__:main",
        "forge-acquisition-event-log = forge.acquisition.eventlog.__main__:main",
        "forge-acquisition-instrument = forge.acquisition.instrument.run:main",
        "forge-acquisition-control = forge.acquisition.control.run:main",
        "forge-acquisition-systemd-startup = forge.acquisition.systemd.startup:main",

        "forge-processing-control = forge.processing.control.cli.__main__:main",
        "forge-processing-incoming = forge.processing.transfer.incoming.__main__:main",
        "forge-processing-storage-server = forge.processing.transfer.storage.server:main",
        "forge-processing-upload = forge.processing.transfer.upload:main",
        "forge-processing-download = forge.processing.transfer.download:main",
        "forge-processing-ingest-server = forge.processing.transfer.ingest.server:main",
        "forge-processing-ingest-notify = forge.processing.transfer.ingest.notify:main",
        "forge-processing-ingest-file = forge.processing.transfer.ingest.file:main",
        "forge-processing-ingest-receive = forge.processing.transfer.ingest.receive:main",

        "forge-telemetry = forge.telemetry.__main__:main",
        "forge-telemetry-control = forge.telemetry.cli.__main__:main",
        "forge-telemetry-oneshot = forge.telemetry.oneshot.__main__:main",
        "forge-telemetry-uplink = forge.telemetry.uplink.__main__:main",
        "forge-telemetry-tunnel-hub = forge.telemetry.tunnel.hub:main",
        "forge-telemetry-tunnel-remote = forge.telemetry.tunnel.remote:main",
        "forge-telemetry-tunnel-proxy = forge.telemetry.tunnel.proxy:main",

        "forge-dashboard = forge.dashboard.__main__:main",
        "forge-dashboard-control = forge.dashboard.cli.__main__:main",
        "forge-dashboard-report = forge.dashboard.report.__main__:main",

        "forge-archive-server = forge.archive.server.__main__:main",
        "forge-archive-server-diagnostics = forge.archive.server.diagnostics:main",
        "forge-archive-put = forge.archive.client.put:cli",
        "forge-archive-reindex = forge.archive.client.reindex:main",
        "forge-archive-edited-updater = forge.archive.update.edited:updater",
        "forge-archive-edited-flush = forge.archive.update.edited:flush",
        "forge-archive-clean-updater = forge.archive.update.clean:updater",
        "forge-archive-clean-flush = forge.archive.update.clean:flush",
        "forge-archive-avgh-updater = forge.archive.update.avgh:updater",
        "forge-archive-avgh-flush = forge.archive.update.avgh:flush",
        "forge-archive-avgd-updater = forge.archive.update.avgd:updater",
        "forge-archive-avgd-flush = forge.archive.update.avgd:flush",
        "forge-archive-avgm-updater = forge.archive.update.avgm:updater",
        "forge-archive-avgm-flush = forge.archive.update.avgm:flush",

        "forge-cpd3-cache-client = forge.cpd3.cache.client:main",
        "forge-cpd3-cache-server = forge.cpd3.cache.server:main",
        "forge-cpd3-pass-server = forge.cpd3.pass.server:main",
        "forge-cpd3-acquisition-incoming = forge.cpd3.acquisition.incoming.__main__:main",
        "forge-cpd3-acquisition-uplink = forge.cpd3.acquisition.incoming.uplink.__main__:main",
        "forge-cpd3-convert-acquisition = forge.cpd3.convert.acquisition:main",
    ], "gui_scripts": [
        "forge-acquisition-serial-setup = forge.acquisition.serial.setup.__main__:main",
    ]},
    packages=find_packages(exclude=["tests"]),
)
