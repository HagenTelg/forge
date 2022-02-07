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

        "forge-processing-control = forge.processing.control.cli.__main__:main",

        "forge-telemetry = forge.telemetry.__main__:main",
        "forge-telemetry-control = forge.telemetry.cli.__main__:main",
        "forge-telemetry-oneshot = forge.telemetry.oneshot.__main__:main",
        "forge-telemetry-uplink = forge.telemetry.uplink.__main__:main",
        "forge-telemetry-tunnel-hub = forge.telemetry.tunnel.hub:main",
        "forge-telemetry-tunnel-remote = forge.telemetry.tunnel.remote:main",
        "forge-telemetry-tunnel-proxy = forge.telemetry.tunnel.proxy:main",

        "forge-cpd3-cache-client = forge.cpd3.cache.client:main",
        "forge-cpd3-cache-server = forge.cpd3.cache.server:main",
        "forge-cpd3-pass-server = forge.cpd3.pass.server:main",
        "forge-cpd3-acquisition-incoming = forge.cpd3.acquisition.incoming.__main__:main",
        "forge-cpd3-acquisition-uplink = forge.cpd3.acquisition.incoming.uplink.__main__:main",
    ]},
    packages=find_packages(exclude=["tests"]),
    package_data={"": [
        "*.html", "*.txt", "*.csv", "*.json", "*.js", "*.css",
        "*.svg", "*.png",
        "*.eot", "*.ttf", "*.woff", "*.woff2"
    ]},
)
