import os.path
from forge.const import STATIONS, HELP_URL, __version__
from . import CONFIGURATION
from jinja2 import Environment, PackageLoader, select_autoescape

_ROOT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

TEMPLATE_ENV = Environment(
    loader=PackageLoader('forge.vis', 'templates'),
    autoescape=select_autoescape(['html', 'js', 'json']),
    enable_async=True,
)
TEMPLATE_ENV.globals['STATIONS'] = STATIONS
TEMPLATE_ENV.globals['VERSION'] = __version__
TEMPLATE_ENV.globals['HELP_URL'] = HELP_URL
TEMPLATE_ENV.globals['DEBUG'] = CONFIGURATION.as_bool('SERVER.DEBUG')
TEMPLATE_ENV.globals['OFFLINE'] = CONFIGURATION.as_bool('SERVER.OFFLINE')


def package_data(*parts):
    return os.path.join(_ROOT_DIRECTORY, *parts)


def package_template(*parts: str):
    return TEMPLATE_ENV.get_template(os.path.join(*parts))


def name_to_initials(name: str) -> str:
    initials = ""
    for w in name.split():
        if len(w) == 0:
            continue
        initials += w[0].upper()
    return initials
