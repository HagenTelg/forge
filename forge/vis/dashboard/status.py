import typing
import enum
from .details import Severity


class Status:
    class Email(enum.Enum):
        ALWAYS = "always"
        INFO = "info"
        WARNING = "warning"
        ERROR = "error"
        OFF = "off"

    def __init__(self, information: typing.Optional[Severity], email: "Status.Email"):
        self.information = information
        self.email = email

    def to_status(self) -> typing.Dict[str, typing.Any]:
        return {
            'information': self.information.value if self.information else 'none',
            'email': self.email.value,
        }
