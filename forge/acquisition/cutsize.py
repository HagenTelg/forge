import typing
from enum import Enum
from . import LayeredConfiguration
from .schedule import Schedule


class CutSize(Schedule):
    class Size(Enum):
        PM1 = 1.0
        PM2_5 = 2.5
        PM10 = 10.0
        WHOLE = None

        @staticmethod
        def parse(data: typing.Any) -> "CutSize.Size":
            try:
                return CutSize.Size(data)
            except (ValueError, TypeError):
                pass
            if isinstance(data, bool) and not data:
                return CutSize.Size.WHOLE
            if data == 25:
                return CutSize.Size.PM2_5
            if data == 0 or data == 10:
                return CutSize.Size.PM10
            if data == 1:
                return CutSize.Size.PM1
            try:
                s = str(data).upper()
                if s == "PM1":
                    return CutSize.Size.PM1
                elif s == "PM25" or s == "PM2.5" or s == "PM2_5":
                    return CutSize.Size.PM2_5
                elif s == "PM10":
                    return CutSize.Size.PM10
                elif s == "NONE" or s == "WHOLE":
                    return CutSize.Size.WHOLE
            except (ValueError, TypeError):
                pass
            raise ValueError("invalid cut size")

        def __str__(self) -> str:
            if self == CutSize.Size.PM1:
                return "PM1"
            elif self == CutSize.Size.PM2_5:
                return "PM25"
            elif self == CutSize.Size.PM10:
                return "PM10"
            elif self == CutSize.Size.WHOLE:
                return "WHOLE"

        def __repr__(self) -> str:
            return f"Size({str(self)})"

    class Active(Schedule.Active):
        def __init__(self, config: LayeredConfiguration):
            super().__init__(config)

            if isinstance(config, LayeredConfiguration):
                constant_config = config.constant()
                if constant_config is not None:
                    self.size = CutSize.Size.parse(constant_config)
                else:
                    self.size = CutSize.Size.parse(config.get("SIZE"))
            elif isinstance(config, dict):
                self.size = CutSize.Size.parse(config.get("SIZE"))
            else:
                self.size = CutSize.Size.parse(config)

        def __repr__(self) -> str:
            return f"CutSize.Active({self.describe_offset()}={str(self.size)})"

    def __init__(self, config: typing.Optional[typing.Union[LayeredConfiguration, str, float, bool]],
                 single_entry: bool = False):
        if not isinstance(config, LayeredConfiguration):
            single_entry = True
        elif not config:
            single_entry = True
        elif config.constant(False):
            single_entry = True
        super().__init__(config, single_entry=single_entry)
        self.constant_size = (len(self.active) == 1)

    def next(self, now: float = None) -> typing.Optional["CutSize.Active"]:
        if self.constant_size:
            return None
        return super().next(now)

    async def automatic_activation(self, now: float = None) -> None:
        if self.constant_size:
            await self.current().automatic_activation(now)
            return
        await super().automatic_activation()
