import typing
from forge.acquisition.cutsize import CutSize
from .translation import RealtimeTranslator as BaseTranslator
from ..data.archive import RealtimeRecord, Record, RealtimeSelection


class Translator(BaseTranslator):
    def __init__(self, records: typing.Dict[str, Record]):
        self._records: typing.Dict[str, RealtimeRecord] = dict()
        for data_name, record in records.items():
            if not isinstance(record, RealtimeRecord):
                continue
            self._records[data_name] = record

    class Instrument(BaseTranslator.Instrument):
        class _FieldTranslator:
            def __init__(self, data_name: str, data_field: str, selection: RealtimeSelection):
                self.data_name = data_name
                self.data_field = data_field
                self.pre_index = selection.acquisition_index

            def process(self, value: typing.Any, output: "BaseTranslator.OutputInterface") -> None:
                if self.pre_index is not None:
                    try:
                        value = value[self.pre_index]
                    except (ValueError, KeyError, TypeError):
                        return
                output.send_field(self.data_name, self.data_field, value)

        def __init__(self, source: str, instrument_info: typing.Dict[str, typing.Any],
                     records: typing.Dict[str, RealtimeRecord]):
            super().__init__(source, instrument_info)
            self._all_records = records
            self._data_dispatch: typing.Dict[typing.Tuple[CutSize.Size, str], typing.List["Translator.Instrument._FieldTranslator"]] = dict()
            self._message_dispatch: typing.Dict[str, typing.List["Translator.Instrument._FieldTranslator"]] = dict()
            self._rebuild_dispatch()

        def _rebuild_dispatch(self) -> None:
            try:
                tags: typing.Set[str] = set([str(t) for t in self.info['tags']])
            except (ValueError, TypeError, KeyError):
                tags: typing.Set[str] = set()
            try:
                instrument_code: str = str(self.info['type'])
            except (ValueError, TypeError, KeyError):
                instrument_code: str = ""

            self._data_dispatch.clear()
            for data_name, record in self._all_records.items():
                for data_field, selections in record.fields.items():  # type: str, typing.List[RealtimeSelection]
                    for sel in selections:
                        if not sel.accept_realtime_source(self.source, tags, instrument_code):
                            continue
                        for cut_size in CutSize.Size:
                            if not sel.accept_realtime_data(cut_size.value):
                                continue
                            key = (cut_size, sel.acquisition_field)
                            target = self._data_dispatch.get(key)
                            if not target:
                                target = list()
                                self._data_dispatch[key] = target
                            target.append(self._FieldTranslator(data_name, data_field, sel))
                        if sel.accept_realtime_message():
                            target = self._message_dispatch.get(sel.acquisition_field)
                            if not target:
                                target = list()
                                self._message_dispatch[sel.acquisition_field] = target
                            target.append(self._FieldTranslator(data_name, data_field, sel))

        def update_information(self, instrument_info: typing.Dict[str, typing.Any]) -> None:
            super().update_information(instrument_info)
            self._rebuild_dispatch()

        def translate_data(self, cutsize: CutSize.Size,
                           values: typing.Dict[str, typing.Union[float, typing.List[float]]],
                           output: "BaseTranslator.OutputInterface") -> None:
            for field, value in values.items():
                targets = self._data_dispatch.get((cutsize, field))
                if not targets:
                    continue
                for t in targets:
                    t.process(value, output)

        def translate_message(self, record: str, message: typing.Any,
                              output: "BaseTranslator.OutputInterface") -> None:
            targets = self._message_dispatch.get(record)
            if not targets:
                return
            for t in targets:
                t.process(message, output)

    def instrument_translator(self, source: str, instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional["Translator.Instrument"]:
        return self.Instrument(source, instrument_info, self._records)
