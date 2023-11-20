import pytest
from netCDF4 import Dataset, Variable
from forge.processing.context.selection import InstrumentSelection, VariableSelection


def test_instrument_selection(tmp_path):
    file = Dataset(str(tmp_path / "file.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"
    file.instrument = "testint"
    file.forge_tags = "tag1 tag2"

    assert InstrumentSelection.matcher({"instrument_id": "X1"})(file)
    assert not InstrumentSelection.matcher({"instrument_id": "X2"})(file)
    assert InstrumentSelection.matcher({"instrument_id": r"X\d"})(file)
    assert not InstrumentSelection.matcher({"instrument_id": r"X\d{2}"})(file)

    assert InstrumentSelection.matcher({"instrument": "testint"})(file)
    assert not InstrumentSelection.matcher({"instrument": "notinst"})(file)

    assert InstrumentSelection.matcher({"tags": "tag1"})(file)
    assert InstrumentSelection.matcher({"tags": "tag2"})(file)
    assert InstrumentSelection.matcher({"tags": "tag1 tag2"})(file)
    assert not InstrumentSelection.matcher({"tags": "tag1 tag2 tag3"})(file)
    assert InstrumentSelection.matcher({"tags": "+tag1 -tag3"})(file)
    assert not InstrumentSelection.matcher({"tags": "-tag1 tag2"})(file)

    assert InstrumentSelection.matcher({"require_tags": "tag1"})(file)
    assert InstrumentSelection.matcher({"require_tags": "tag1 tag2"})(file)
    assert not InstrumentSelection.matcher({"require_tags": "tag3"})(file)

    assert InstrumentSelection.matcher({"exclude_tags": "tag3 tag4"})(file)
    assert not InstrumentSelection.matcher({"exclude_tags": "tag3 tag1"})(file)

    assert InstrumentSelection.matcher({
        "tags": "tag1",
        "instrument_id": "X1",
    })(file)
    assert not InstrumentSelection.matcher({
        "tags": "tag3",
        "instrument_id": "X1",
    })(file)
    assert not InstrumentSelection.matcher({
        "tags": "tag1",
        "instrument_id": "X2",
    })(file)


def test_variable_selection(tmp_path):
    file = Dataset(str(tmp_path / "file.nc"), 'w', format='NETCDF4')
    var: Variable = file.createVariable("testvar", 'f8', ())
    var.variable_id = "F1"
    var.standard_name = "system_flags"
    var.units = "nm"

    assert VariableSelection.matcher({"variable_id": "F1"})(var)
    assert not VariableSelection.matcher({"variable_id": "F2"})(var)
    assert VariableSelection.matcher({"variable_id": r"F\d"})(var)
    assert not VariableSelection.matcher({"variable_id": r"F\d{2}"})(var)

    assert VariableSelection.matcher({"variable_name": "testvar"})(var)
    assert not VariableSelection.matcher({"variable_name": "notvar"})(var)

    assert VariableSelection.matcher({"standard_name": "system_flags"})(var)
    assert not VariableSelection.matcher({"standard_name": "instrument_flags"})(var)

    assert VariableSelection.matcher({"units": "nm"})(var)
    assert not VariableSelection.matcher({"units": "%"})(var)

    assert VariableSelection.matcher({
        "standard_name": "system_flags",
        "variable_name": "testvar",
    })(var)
    assert not VariableSelection.matcher({
        "standard_name": "instrument_flags",
        "variable_name": "testvar",
    })(var)
    assert not VariableSelection.matcher({
        "standard_name": "system_flags",
        "variable_name": "notvar",
    })(var)
