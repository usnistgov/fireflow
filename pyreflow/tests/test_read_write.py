from typing import cast
from datetime import date, datetime, time

import pytest

from pyreflow.typing import (
    MeasIndex,
    Range,
    Trigger,
    Timestep,
    Shortname,
    AnyCoreTEXT,
    AnyCoreDataset,
    AnyCore,
    AnalysisBytes,
)
import pyreflow as pf
import polars as pl

from .conftest import lazy_fixture


@pytest.fixture
def blank_core_2_0() -> pf.CoreTEXT2_0:
    return pf.CoreTEXT2_0("L")


@pytest.fixture
def blank_core_3_0() -> pf.CoreTEXT3_0:
    return pf.CoreTEXT3_0("L")


@pytest.fixture
def blank_core_3_1() -> pf.CoreTEXT3_1:
    return pf.CoreTEXT3_1("L")


@pytest.fixture
def blank_core_3_2() -> pf.CoreTEXT3_2:
    return pf.CoreTEXT3_2("Moca Emporium")


@pytest.fixture
def blank_dataset_2_0(blank_core_2_0: pf.CoreTEXT2_0) -> pf.CoreDataset2_0:
    return blank_core_2_0.to_dataset([], AnalysisBytes(b""), [])


@pytest.fixture
def blank_dataset_3_0(blank_core_3_0: pf.CoreTEXT3_0) -> pf.CoreDataset3_0:
    return blank_core_3_0.to_dataset([], AnalysisBytes(b""), [])


@pytest.fixture
def blank_dataset_3_1(blank_core_3_1: pf.CoreTEXT3_1) -> pf.CoreDataset3_1:
    return blank_core_3_1.to_dataset([], AnalysisBytes(b""), [])


@pytest.fixture
def blank_dataset_3_2(blank_core_3_2: pf.CoreTEXT3_2) -> pf.CoreDataset3_2:
    return blank_core_3_2.to_dataset([], AnalysisBytes(b""), [])


@pytest.fixture
def blank_optical_2_0() -> pf.Optical2_0:
    return pf.Optical2_0()


@pytest.fixture
def blank_optical_3_0() -> pf.Optical3_0:
    return pf.Optical3_0(())


@pytest.fixture
def blank_optical_3_1() -> pf.Optical3_1:
    return pf.Optical3_1(())


@pytest.fixture
def blank_optical_3_2() -> pf.Optical3_2:
    return pf.Optical3_2(())


@pytest.fixture
def blank_temporal_2_0() -> pf.Temporal2_0:
    return pf.Temporal2_0()


@pytest.fixture
def blank_temporal_3_0() -> pf.Temporal3_0:
    return pf.Temporal3_0(Timestep(1.0))


@pytest.fixture
def blank_temporal_3_1() -> pf.Temporal3_1:
    return pf.Temporal3_1(Timestep(1.0))


@pytest.fixture
def blank_temporal_3_2() -> pf.Temporal3_2:
    return pf.Temporal3_2(Timestep(1.0))


LINK_NAME = Shortname("wubbalubbadubdub")


@pytest.fixture
def core_2_0(
    blank_core_2_0: pf.CoreTEXT2_0, blank_optical_2_0: pf.Optical2_0
) -> pf.CoreTEXT2_0:
    blank_core_2_0.push_optical(blank_optical_2_0, LINK_NAME, Range(9001))
    return blank_core_2_0


@pytest.fixture
def core_3_0(
    blank_core_3_0: pf.CoreTEXT3_0, blank_optical_3_0: pf.Optical3_0
) -> pf.CoreTEXT3_0:
    blank_core_3_0.push_optical(blank_optical_3_0, LINK_NAME, Range(9001))
    return blank_core_3_0


@pytest.fixture
def core_3_1(
    blank_core_3_1: pf.CoreTEXT3_1, blank_optical_3_1: pf.Optical3_1
) -> pf.CoreTEXT3_1:
    blank_core_3_1.push_optical(blank_optical_3_1, LINK_NAME, Range(9001))
    return blank_core_3_1


@pytest.fixture
def core_3_2(
    blank_core_3_2: pf.CoreTEXT3_2, blank_optical_3_2: pf.Optical3_2
) -> pf.CoreTEXT3_2:
    blank_core_3_2.push_optical(blank_optical_3_2, LINK_NAME, Range(9001))
    return blank_core_3_2


@pytest.fixture
def dataset_2_0(
    blank_dataset_2_0: pf.CoreDataset2_0, blank_optical_2_0: pf.Optical2_0
) -> pf.CoreDataset2_0:
    ser = pl.Series("blub", [1, 2, 3], dtype=pl.UInt64)
    blank_dataset_2_0.push_optical(blank_optical_2_0, ser, LINK_NAME, Range(9001))
    return blank_dataset_2_0


@pytest.fixture
def dataset_3_0(
    blank_dataset_3_0: pf.CoreDataset3_0, blank_optical_3_0: pf.Optical3_0
) -> pf.CoreDataset3_0:
    ser = pl.Series("blub", [1, 2, 3], dtype=pl.UInt64)
    blank_dataset_3_0.push_optical(blank_optical_3_0, ser, LINK_NAME, Range(9001))
    return blank_dataset_3_0


@pytest.fixture
def dataset_3_1(
    blank_dataset_3_1: pf.CoreDataset3_1, blank_optical_3_1: pf.Optical3_1
) -> pf.CoreDataset3_1:
    ser = pl.Series("blub", [1, 2, 3], dtype=pl.UInt64)
    blank_dataset_3_1.push_optical(blank_optical_3_1, ser, LINK_NAME, Range(9001))
    return blank_dataset_3_1


@pytest.fixture
def dataset_3_2(
    blank_dataset_3_2: pf.CoreDataset3_2, blank_optical_3_2: pf.Optical3_2
) -> pf.CoreDataset3_2:
    ser = pl.Series("blub", [1, 2, 3], dtype=pl.UInt64)
    blank_dataset_3_2.push_optical(blank_optical_3_2, ser, LINK_NAME, Range(9001))
    return blank_dataset_3_2


all_blank_core = pytest.mark.parametrize(
    "core",
    [
        lazy_fixture("blank_core_2_0"),
        lazy_fixture("blank_core_3_0"),
        lazy_fixture("blank_core_3_1"),
        lazy_fixture("blank_core_3_2"),
        lazy_fixture("blank_dataset_2_0"),
        lazy_fixture("blank_dataset_3_0"),
        lazy_fixture("blank_dataset_3_1"),
        lazy_fixture("blank_dataset_3_2"),
    ],
)

all_core = pytest.mark.parametrize(
    "core",
    [
        lazy_fixture("core_2_0"),
        lazy_fixture("core_3_0"),
        lazy_fixture("core_3_1"),
        lazy_fixture("core_3_2"),
        lazy_fixture("dataset_2_0"),
        lazy_fixture("dataset_3_0"),
        lazy_fixture("dataset_3_1"),
        lazy_fixture("dataset_3_2"),
    ],
)


# all of these attributes should be either None or a positive integer
@all_blank_core
@pytest.mark.parametrize("attr", ["abrt", "lost"])
def test_core_metaroot_opt_int(attr: str, core: AnyCore) -> None:
    good = 420
    assert getattr(core, attr) is None
    setattr(core, attr, good)
    assert getattr(core, attr) == good
    with pytest.raises(TypeError):
        setattr(core, attr, "420")
    with pytest.raises(TypeError):
        setattr(core, attr, -420)


# all of these attributes should be either None or a string
@all_blank_core
@pytest.mark.parametrize(
    "attr", ["cells", "com", "exp", "fil", "inst", "op", "proj", "smno", "src", "sys"]
)
def test_core_metaroot_opt_str(attr: str, core: AnyCore) -> None:
    good = "spongebob"
    assert getattr(core, attr) is None
    setattr(core, attr, good)
    assert getattr(core, attr) == good
    with pytest.raises(TypeError):
        setattr(core, attr, 3.14)


# these should be time objects
@all_blank_core
@pytest.mark.parametrize("attr", ["btim", "etim"])
def test_time(attr: str, core: AnyCore) -> None:
    good = time(23, 58)
    assert getattr(core, attr) is None
    setattr(core, attr, good)
    assert getattr(core, attr) == good
    with pytest.raises(TypeError):
        setattr(core, attr, "thermonuclear war")


@all_blank_core
def test_date(core: AnyCore) -> None:
    good = date(1991, 8, 25)
    assert core.date is None
    core.date = good
    assert core.date == good
    with pytest.raises(TypeError):
        core.date = cast(date, "Apr 1, 1976")


@all_core
def test_trigger(core: AnyCore) -> None:
    assert core.trigger is None
    tr = (LINK_NAME, 0)
    core.trigger = tr
    assert core.trigger == tr


@all_blank_core
def test_trigger_bad(core: AnyCore) -> None:
    with pytest.raises(TypeError):
        core.trigger = cast(Trigger, "over,9000")


@all_blank_core
def test_trigger_nolink(core: AnyCore) -> None:
    with pytest.raises(pf.PyreflowException):
        core.trigger = (Shortname("harold"), 0)


@all_core
def test_shortnames(core: AnyCore) -> None:
    assert core.all_shortnames == [LINK_NAME]
    new_name = Shortname("I can haz IP")
    core.all_shortnames = [new_name]
    assert core.all_shortnames == [new_name]
    with pytest.raises(ValueError):
        core.all_shortnames = [Shortname("I,can,haz,IP")]


@all_core
def test_longnames(core: AnyCore) -> None:
    assert core.longnames == [None]
    new_name = "I can haz IP"
    core.longnames = [new_name]
    assert core.longnames == [new_name]
    with pytest.raises(TypeError):
        core.longnames = [cast(str, 42)]


# each of these should be strings or None
@all_core
@pytest.mark.parametrize(
    "get, set",
    [(x, f"set_{x}") for x in ["filters", "percents_emitted", "detector_types"]],
)
def test_core_meas_opt_strs(get: str, set: str, core: AnyCore) -> None:
    assert getattr(core, get) == [(1, None)]
    new = "bla"
    getattr(core, set)([new])
    assert getattr(core, get) == [(1, new)]
    with pytest.raises(TypeError):
        getattr(core, set)([42])


# each of these should be a non-negative float
@all_core
@pytest.mark.parametrize(
    "get, set",
    [(x, f"set_{x}") for x in ["powers", "detector_voltages"]],
)
def test_core_meas_opt_floats(get: str, set: str, core: AnyCore) -> None:
    assert getattr(core, get) == [(1, None)]
    new = 0.5
    getattr(core, set)([new])
    assert getattr(core, get) == [(1, new)]
    newer = 0.0
    getattr(core, set)([newer])
    assert getattr(core, get) == [(1, newer)]
    with pytest.raises(TypeError):
        getattr(core, set)([-1.0])
    with pytest.raises(TypeError):
        getattr(core, set)(["pickle rick"])
