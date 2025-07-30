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


@all_blank_core
@pytest.mark.parametrize("good, bad", [(420, "420")])
def test_abrt(core: AnyCore, good: int, bad: str) -> None:
    assert core.abrt is None
    core.abrt = good
    assert core.abrt == good
    with pytest.raises(TypeError):
        core.abrt = cast(int, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("spongebob", 3.14)])
def test_cells(core: AnyCore, good: str, bad: float) -> None:
    assert core.cells is None
    core.cells = good
    assert core.cells == good
    with pytest.raises(TypeError):
        core.cells = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("pickle rick", 3.14)])
def test_com(core: AnyCore, good: str, bad: float) -> None:
    assert core.com is None
    core.com = good
    assert core.com == good
    with pytest.raises(TypeError):
        core.com = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("juice malouse", 3.14)])
def test_exp(core: AnyCore, good: str, bad: float) -> None:
    assert core.exp is None
    core.exp = good
    assert core.exp == good
    with pytest.raises(TypeError):
        core.exp = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("forkbomb", 3.14)])
def test_fil(core: AnyCore, good: str, bad: float) -> None:
    assert core.fil is None
    core.fil = good
    assert core.fil == good
    with pytest.raises(TypeError):
        core.fil = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("utube university", 3.14)])
def test_inst(core: AnyCore, good: str, bad: float) -> None:
    assert core.inst is None
    core.inst = good
    assert core.inst == good
    with pytest.raises(TypeError):
        core.inst = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [(2112, "YYZ")])
def test_lost(core: AnyCore, good: int, bad: str) -> None:
    assert core.lost is None
    core.lost = good
    assert core.lost == good
    with pytest.raises(TypeError):
        core.lost = cast(int, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("pop == soda", 0.1)])
def test_op(core: AnyCore, good: str, bad: float) -> None:
    assert core.op is None
    core.op = good
    assert core.op == good
    with pytest.raises(TypeError):
        core.op = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("midnight rowhammer", 0.1)])
def test_proj(core: AnyCore, good: str, bad: float) -> None:
    assert core.proj is None
    core.proj = good
    assert core.proj == good
    with pytest.raises(TypeError):
        core.proj = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("cereal killer", 0.1)])
def test_smno(core: AnyCore, good: str, bad: float) -> None:
    assert core.smno is None
    core.smno = good
    assert core.smno == good
    with pytest.raises(TypeError):
        core.smno = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("dst", 0.1)])
def test_src(core: AnyCore, good: str, bad: float) -> None:
    assert core.src is None
    core.src = good
    assert core.src == good
    with pytest.raises(TypeError):
        core.src = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [("arch (btw)", 0.1)])
def test_sys(core: AnyCore, good: str, bad: float) -> None:
    assert core.sys is None
    core.sys = good
    assert core.sys == good
    with pytest.raises(TypeError):
        core.sys = cast(str, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [(time(23, 58), 0.1)])
def test_btim(core: AnyCore, good: time, bad: float) -> None:
    assert core.btim is None
    core.btim = good
    assert core.btim == good
    with pytest.raises(TypeError):
        core.btim = cast(time, bad)


@all_blank_core
@pytest.mark.parametrize("good, bad", [(time(23, 58), 0.1)])
def test_etim(core: AnyCore, good: time, bad: float) -> None:
    assert core.etim is None
    core.etim = good
    assert core.etim == good
    with pytest.raises(TypeError):
        core.etim = cast(time, bad)


@all_blank_core
@pytest.mark.parametrize(
    "good, bad", [(date(1974, 4, 1), "should have been a real joke")]
)
def test_date(core: AnyCore, good: date, bad: str) -> None:
    assert core.date is None
    core.date = good
    assert core.date == good
    with pytest.raises(TypeError):
        core.date = cast(date, bad)


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


@all_core
def test_filters(core: AnyCore) -> None:
    assert core.filters == [(1, None)]
    new = "bla"
    core.set_filters([new])
    assert core.filters == [(1, new)]
    with pytest.raises(TypeError):
        core.set_filters([cast(str, 42)])
