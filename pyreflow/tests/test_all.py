import numpy as np
import inspect as ins
from typing import cast, Any, NamedTuple
from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from copy import deepcopy

import pytest

from pyreflow.typing import (
    Trigger,
    MixedType,
    Datatype,
    AnyCoreTEXT,
    AnyCoreDataset,
    AnyOptical,
    AnyCore,
    AnyMeas,
    AppliedGates2_0,
    AppliedGates3_0,
    AppliedGates3_2,
    ByteOrd,
)
import pyreflow as pf
import pyreflow.pydantic as pfp
import polars as pl

from .conftest import lazy_fixture

import ast

LINK_NAME1 = "wubbalubbadubdub"
LINK_NAME2 = "maple lattes"
LINK_NAME3 = "silent man"

# used for testing the pydantic model against the types in the pyi file
with open("python/pyreflow/_pyreflow.pyi") as f:
    tree = ast.parse(f.read())


@pytest.fixture
def blank_gated_meas() -> pf.GatedMeasurement:
    return pf.GatedMeasurement()


@pytest.fixture
def blank_text_2_0() -> pf.CoreTEXT2_0:
    return pf.CoreTEXT2_0([], pf.OrderedUint32Layout([]))


@pytest.fixture
def blank_text_3_0() -> pf.CoreTEXT3_0:
    return pf.CoreTEXT3_0([], pf.OrderedUint32Layout([]))


@pytest.fixture
def blank_text_3_1() -> pf.CoreTEXT3_1:
    return pf.CoreTEXT3_1([], pf.EndianUintLayout([]))


@pytest.fixture
def blank_text_3_2() -> pf.CoreTEXT3_2:
    return pf.CoreTEXT3_2([], pf.EndianUintLayout([]), "Moca Emporium")


@pytest.fixture
def blank_dataset_2_0(blank_text_2_0: pf.CoreTEXT2_0) -> pf.CoreDataset2_0:
    return blank_text_2_0.to_dataset(pl.DataFrame())


@pytest.fixture
def blank_dataset_3_0(blank_text_3_0: pf.CoreTEXT3_0) -> pf.CoreDataset3_0:
    return blank_text_3_0.to_dataset(pl.DataFrame())


@pytest.fixture
def blank_dataset_3_1(blank_text_3_1: pf.CoreTEXT3_1) -> pf.CoreDataset3_1:
    return blank_text_3_1.to_dataset(pl.DataFrame())


@pytest.fixture
def blank_dataset_3_2(blank_text_3_2: pf.CoreTEXT3_2) -> pf.CoreDataset3_2:
    return blank_text_3_2.to_dataset(pl.DataFrame())


@pytest.fixture
def blank_optical_2_0() -> pf.Optical2_0:
    return pf.Optical2_0()


@pytest.fixture
def blank_optical_3_0() -> pf.Optical3_0:
    return pf.Optical3_0(1.0)


@pytest.fixture
def blank_optical_3_1() -> pf.Optical3_1:
    return pf.Optical3_1(1.0)


@pytest.fixture
def blank_optical_3_2() -> pf.Optical3_2:
    return pf.Optical3_2(1.0)


@pytest.fixture
def blank_temporal_2_0() -> pf.Temporal2_0:
    return pf.Temporal2_0()


@pytest.fixture
def blank_temporal_3_0() -> pf.Temporal3_0:
    return pf.Temporal3_0(1.0)


@pytest.fixture
def blank_temporal_3_1() -> pf.Temporal3_1:
    return pf.Temporal3_1(1.0)


@pytest.fixture
def blank_temporal_3_2() -> pf.Temporal3_2:
    return pf.Temporal3_2(1.0)


@pytest.fixture
def series1() -> pl.Series:
    return pl.Series("blub", [1, 2, 3], dtype=pl.UInt32)


@pytest.fixture
def series2() -> pl.Series:
    return pl.Series("C--", [1, 2, 3], dtype=pl.UInt32)


@pytest.fixture
def series3() -> pl.Series:
    return pl.Series("arnoldC", [1, 2, 3], dtype=pl.UInt32)


@pytest.fixture
def text_2_0(
    blank_text_2_0: pf.CoreTEXT2_0, blank_optical_2_0: pf.Optical2_0
) -> pf.CoreTEXT2_0:
    blank_text_2_0.push_optical(LINK_NAME1, blank_optical_2_0, 9001)
    return blank_text_2_0


@pytest.fixture
def text_3_0(
    blank_text_3_0: pf.CoreTEXT3_0, blank_optical_3_0: pf.Optical3_0
) -> pf.CoreTEXT3_0:
    blank_text_3_0.push_optical(LINK_NAME1, blank_optical_3_0, 9001)
    return blank_text_3_0


@pytest.fixture
def text_3_1(
    blank_text_3_1: pf.CoreTEXT3_1, blank_optical_3_1: pf.Optical3_1
) -> pf.CoreTEXT3_1:
    blank_text_3_1.push_optical(LINK_NAME1, blank_optical_3_1, 9001)
    return blank_text_3_1


@pytest.fixture
def text_3_2(
    blank_text_3_2: pf.CoreTEXT3_2, blank_optical_3_2: pf.Optical3_2
) -> pf.CoreTEXT3_2:
    blank_text_3_2.push_optical(LINK_NAME1, blank_optical_3_2, 9001)
    return blank_text_3_2


@pytest.fixture
def dataset_2_0(
    blank_dataset_2_0: pf.CoreDataset2_0,
    blank_optical_2_0: pf.Optical2_0,
    series1: pl.Series,
) -> pf.CoreDataset2_0:
    blank_dataset_2_0.push_optical(LINK_NAME1, blank_optical_2_0, series1, 9001)
    return blank_dataset_2_0


@pytest.fixture
def dataset_3_0(
    blank_dataset_3_0: pf.CoreDataset3_0,
    blank_optical_3_0: pf.Optical3_0,
    series1: pl.Series,
) -> pf.CoreDataset3_0:
    blank_dataset_3_0.push_optical(LINK_NAME1, blank_optical_3_0, series1, 9001)
    return blank_dataset_3_0


@pytest.fixture
def dataset_3_1(
    blank_dataset_3_1: pf.CoreDataset3_1,
    blank_optical_3_1: pf.Optical3_1,
    series1: pl.Series,
) -> pf.CoreDataset3_1:
    blank_dataset_3_1.push_optical(LINK_NAME1, blank_optical_3_1, series1, 9001)
    return blank_dataset_3_1


@pytest.fixture
def dataset_3_2(
    blank_dataset_3_2: pf.CoreDataset3_2,
    blank_optical_3_2: pf.Optical3_2,
    series1: pl.Series,
) -> pf.CoreDataset3_2:
    blank_dataset_3_2.push_optical(LINK_NAME1, blank_optical_3_2, series1, 9001)
    return blank_dataset_3_2


@pytest.fixture
def text2_2_0(
    text_2_0: pf.CoreTEXT2_0, blank_temporal_2_0: pf.Temporal2_0
) -> pf.CoreTEXT2_0:
    text_2_0.push_temporal(LINK_NAME2, blank_temporal_2_0, 9002)
    return text_2_0


@pytest.fixture
def text2_3_0(
    text_3_0: pf.CoreTEXT3_0, blank_temporal_3_0: pf.Temporal3_0
) -> pf.CoreTEXT3_0:
    text_3_0.push_temporal(LINK_NAME2, blank_temporal_3_0, 9002)
    return text_3_0


@pytest.fixture
def text2_3_1(
    text_3_1: pf.CoreTEXT3_1, blank_temporal_3_1: pf.Temporal3_1
) -> pf.CoreTEXT3_1:
    text_3_1.push_temporal(LINK_NAME2, blank_temporal_3_1, 9002)
    return text_3_1


@pytest.fixture
def text2_3_2(
    text_3_2: pf.CoreTEXT3_2, blank_temporal_3_2: pf.Temporal3_2
) -> pf.CoreTEXT3_2:
    text_3_2.push_temporal(LINK_NAME2, blank_temporal_3_2, 9002)
    return text_3_2


@pytest.fixture
def dataset2_2_0(
    dataset_2_0: pf.CoreDataset2_0,
    blank_temporal_2_0: pf.Temporal2_0,
    series2: pl.Series,
) -> pf.CoreDataset2_0:
    dataset_2_0.push_temporal(LINK_NAME2, blank_temporal_2_0, series2, 9002)
    return dataset_2_0


@pytest.fixture
def dataset2_3_0(
    dataset_3_0: pf.CoreDataset3_0,
    blank_temporal_3_0: pf.Temporal3_0,
    series2: pl.Series,
) -> pf.CoreDataset3_0:
    dataset_3_0.push_temporal(LINK_NAME2, blank_temporal_3_0, series2, 9002)
    return dataset_3_0


@pytest.fixture
def dataset2_3_1(
    dataset_3_1: pf.CoreDataset3_1,
    blank_temporal_3_1: pf.Temporal3_1,
    series2: pl.Series,
) -> pf.CoreDataset3_1:
    dataset_3_1.push_temporal(LINK_NAME2, blank_temporal_3_1, series2, 9002)
    return dataset_3_1


@pytest.fixture
def dataset2_3_2(
    dataset_3_2: pf.CoreDataset3_2,
    blank_temporal_3_2: pf.Temporal3_2,
    series2: pl.Series,
) -> pf.CoreDataset3_2:
    dataset_3_2.push_temporal(LINK_NAME2, blank_temporal_3_2, series2, 9002)
    return dataset_3_2


@pytest.fixture
def text3_2_0(
    text2_2_0: pf.CoreTEXT2_0, blank_optical_2_0: pf.Optical2_0
) -> pf.CoreTEXT2_0:
    text2_2_0.push_optical(LINK_NAME3, blank_optical_2_0, 9003)
    return text2_2_0


@pytest.fixture
def text3_3_0(
    text2_3_0: pf.CoreTEXT3_0, blank_optical_3_0: pf.Optical3_0
) -> pf.CoreTEXT3_0:
    text2_3_0.push_optical(LINK_NAME3, blank_optical_3_0, 9003)
    return text2_3_0


@pytest.fixture
def text3_3_1(
    text2_3_1: pf.CoreTEXT3_1, blank_optical_3_1: pf.Optical3_1
) -> pf.CoreTEXT3_1:
    text2_3_1.push_optical(LINK_NAME3, blank_optical_3_1, 9003)
    return text2_3_1


@pytest.fixture
def text3_3_2(
    text2_3_2: pf.CoreTEXT3_2, blank_optical_3_2: pf.Optical3_2
) -> pf.CoreTEXT3_2:
    text2_3_2.push_optical(LINK_NAME3, blank_optical_3_2, 9003)
    return text2_3_2


@pytest.fixture
def dataset3_2_0(
    dataset2_2_0: pf.CoreDataset2_0,
    blank_optical_2_0: pf.Optical2_0,
    series3: pl.Series,
) -> pf.CoreDataset2_0:
    dataset2_2_0.push_optical(LINK_NAME3, blank_optical_2_0, series3, 9003)
    return dataset2_2_0


@pytest.fixture
def dataset3_3_0(
    dataset2_3_0: pf.CoreDataset3_0,
    blank_optical_3_0: pf.Optical3_0,
    series3: pl.Series,
) -> pf.CoreDataset3_0:
    dataset2_3_0.push_optical(LINK_NAME3, blank_optical_3_0, series3, 9003)
    return dataset2_3_0


@pytest.fixture
def dataset3_3_1(
    dataset2_3_1: pf.CoreDataset3_1,
    blank_optical_3_1: pf.Optical3_1,
    series3: pl.Series,
) -> pf.CoreDataset3_1:
    dataset2_3_1.push_optical(LINK_NAME3, blank_optical_3_1, series3, 9003)
    return dataset2_3_1


@pytest.fixture
def dataset3_3_2(
    dataset2_3_2: pf.CoreDataset3_2,
    blank_optical_3_2: pf.Optical3_2,
    series3: pl.Series,
) -> pf.CoreDataset3_2:
    dataset2_3_2.push_optical(LINK_NAME3, blank_optical_3_2, series3, 9003)
    return dataset2_3_2


def parameterize_versions(
    arg: str, versions: list[str], targets: list[str]
) -> pytest.MarkDecorator:
    return pytest.mark.parametrize(
        arg,
        [lazy_fixture(f"{t}_{v}") for v in versions for t in targets],
    )


all_blank_core = parameterize_versions(
    "core",
    ["2_0", "3_0", "3_1", "3_2"],
    ["blank_text", "blank_dataset"],
)


all_core = parameterize_versions(
    "core",
    ["2_0", "3_0", "3_1", "3_2"],
    ["text", "dataset"],
)

all_core2 = parameterize_versions(
    "core",
    ["2_0", "3_0", "3_1", "3_2"],
    ["text2", "dataset2"],
)

all_core3 = parameterize_versions(
    "core",
    ["2_0", "3_0", "3_1", "3_2"],
    ["text3", "dataset3"],
)

all_blank_optical = parameterize_versions(
    "meas",
    ["2_0", "3_0", "3_1", "3_2"],
    ["blank_optical"],
)

all_blank_temporal = parameterize_versions(
    "meas",
    ["2_0", "3_0", "3_1", "3_2"],
    ["blank_temporal"],
)

all_blank_meas = parameterize_versions(
    "meas",
    ["2_0", "3_0", "3_1", "3_2"],
    ["blank_temporal", "blank_optical"],
)


class TestCore:
    # all of these attributes should be either None or a positive integer
    @all_blank_core
    @pytest.mark.parametrize("attr", ["abrt", "lost"])
    def test_metaroot_opt_int(self, attr: str, core: AnyCore) -> None:
        good = 420
        assert getattr(core, attr) is None
        setattr(core, attr, good)
        assert getattr(core, attr) == good
        with pytest.raises(TypeError):
            setattr(core, attr, "420")
        with pytest.raises(OverflowError):
            setattr(core, attr, -420)

    # all of these attributes should be either None or a string
    @all_blank_core
    @pytest.mark.parametrize(
        "attr",
        ["cells", "com", "exp", "fil", "inst", "op", "proj", "smno", "src", "sys"],
    )
    def test_metaroot_opt_str(self, attr: str, core: AnyCore) -> None:
        good = "spongebob"
        assert getattr(core, attr) == ""
        setattr(core, attr, good)
        assert getattr(core, attr) == good
        with pytest.raises(TypeError):
            setattr(core, attr, 3.14)

    # these should be time objects
    @all_blank_core
    @pytest.mark.parametrize("attr", ["btim", "etim"])
    def test_time(self, attr: str, core: AnyCore) -> None:
        good = time(23, 58)
        assert getattr(core, attr) is None
        setattr(core, attr, good)
        assert getattr(core, attr) == good
        with pytest.raises(TypeError):
            setattr(core, attr, "thermonuclear war")

    @pytest.mark.parametrize(
        "core0",
        [lazy_fixture(f"blank_{t}_2_0") for t in ["text", "dataset"]],
    )
    def test_imprecise_time_2_0(
        self, core0: pf.CoreTEXT2_0 | pf.CoreDataset2_0
    ) -> None:
        # these timestamps should be "the same" because 2.0 doesn't have sub-seconds
        t0 = time(23, 58, 0, 0)
        t1 = time(23, 58, 0, 1)
        # and this should be different
        t2 = time(23, 58, 1, 1)
        core1 = deepcopy(core0)
        assert core0 == core1
        core0.btim = t0
        core1.btim = t1
        assert core0 == core1
        core1.btim = t2
        assert core0 != core1

    @pytest.mark.parametrize(
        "core0",
        [lazy_fixture(f"blank_{t}_3_0") for t in ["text", "dataset"]],
    )
    def test_imprecise_time_3_0(
        self, core0: pf.CoreTEXT3_0 | pf.CoreDataset3_0
    ) -> None:
        # these timestamps should be "the same" because 3.0 is only precise up to 1/60 seconds
        t0 = time(23, 58, 0, 17000)
        t1 = time(23, 58, 0, 18000)
        # and this one should be different
        t2 = time(23, 58, 0, 340000)
        core1 = deepcopy(core0)
        assert core0 == core1
        core0.btim = t0
        core1.btim = t1
        assert core0 == core1
        core1.btim = t2
        assert core0 != core1

    @pytest.mark.parametrize(
        "core0",
        [
            lazy_fixture(f"blank_{t}_{v}")
            for t in ["text", "dataset"]
            for v in ["3_1", "3_2"]
        ],
    )
    def test_imprecise_time_3_1(
        self,
        core0: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        # these timestamps should be "the same" because 3.1 is only precise up to centiseconds
        t0 = time(23, 58, 0, 0)
        t1 = time(23, 58, 0, 1)
        # and this one should be different
        t2 = time(23, 58, 0, 10000)
        core1 = deepcopy(core0)
        assert core0 == core1
        core0.btim = t0
        core1.btim = t1
        assert core0 == core1
        core1.btim = t2
        assert core0 != core1

    @all_blank_core
    def test_date(self, core: AnyCore) -> None:
        good = date(1991, 8, 25)
        assert core.date is None
        core.date = good
        assert core.date == good
        with pytest.raises(TypeError):
            core.date = cast(date, "Apr 1, 1976")

    @all_core
    def test_trigger(self, core: AnyCore) -> None:
        assert core.tr is None
        tr = (LINK_NAME1, 0)
        core.tr = tr
        assert core.tr == tr

    @all_core
    def test_trigger_threshold(self, core: AnyCore) -> None:
        tr = (LINK_NAME1, 0)
        core.tr = tr
        assert core.tr == tr
        core.set_trigger_threshold(1)
        assert core.tr == (LINK_NAME1, 1)

    @all_blank_core
    def test_trigger_bad(self, core: AnyCore) -> None:
        with pytest.raises(TypeError):
            core.tr = cast(Trigger, "over,9000")

    @all_blank_core
    def test_trigger_nolink(self, core: AnyCore) -> None:
        with pytest.raises(pf.RelationalError):
            core.tr = ("harold", 0)

    @all_blank_core
    def test_trigger_temporal(self, core: AnyCore) -> None:
        with pytest.raises(pf.RelationalError):
            core.tr = (LINK_NAME2, 0)

    @all_core
    def test_par(self, core: AnyCore) -> None:
        assert core.par == 1

    @all_core2
    def test_shortnames(self, core: AnyCore) -> None:
        assert core.all_shortnames == [LINK_NAME1, LINK_NAME2]
        new_names = ["I can haz IP", "=Coffee"]
        core.all_shortnames = new_names
        assert core.all_shortnames == new_names
        with pytest.raises(pf.ParseKeywordValueError):
            core.all_shortnames = ["I,can,haz,IP", "=,=,=,=Coffee"]

    @parameterize_versions("core", ["2_0", "3_0", "3_1"], ["text2", "dataset2"])
    @pytest.mark.parametrize("attr", ["all_peak_bins", "all_peak_sizes"])
    def test_peak(
        self,
        core: pf.CoreTEXT2_0
        | pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreDataset2_0
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1,
        attr: str,
    ) -> None:
        assert getattr(core, attr) == [None, None]
        setattr(core, attr, [1, 2])
        assert getattr(core, attr) == [1, 2]
        with pytest.raises(TypeError):
            setattr(core, attr, [6.9, 4.20])

    @parameterize_versions("core", ["2_0", "3_0"], ["text2", "dataset2"])
    def test_shortnames_maybe(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.all_shortnames_maybe == [LINK_NAME1, LINK_NAME2]
        core.all_shortnames_maybe = [None, LINK_NAME2]
        assert core.all_shortnames_maybe == [None, LINK_NAME2]
        with pytest.raises(pf.PyreflowError):
            core.all_shortnames_maybe = [None, None]

    @all_core
    def test_longnames(self, core: AnyCore) -> None:
        assert core.all_longnames == [""]
        new_name = "I can haz IP"
        core.all_longnames = [new_name]
        assert core.all_longnames == [new_name]
        with pytest.raises(TypeError):
            core.all_longnames = [cast(str, 42)]

    # TODO make more tests to ensure keywords are dumped properly
    @parameterize_versions("core", ["2_0"], ["text", "dataset"])
    def test_standard_keywords_2_0(self, core: AnyCore) -> None:
        # TODO make these default
        kws = core.standard_keywords("both", "both")
        expected = {
            "$BYTEORD": "1,2,3,4",
            "$DATATYPE": "I",
            "$MODE": "L",
            "$PAR": "1",
            "$P1B": "32",
            "$P1N": LINK_NAME1,
            "$P1R": "9001",
        }
        for k, v in expected.items():
            assert k in kws
            assert kws[k] == v
        for k, v in kws.items():
            assert k in expected
            assert expected[k] == v

    @parameterize_versions("core", ["3_0"], ["text", "dataset"])
    def test_standard_keywords_3_0(self, core: AnyCore) -> None:
        kws = core.standard_keywords("both", "both")
        expected = {
            "$BYTEORD": "1,2,3,4",
            "$DATATYPE": "I",
            "$MODE": "L",
            "$PAR": "1",
            "$P1B": "32",
            "$P1N": LINK_NAME1,
            "$P1R": "9001",
            "$P1E": "0,0",
        }
        for k, v in expected.items():
            assert k in kws
            assert kws[k] == v
        for k, v in kws.items():
            assert k in expected
            assert expected[k] == v

    @parameterize_versions("core", ["3_1"], ["text", "dataset"])
    def test_standard_keywords_3_1(self, core: AnyCore) -> None:
        kws = core.standard_keywords("both", "both")
        expected = {
            "$BYTEORD": "1,2,3,4",
            "$DATATYPE": "I",
            "$MODE": "L",
            "$PAR": "1",
            "$P1B": "16",
            "$P1N": LINK_NAME1,
            "$P1R": "9001",
            "$P1E": "0,0",
        }
        for k, v in expected.items():
            assert k in kws
            assert kws[k] == v
        for k, v in kws.items():
            assert k in expected
            assert expected[k] == v

    @parameterize_versions("core", ["3_2"], ["text", "dataset"])
    def test_standard_keywords_3_2(self, core: AnyCore) -> None:
        kws = core.standard_keywords("both", "both")
        expected = {
            "$BYTEORD": "1,2,3,4",
            "$DATATYPE": "I",
            "$PAR": "1",
            "$CYT": "Moca Emporium",
            "$P1B": "16",
            "$P1N": LINK_NAME1,
            "$P1R": "9001",
            "$P1E": "0,0",
        }
        for k, v in expected.items():
            assert k in kws
            assert kws[k] == v
        for k, v in kws.items():
            assert k in expected
            assert expected[k] == v

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["text2", "dataset2"])
    def test_timestep(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.timestep == 1.0
        core.set_timestep(2.0)
        assert core.timestep == 2.0

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    @pytest.mark.parametrize(
        "attr,value,default",
        [
            ("originality", "Original", None),
            ("last_modified", datetime(2112, 1, 1, 0, 0), None),
            ("last_modifier", "you, obviously", ""),
            ("platename", "juice malouse", ""),
            ("plateid", "666", ""),
            ("wellid", "9.75", ""),
        ],
    )
    def test_modified_plate(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
        attr: str,
        value: Any,
        default: Any,
    ) -> None:
        assert getattr(core, attr) == default
        setattr(core, attr, value)
        assert getattr(core, attr) == value
        with pytest.raises(TypeError):
            setattr(core, attr, 1.61)

    @parameterize_versions("core", ["2_0", "3_0"], ["text3", "dataset3"])
    def test_comp(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.comp is None
        new = np.array(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float32
        )
        core.comp = new
        assert core.comp is not None and np.array_equal(core.comp, new)
        core.comp = None
        assert core.comp is None

    @parameterize_versions("core", ["2_0", "3_0"], ["text3", "dataset3"])
    def test_comp_not_par_low(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.comp is None
        with pytest.raises(pf.RelationalError):
            core.comp = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)

    @parameterize_versions("core", ["2_0", "3_0"], ["text3", "dataset3"])
    def test_comp_not_par_low_high(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.comp is None
        with pytest.raises(pf.RelationalError):
            core.comp = np.array(
                [
                    [1.0, 0.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0, 0.0],
                    [0.0, 0.0, 1.0, 0.0],
                    [0.0, 0.0, 0.0, 1.0],
                ],
                dtype=np.float32,
            )

    @parameterize_versions("core", ["2_0", "3_0"], ["text3", "dataset3"])
    def test_comp_toosmall(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        with pytest.raises(pf.InvalidKeywordValueError):
            core.comp = np.array([[1.0], [0.0]], dtype=np.float32)

    @parameterize_versions("core", ["2_0", "3_0"], ["text3", "dataset3"])
    def test_comp_nonsquare(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        with pytest.raises(pf.InvalidKeywordValueError):
            core.comp = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], dtype=np.float32)

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_spillover(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.spillover is None
        new = (
            [LINK_NAME1, LINK_NAME3],
            np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
        )
        core.spillover = new
        ms, arr = core.spillover
        assert ms == new[0]
        assert np.array_equal(arr, new[1])
        core.spillover = None
        assert core.spillover is None

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_spillover_toosmall(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.raises(pf.InvalidKeywordValueError):
            core.spillover = (
                [LINK_NAME1],
                np.array([[1.0]], dtype=np.float32),
            )

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_spillover_nonsquare(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.raises(pf.InvalidKeywordValueError):
            core.spillover = (
                [LINK_NAME1],
                np.array([[1.0, 0.0], [1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            )

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_spillover_temporal(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.RaisesGroup(pf.RelationalError):
            core.spillover = (
                [LINK_NAME1, LINK_NAME2],
                np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            )

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_spillover_nolink(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.RaisesGroup(pf.RelationalError):
            core.spillover = (
                [LINK_NAME1, "010011110100110101000111-010101110101010001001000"],
                np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            )

    @parameterize_versions("core", ["3_0"], ["text2", "dataset2"])
    def test_unicode(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.unicode is None
        # the actual contents arent' checked, presumably because nobody really
        # cares about this
        new = (666, ["$$$$"])
        core.unicode = new
        assert core.unicode == new
        with pytest.raises(TypeError):
            core.unicode = "latin_minus_20"  # type: ignore

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_vol(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.vol is None
        core.vol = 0.0
        assert core.vol == 0.0
        core.vol = 1.0
        assert core.vol == 1.0
        with pytest.raises(pf.InvalidKeywordValueError):
            core.vol = -1.0

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["text2", "dataset2"])
    def test_cytsn(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.cytsn == ""
        new = "12345"
        core.cytsn = new
        assert core.cytsn == new
        with pytest.raises(TypeError):
            core.cytsn = cast(str, 0.0)

    @parameterize_versions("core", ["2_0", "3_0", "3_1"], ["text2", "dataset2"])
    def test_mode(
        self,
        core: pf.CoreTEXT2_0
        | pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreDataset2_0
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1,
    ) -> None:
        assert core.mode == "L"
        core.mode = "U"
        assert core.mode == "U"
        with pytest.raises(pf.ParseKeywordValueError):
            core.mode = "fart"  # type: ignore

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_mode3_2(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        assert core.mode is None
        core.mode = "L"
        assert core.mode == "L"
        with pytest.raises(pf.ParseKeywordValueError):
            core.mode = "bear"  # type: ignore

    @parameterize_versions("core", ["2_0", "3_0", "3_1"], ["text2", "dataset2"])
    def test_cyt(
        self,
        core: pf.CoreTEXT2_0
        | pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreDataset2_0
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1,
    ) -> None:
        assert core.cyt == ""
        core.cyt = "meat grinder"
        assert core.cyt == "meat grinder"

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_cyt3_2(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        new = "meat grinder"
        core.cyt = new
        assert core.cyt == new
        with pytest.raises(TypeError):
            core.cyt = cast(str, None)
        with pytest.raises(pf.ParseKeywordValueError):
            core.cyt = ""

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    @pytest.mark.parametrize(
        "attr, good, bad, default",
        [
            ("flowrate", "plaid", 0.5, ""),
            ("unstainedinfo", "(redacted)", 1.61, ""),
            ("carriertype", "pigeon", -39, ""),
            ("carrierid", "bloodwing", 0xDEADBEEF, ""),
            ("locationid", "0", 3, ""),
            (
                "begindatetime",
                datetime(2112, 1, 1, tzinfo=timezone(timedelta(hours=-5))),
                "root",
                None,
            ),
            (
                "enddatetime",
                datetime(2112, 1, 2, tzinfo=timezone(timedelta(hours=-5))),
                "octave",
                None,
            ),
        ],
    )
    def test_metaroot_3_2_opt(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
        attr: str,
        good: Any,
        bad: Any,
        default: Any,
    ) -> None:
        assert getattr(core, attr) == default
        setattr(core, attr, good)
        assert getattr(core, attr) == good
        with pytest.raises(TypeError):
            setattr(core, attr, bad)

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_unstained_centers(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        assert core.unstainedcenters == {}
        core.unstainedcenters = {LINK_NAME1: 42}
        assert core.unstainedcenters == {LINK_NAME1: 42}
        core.unstainedcenters = {}
        assert core.unstainedcenters == {}

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_unstained_centers_temporal(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.RaisesGroup(pf.RelationalError):
            core.unstainedcenters = {LINK_NAME2: 42}

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_unstained_centers_nolink(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        with pytest.RaisesGroup(pf.RelationalError):
            core.unstainedcenters = {"barking pimpernel": 420}

    @parameterize_versions("core", ["2_0"], ["text2", "dataset2"])
    def test_applied_gates_2_0(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreDataset2_0,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion2_0(0, (0.0, 1.0))
        ag: AppliedGates2_0 = ([blank_gated_meas], {0: ur}, "NOT R1")
        core.applied_gates = ag

    @parameterize_versions("core", ["2_0"], ["text2", "dataset2"])
    def test_applied_gates_2_0_overrange(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreDataset2_0,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        # index 1 does not exist
        ur = pf.UnivariateRegion2_0(1, (0.0, 1.0))
        ag: AppliedGates2_0 = ([blank_gated_meas], {0: ur}, "NOT R1")
        with pytest.raises(pf.RelationalError):
            core.applied_gates = ag

    @parameterize_versions("core", ["2_0"], ["text2", "dataset2"])
    def test_applied_gates_2_0_bad_gating(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreDataset2_0,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion2_0(0, (0.0, 1.0))
        # R2 does not exist
        ag: AppliedGates2_0 = ([blank_gated_meas], {0: ur}, "NOT R2")
        with pytest.raises(pf.RelationalError):
            core.applied_gates = ag

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_applied_gates_3_0(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion3_0("P1", (0.0, 1.0))
        ag: AppliedGates3_0 = ([], {0: ur}, "NOT R1")
        core.applied_gates = ag

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_applied_gates_3_0_bad_meas_link(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        with pytest.RaisesGroup(pf.RelationalError):
            # P3 does not point to anything
            ur_bad = pf.UnivariateRegion3_0("P3", (0.0, 1.0))
            ag_bad = cast(AppliedGates3_0, ([], {0: ur_bad}, None))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_applied_gates_3_0_bad_gate_link(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        with pytest.raises(pf.RelationalError):
            # there are no gating keywords to reference here
            ur_bad = pf.UnivariateRegion3_0("G1", (0.0, 1.0))
            ag_bad = cast(AppliedGates3_0, ([], {0: ur_bad}, None))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_applied_gates_3_0_bad_gating(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        with pytest.raises(pf.RelationalError):
            # there are no gating keywords to reference here
            ur_bad = pf.UnivariateRegion3_0("P1", (0.0, 1.0))
            ag_bad = cast(AppliedGates3_0, ([], {0: ur_bad}, "NOT R2"))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_applied_gates_3_2(self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2) -> None:
        ur = pf.UnivariateRegion3_2(0, (0.0, 1.0))
        ag: AppliedGates3_2 = ({0: ur}, "NOT R1")
        core.applied_gates = ag

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_applied_gates_3_2_bad_index(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        with pytest.RaisesGroup(pf.PyreflowError):
            # 2 does not point to anything
            ur_bad = pf.UnivariateRegion3_2(2, (0.0, 1.0))
            ag_bad = cast(AppliedGates3_2, ({0: ur_bad}, None))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_applied_gates_3_2_bad_gating(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        with pytest.raises(pf.PyreflowError):
            ur_bad = pf.UnivariateRegion3_2(2, (0.0, 1.0))
            # R2 doesn't point to anything
            ag_bad = cast(AppliedGates3_2, ({0: ur_bad}, "NOT R2"))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["2_0"], ["text2", "dataset2"])
    def test_meas_scales(self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0) -> None:
        assert core.all_scales == [None, ()]
        core.all_scales = [(), ()]
        assert core.all_scales == [(), ()]

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["text2", "dataset2"])
    def test_meas_all_transforms(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.all_scale_transforms == [1.0, 1.0]

    # each of these should be strings or None
    @all_core2
    @pytest.mark.parametrize(
        "attr", [f"all_{x}" for x in ["filters", "detector_types"]]
    )
    def test_meas_opt_strs(self, attr: str, core: AnyCore) -> None:
        assert getattr(core, attr) == ["", ()]
        new = ["bla", ()]
        setattr(core, attr, new)
        assert getattr(core, attr) == new
        with pytest.raises(TypeError):
            setattr(core, attr, [42, ()])

    # each of these should be a non-negative float
    @all_core2
    @pytest.mark.parametrize(
        "attr",
        [f"all_{x}" for x in ["powers", "percents_emitted", "detector_voltages"]],
    )
    def test_meas_opt_floats(self, attr: str, core: AnyCore) -> None:
        assert getattr(core, attr) == [None, ()]
        new = 0.5
        setattr(core, attr, [new, ()])
        assert getattr(core, attr) == [new, ()]
        newer = 0.0
        setattr(core, attr, [newer, ()])
        assert getattr(core, attr) == [newer, ()]
        with pytest.raises(pf.InvalidKeywordValueError):
            setattr(core, attr, [-1.0, ()])
        with pytest.raises(TypeError):
            setattr(core, attr, ["pickle rick", ()])

    @pytest.mark.parametrize(
        "core, optical, temporal",
        [
            (lazy_fixture("text2_2_0"), pf.Optical2_0, pf.Temporal2_0),
            (lazy_fixture("text2_3_0"), pf.Optical3_0, pf.Temporal3_0),
            (lazy_fixture("text2_3_1"), pf.Optical3_1, pf.Temporal3_1),
            (lazy_fixture("text2_3_2"), pf.Optical3_2, pf.Temporal3_2),
            (lazy_fixture("dataset2_2_0"), pf.Optical2_0, pf.Temporal2_0),
            (lazy_fixture("dataset2_3_0"), pf.Optical3_0, pf.Temporal3_0),
            (lazy_fixture("dataset2_3_1"), pf.Optical3_1, pf.Temporal3_1),
            (lazy_fixture("dataset2_3_2"), pf.Optical3_2, pf.Temporal3_2),
        ],
    )
    def test_measurement_at(self, core: AnyCore, optical: type, temporal: type) -> None:
        assert isinstance(core.measurement_at(0), optical)
        assert isinstance(core.measurement_at(1), temporal)

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    @pytest.mark.parametrize(
        "attr",
        [f"all_{x}" for x in ["detector_names", "tags", "analytes"]],
    )
    def test_meas_3_2_str(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2, attr: str
    ) -> None:
        new = "ziltoid"
        getattr(core, attr) == [None, ()]
        setattr(core, attr, [new, ()])
        getattr(core, attr) == [new, ()]
        with pytest.raises(TypeError):
            setattr(core, attr, [10000000000000000000000, ()])

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    @pytest.mark.parametrize("attr", ["all_measurement_types"])
    def test_meas_3_2_measurement_types(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2, attr: str
    ) -> None:
        new = "--- --"
        getattr(core, attr) == ["", False]
        setattr(core, attr, [new, True])
        getattr(core, attr) == [new, True]
        with pytest.raises(TypeError):
            setattr(core, attr, [10000000000000000000000, None])
            setattr(core, attr, ["-.--.----..", "false"])

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_meas_3_2_feature(self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2) -> None:
        core.all_features == [None, ()]
        core.all_features = ["Area", ()]
        assert core.all_features == ["Area", ()]
        # this is also allowed
        core.all_features = ["Urea", ()]
        assert core.all_features == ["Urea", ()]

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_meas_3_2_awh_feature(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        core.all_awh_features == [None, ()]
        core.all_awh_features = ["Height", ()]
        assert core.all_features == ["Height", ()]
        with pytest.raises(pf.ParseKeywordValueError):
            core.all_awh_features = ["Seight", ()]  # type: ignore

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_meas_3_2_other_feature(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        core.all_other_features == [None, ()]
        core.all_awh_features = ["Width", ()]
        assert core.all_other_features == [None, ()]
        core.all_features = ["htdiW", ()]
        assert core.all_other_features == ["htdiW", ()]

    @parameterize_versions("core", ["3_1"], ["text2", "dataset2"])
    def test_meas_3_1_calibration(
        self, core: pf.CoreTEXT3_1 | pf.CoreDataset3_1
    ) -> None:
        new = (0.5, "NVidia A100 Heat Output")
        core.all_calibrations == [None, ()]
        core.all_calibrations = [new, ()]
        assert core.all_calibrations == [new, ()]
        with pytest.raises(TypeError):
            core.all_calibrations = ["AMD Threadripper Power Consumptions", ()]  # type: ignore

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_meas_3_2_calibration(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        new = (0.5, 0.25, "Gouda Cheese Wheels")
        core.all_calibrations == [None, ()]
        core.all_calibrations = [new, ()]
        assert core.all_calibrations == [new, ()]
        with pytest.raises(TypeError):
            core.all_calibrations = ["Sacred Cows", ()]  # type: ignore

    @parameterize_versions("core", ["2_0", "3_0"], ["text2", "dataset2"])
    def test_meas_wavelengths_singleton(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.all_wavelengths == [None, ()]
        core.all_wavelengths = [1.0, ()]
        assert core.all_wavelengths == [1.0, ()]
        with pytest.raises(pf.InvalidKeywordValueError):
            core.all_wavelengths = [0.0, ()]
        with pytest.raises(pf.InvalidKeywordValueError):
            core.all_wavelengths = [-1.0, ()]

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_meas_wavelengths_vector(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.all_wavelengths == [[], ()]
        new = [1.0, 2.0]
        core.all_wavelengths = [new, ()]
        assert core.all_wavelengths == [new, ()]
        with pytest.raises(pf.InvalidKeywordValueError):
            core.all_wavelengths = [[0.0], ()]
        with pytest.raises(pf.InvalidKeywordValueError):
            core.all_wavelengths = [[-1.0], ()]

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_meas_displays(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.all_displays == [None, None]
        new: list[tuple[bool, float, float] | None] = [
            (False, -1.0, 2.0),
            (True, 4.0, 0.5),
        ]
        core.all_displays = new
        assert core.all_displays == new

    @all_core
    def test_nonstandard(self, core: AnyCore) -> None:
        k = "midnight"
        v = "rowhammer"
        assert core.nonstandard_keywords == {}
        core.nonstandard_keywords = {k: v}
        assert core.nonstandard_keywords == {k: v}
        with pytest.raises(pf.ParseKeyError):
            core.nonstandard_keywords = {"$" + k: v}  # type: ignore

        # trying to get key from empty list should return None
        # assert core.get_nonstandard(k) is None
        # # ditto if we try to remove it
        # assert core.remove_nonstandard(k) is None
        # # insert should succeed
        # core.insert_nonstandard(k, v)
        # # now the key should be present
        # assert core.get_nonstandard(k) == v
        # # if we remove it we should also get the key
        # assert core.remove_nonstandard(k) == v
        # # no the key shouldn't be present again
        # assert core.get_nonstandard(k) is None
        # # and it shouldn't return anything if we try to remove it a 2nd time
        # assert core.remove_nonstandard(k) is None

    @parameterize_versions("core", ["2_0"], ["text", "dataset"])
    def test_temporal_no_timestep(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0
    ) -> None:
        assert core.temporal is None
        core.set_temporal(LINK_NAME1)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1
        assert core.unset_temporal() is True
        assert core.temporal is None
        assert core.unset_temporal() is False

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["text", "dataset"])
    def test_temporal_timestep(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.temporal is None
        ts = 1.0
        core.set_temporal(LINK_NAME1, ts)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1
        assert core.unset_temporal() == ts
        assert core.temporal is None
        assert core.unset_temporal() is None

    @parameterize_versions("core", ["2_0"], ["text", "dataset"])
    def test_temporal_no_timestep_at(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0
    ) -> None:
        assert core.temporal is None
        core.set_temporal_at(0, "false")
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["text", "dataset"])
    def test_temporal_timestep_at(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.temporal is None
        ts = 1.0
        core.set_temporal_at(0, ts, "false")
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), o)
            for c, o in [
                ("text_2_0", pf.Optical2_0),
                ("text_3_0", pf.Optical3_0),
                ("text_3_1", pf.Optical3_1),
                ("text_3_2", pf.Optical3_2),
            ]
        ],
    )
    def test_text_remove_meas_by_name(self, core: AnyCoreTEXT, optical: type) -> None:
        assert len(core.measurements) == 1
        i, m, r = core.remove_measurement_by_name(LINK_NAME1)
        assert i == 0
        assert isinstance(m, optical)
        assert r == 9001
        assert len(core.measurements) == 0
        with pytest.raises(KeyError):
            core.remove_measurement_by_name(LINK_NAME1)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), o)
            for c, o in [
                ("dataset_2_0", pf.Optical2_0),
                ("dataset_3_0", pf.Optical3_0),
                ("dataset_3_1", pf.Optical3_1),
                ("dataset_3_2", pf.Optical3_2),
            ]
        ],
    )
    def test_dataset_remove_meas_by_name(
        self, core: AnyCoreDataset, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        i, m, c, r = core.remove_measurement_by_name(LINK_NAME1)
        assert i == 0
        assert isinstance(m, optical)
        assert c.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert r == 9001
        assert len(core.measurements) == 0
        with pytest.raises(KeyError):
            core.remove_measurement_by_name(LINK_NAME1)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), o)
            for c, o in [
                ("text_2_0", pf.Optical2_0),
                ("text_3_0", pf.Optical3_0),
                ("text_3_1", pf.Optical3_1),
                ("text_3_2", pf.Optical3_2),
            ]
        ],
    )
    def test_text_remove_meas_by_index(self, core: AnyCoreTEXT, optical: type) -> None:
        assert len(core.measurements) == 1
        n, m, r = core.remove_measurement_by_index(0)
        assert n == LINK_NAME1
        assert isinstance(m, optical)
        assert r == 9001
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), o)
            for c, o in [
                ("dataset_2_0", pf.Optical2_0),
                ("dataset_3_0", pf.Optical3_0),
                ("dataset_3_1", pf.Optical3_1),
                ("dataset_3_2", pf.Optical3_2),
            ]
        ],
    )
    def test_dataset_remove_meas_by_index(
        self, core: AnyCoreDataset, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        n, m, c, r = core.remove_measurement_by_index(0)
        assert n == LINK_NAME1
        assert isinstance(m, optical)
        assert c.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert r == 9001
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @all_core3
    def test_remove_meas_by_name_with_tr(self, core: AnyCore) -> None:
        core.tr = (LINK_NAME1, 1)
        assert core.remove_measurement_by_name(LINK_NAME2) is not None
        assert core.remove_measurement_by_name(LINK_NAME3) is not None
        # choke if linked
        with pytest.RaisesGroup(pf.RelationalError):
            assert core.remove_measurement_by_name(LINK_NAME1) is not None

    @parameterize_versions("core", ["3_1", "3_2"], ["text3", "dataset3"])
    def test_remove_meas_by_name_with_spillover(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        sp = (
            [LINK_NAME1, LINK_NAME3],
            np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
        )
        core.spillover = sp
        assert core.remove_measurement_by_name(LINK_NAME2) is not None
        # choke if linked
        with pytest.RaisesGroup(pf.RelationalError):
            assert core.remove_measurement_by_name(LINK_NAME1) is not None

    @parameterize_versions("core", ["3_2"], ["text3", "dataset3"])
    def test_remove_meas_by_name_with_unstained(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        core.unstainedcenters = {LINK_NAME1: 42}
        assert core.remove_measurement_by_name(LINK_NAME2) is not None
        assert core.remove_measurement_by_name(LINK_NAME3) is not None
        # choke if linked
        with pytest.RaisesGroup(pf.RelationalError):
            assert core.remove_measurement_by_name(LINK_NAME1) is not None

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_remove_meas_by_index_ag3_0(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion3_0("P1", (0.0, 1.0))
        ag: AppliedGates3_0 = ([], {0: ur}, "NOT R1")
        core.applied_gates = ag
        core.remove_measurement_by_index(1)
        with pytest.RaisesGroup(pf.RelationalError):
            core.remove_measurement_by_index(0)

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_remove_meas_by_index_ag3_2(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion3_2(0, (0.0, 1.0))
        ag: AppliedGates3_2 = ({0: ur}, "NOT R1")
        core.applied_gates = ag
        core.remove_measurement_by_index(1)
        with pytest.RaisesGroup(pf.RelationalError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_2_0", "blank_optical_2_0"),
                ("text_3_0", "blank_optical_3_0"),
                ("text_3_1", "blank_optical_3_1"),
                ("text_3_2", "blank_optical_3_2"),
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_replace_optical_at(self, core: AnyCore, optical: Any) -> None:
        ln = "I am not living"
        optical.longname = ln
        core.replace_optical_at(0, optical)
        core.measurement_at(0).longname == ln

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_2_0", "blank_optical_2_0"),
                ("text_3_0", "blank_optical_3_0"),
                ("text_3_1", "blank_optical_3_1"),
                ("text_3_2", "blank_optical_3_2"),
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_replace_optical_named(self, core: AnyCore, optical: Any) -> None:
        ln = "I'm asleep"
        optical.longname = ln
        core.replace_optical_named(LINK_NAME1, optical)
        core.measurement_at(0).longname == ln

    @pytest.mark.parametrize(
        "core, temporal",
        [
            (lazy_fixture(c), lazy_fixture(t))
            for c, t in [
                ("text2_2_0", "blank_temporal_2_0"),
                ("text2_3_0", "blank_temporal_3_0"),
                ("text2_3_1", "blank_temporal_3_1"),
                ("text2_3_2", "blank_temporal_3_2"),
                ("dataset2_2_0", "blank_temporal_2_0"),
                ("dataset2_3_0", "blank_temporal_3_0"),
                ("dataset2_3_1", "blank_temporal_3_1"),
                ("dataset2_3_2", "blank_temporal_3_2"),
            ]
        ],
    )
    def test_replace_temporal_at(self, core: AnyCore, temporal: Any) -> None:
        ln = "show me wut u got"
        temporal.longname = ln
        core.replace_temporal_at(1, temporal)
        core.measurement_at(1).longname == ln

    @pytest.mark.parametrize(
        "core, temporal",
        [
            (lazy_fixture(c), lazy_fixture(t))
            for c, t in [
                ("text2_2_0", "blank_temporal_2_0"),
                ("text2_3_0", "blank_temporal_3_0"),
                ("text2_3_1", "blank_temporal_3_1"),
                ("text2_3_2", "blank_temporal_3_2"),
                ("dataset2_2_0", "blank_temporal_2_0"),
                ("dataset2_3_0", "blank_temporal_3_0"),
                ("dataset2_3_1", "blank_temporal_3_1"),
                ("dataset2_3_2", "blank_temporal_3_2"),
            ]
        ],
    )
    def test_replace_temporal_named(self, core: AnyCore, temporal: Any) -> None:
        ln = "the combination is... 1. 2. 3. 4. 5."
        temporal.longname = ln
        core.replace_temporal_named(LINK_NAME2, temporal)
        core.measurement_at(1).longname == ln

    @all_core2
    def test_rename_temporal(self, core: AnyCore) -> None:
        new = "they've gone plaid"
        assert core.rename_temporal(new) == LINK_NAME2

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("blank_text_2_0", "blank_optical_2_0"),
                ("blank_text_3_0", "blank_optical_3_0"),
                ("blank_text_3_1", "blank_optical_3_1"),
                ("blank_text_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_text_insert_optical(self, core: AnyCoreTEXT, optical: Any) -> None:
        core.insert_optical(0, LINK_NAME1, optical, 9001)
        assert isinstance(core.measurement_at(0), type(optical))

    @pytest.mark.parametrize(
        "core, temporal",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("blank_text_2_0", "blank_temporal_2_0"),
                ("blank_text_3_0", "blank_temporal_3_0"),
                ("blank_text_3_1", "blank_temporal_3_1"),
                ("blank_text_3_2", "blank_temporal_3_2"),
            ]
        ],
    )
    def test_text_insert_temporal(self, core: AnyCoreTEXT, temporal: Any) -> None:
        core.insert_temporal(0, LINK_NAME1, temporal, 9001)
        assert isinstance(core.measurement_at(0), type(temporal))

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("blank_dataset_2_0", "blank_optical_2_0"),
                ("blank_dataset_3_0", "blank_optical_3_0"),
                ("blank_dataset_3_1", "blank_optical_3_1"),
                ("blank_dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_dataset_insert_optical(
        self, core: AnyCoreDataset, optical: Any, series1: pl.Series
    ) -> None:
        core.insert_optical(0, LINK_NAME1, optical, series1, 9001)
        assert isinstance(core.measurement_at(0), type(optical))

    @pytest.mark.parametrize(
        "core, temporal",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("blank_dataset_2_0", "blank_temporal_2_0"),
                ("blank_dataset_3_0", "blank_temporal_3_0"),
                ("blank_dataset_3_1", "blank_temporal_3_1"),
                ("blank_dataset_3_2", "blank_temporal_3_2"),
            ]
        ],
    )
    def test_dataset_insert_temporal(
        self, core: AnyCoreDataset, temporal: Any, series1: pl.Series
    ) -> None:
        core.insert_temporal(0, LINK_NAME1, temporal, series1, 9001)
        assert isinstance(core.measurement_at(0), type(temporal))

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text2"])
    def test_unset_measurements(self, core: AnyCoreTEXT) -> None:
        assert len(core.measurements) == 2
        core.unset_measurements()
        assert len(core.measurements) == 0

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    def test_unset_data(self, core: AnyCoreDataset) -> None:
        df0 = core.data
        assert df0.height == 3
        assert df0.width == 2
        assert len(core.measurements) == 2
        core.unset_data()
        df1 = core.data
        assert df1.height == 0
        assert df1.width == 0
        assert len(core.measurements) == 0

    @parameterize_versions("core", ["2_0", "3_0"], ["text2", "dataset2"])
    def test_ordered_layout(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert isinstance(core.layout, pf.OrderedUint32Layout)
        core.layout = pf.OrderedUint64Layout([9002, 9003])
        assert isinstance(core.layout, pf.OrderedUint64Layout)
        with pytest.raises(TypeError):
            core.layout = pf.EndianUintLayout([9002, 9003], False)  # type: ignore

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_endian_layout(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert isinstance(core.layout, pf.EndianUintLayout)
        core.layout = pf.EndianF32Layout([Decimal(9002), Decimal(9003)])
        assert isinstance(core.layout, pf.EndianF32Layout)
        with pytest.raises(TypeError):
            core.layout = pf.OrderedUint64Layout([9002, 9003])  # type: ignore

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_2_0", "blank_optical_2_0"),
                ("text_3_0", "blank_optical_3_0"),
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
            ]
        ],
    )
    def test_ordered_set_measurements(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
        optical: Any,
    ) -> None:
        core.set_named_measurements([(LINK_NAME1, optical)], False, False)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_3_1", "blank_optical_3_1"),
                ("text_3_2", "blank_optical_3_2"),
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_endian_set_measurements(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
    ) -> None:
        core.set_named_measurements([(LINK_NAME1, optical)], False, False)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_2_0", "blank_optical_2_0"),
                ("text_3_0", "blank_optical_3_0"),
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
            ]
        ],
    )
    def test_ordered_set_measurements_and_layout(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
        optical: Any,
    ) -> None:
        new = pf.OrderedUint64Layout([1])
        core.set_named_measurements_and_layout(
            [(LINK_NAME1, optical)], new, False, False
        )

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("text_3_1", "blank_optical_3_1"),
                ("text_3_2", "blank_optical_3_2"),
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_endian_set_measurements_and_layout(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
    ) -> None:
        new = pf.EndianF32Layout([Decimal(1)])
        core.set_named_measurements_and_layout(
            [(LINK_NAME1, optical)], new, False, False
        )

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
            ]
        ],
    )
    def test_ordered_set_measurements_and_data(
        self,
        core: pf.CoreDataset2_0 | pf.CoreDataset3_0,
        optical: Any,
        series2: pl.Series,
    ) -> None:
        core.set_named_measurements_and_data(
            [(LINK_NAME1, optical)], pl.DataFrame([series2]), False, False
        )

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_endian_set_measurements_and_data(
        self,
        core: pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
        series2: pl.Series,
    ) -> None:
        core.set_named_measurements_and_data(
            [(LINK_NAME1, optical)], pl.DataFrame([series2]), False, False
        )

    @pytest.mark.parametrize(
        "core, optical, temporal",
        [
            (lazy_fixture("text2_2_0"), pf.Optical2_0, pf.Temporal2_0),
            (lazy_fixture("text2_3_0"), pf.Optical3_0, pf.Temporal3_0),
            (lazy_fixture("text2_3_1"), pf.Optical3_1, pf.Temporal3_1),
            (lazy_fixture("text2_3_2"), pf.Optical3_2, pf.Temporal3_2),
            (lazy_fixture("dataset2_2_0"), pf.Optical2_0, pf.Temporal2_0),
            (lazy_fixture("dataset2_3_0"), pf.Optical3_0, pf.Temporal3_0),
            (lazy_fixture("dataset2_3_1"), pf.Optical3_1, pf.Temporal3_1),
            (lazy_fixture("dataset2_3_2"), pf.Optical3_2, pf.Temporal3_2),
        ],
    )
    def test_measurements(self, core: AnyCore, optical: type, temporal: type) -> None:
        assert len(core.measurements) == 2
        assert isinstance(core.measurements[0], optical)
        assert isinstance(core.measurements[1], temporal)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_2_0", pf.CoreTEXT3_0),
                ("dataset2_2_0", pf.CoreDataset3_0),
            ]
        ],
    )
    def test_2_0_to_3_0(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0, target: type
    ) -> None:
        # should fail if $PnE are missing
        with pytest.RaisesGroup(pf.PyreflowError):
            core.to_version_3_0()
        # and should still fail when forced since $PnE is missing
        with pytest.RaisesGroup(pf.PyreflowError):
            core.to_version_3_0("true")
        core.all_scales = [(), ()]
        new = core.to_version_3_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_2_0", pf.CoreTEXT3_1),
                ("dataset2_2_0", pf.CoreDataset3_1),
            ]
        ],
    )
    def test_2_0_to_3_1(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0, target: type
    ) -> None:
        # should fail if $PnE are missing
        with pytest.RaisesGroup(pf.PyreflowError):
            core.to_version_3_1()
        # and should still fail when forced since $PnE is missing
        with pytest.RaisesGroup(pf.PyreflowError):
            core.to_version_3_1("true")
        core.all_scales = [(), ()]
        new = core.to_version_3_1()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_2_0", pf.CoreTEXT3_2),
                ("dataset2_2_0", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_2_0_to_3_2(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0, target: type
    ) -> None:
        # should fail if $PnE and $CYT are missing
        with pytest.RaisesGroup(pf.ConversionError, pf.ConversionError):
            core.to_version_3_2()
        # and should still fail if we force since $CYT and $PnE are missing
        with pytest.RaisesGroup(pf.ConversionError, pf.ConversionError):
            core.to_version_3_2("true")
        core.cyt = "T cell incinerator"
        core.all_scales = [(), ()]
        core.cyt = "T cell incinerator"
        new = core.to_version_3_2()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_0", pf.CoreTEXT2_0),
                ("dataset2_3_0", pf.CoreDataset2_0),
            ]
        ],
    )
    def test_3_0_to_2_0(
        self, core: pf.CoreTEXT3_0 | pf.CoreDataset3_0, target: type
    ) -> None:
        new = core.to_version_2_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_0", pf.CoreTEXT3_1),
                ("dataset2_3_0", pf.CoreDataset3_1),
            ]
        ],
    )
    def test_3_0_to_3_1(
        self, core: pf.CoreTEXT3_0 | pf.CoreDataset3_0, target: type
    ) -> None:
        new = core.to_version_3_1()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_0", pf.CoreTEXT3_2),
                ("dataset2_3_0", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_3_0_to_3_2(
        self, core: pf.CoreTEXT3_0 | pf.CoreDataset3_0, target: type
    ) -> None:
        # should fail if $CYT is missing
        with pytest.RaisesGroup(pf.ConversionError):
            core.to_version_3_2()
        # and should still fail if forced since $CYT is missing
        with pytest.RaisesGroup(pf.ConversionError):
            core.to_version_3_2("true")
        core.cyt = "the dark eternal void from which cells will never escape"
        new = core.to_version_3_2()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_1", pf.CoreTEXT2_0),
                ("dataset2_3_1", pf.CoreDataset2_0),
            ]
        ],
    )
    def test_3_1_to_2_0(
        self, core: pf.CoreTEXT3_1 | pf.CoreDataset3_1, target: type
    ) -> None:
        new = core.to_version_2_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_1", pf.CoreTEXT3_0),
                ("dataset2_3_1", pf.CoreDataset3_0),
            ]
        ],
    )
    def test_3_1_to_3_0(
        self, core: pf.CoreTEXT3_1 | pf.CoreDataset3_1, target: type
    ) -> None:
        new = core.to_version_3_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_1", pf.CoreTEXT3_2),
                ("dataset2_3_1", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_3_1_to_3_2(
        self, core: pf.CoreTEXT3_1 | pf.CoreDataset3_1, target: type
    ) -> None:
        # should fail if $CYT is missing
        with pytest.RaisesGroup(pf.ConversionError):
            core.to_version_3_2()
        # should still fail when forced
        with pytest.RaisesGroup(pf.ConversionError):
            core.to_version_3_2("true")
        core.cyt = "Cygnus X-1"
        new = core.to_version_3_2()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_2", pf.CoreTEXT2_0),
                ("dataset2_3_2", pf.CoreDataset2_0),
            ]
        ],
    )
    def test_3_2_to_2_0(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2, target: type
    ) -> None:
        new = core.to_version_2_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_2", pf.CoreTEXT3_0),
                ("dataset2_3_2", pf.CoreDataset3_0),
            ]
        ],
    )
    def test_3_2_to_3_0(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2, target: type
    ) -> None:
        new = core.to_version_3_0()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_3_2", pf.CoreTEXT3_1),
                ("dataset2_3_2", pf.CoreDataset3_1),
            ]
        ],
    )
    def test_3_2_to_3_1(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2, target: type
    ) -> None:
        new = core.to_version_3_1()
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text2_2_0", pf.CoreDataset2_0),
                ("text2_3_0", pf.CoreDataset3_0),
                ("text2_3_1", pf.CoreDataset3_1),
                ("text2_3_2", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_text_to_dataset(
        self, core: AnyCoreTEXT, target: type, series1: pl.Series, series2: pl.Series
    ) -> None:
        with pytest.raises(pf.PyreflowError):
            core.to_dataset(pl.DataFrame([series1]), b"", [])
        new = core.to_dataset(pl.DataFrame([series1, series2]), b"", [])
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text_2_0", pf.CoreDataset2_0),
                ("text_3_0", pf.CoreDataset3_0),
                ("text_3_1", pf.CoreDataset3_1),
                ("text_3_2", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_text_to_dataset_chunked(
        self, core: AnyCoreTEXT, target: type, series1: pl.Series, series2: pl.Series
    ) -> None:
        # Despite having multiple chunks, this should never fail because the
        # python->rust conversion for PySeries will call .rechunk. It also calls
        # .to_arrow which is why pyarrow is necessary. See
        # https://github.com/pola-rs/polars/blob/f91c3a865aaea6dc92cad7bc75572f2c9dd23ac9/pyo3-polars/pyo3-polars/src/types.rs#L177
        d0 = pl.DataFrame([[1, 2]], {LINK_NAME1: pl.UInt32})
        d1 = pl.DataFrame([[3, 4]], {LINK_NAME1: pl.UInt32})
        d2 = d0.vstack(d1)
        assert d2.n_chunks() == 2
        new = core.to_dataset(d2, b"", [])
        assert isinstance(new, target)

    @pytest.mark.parametrize(
        "core, target",
        [
            (lazy_fixture(c), t)
            for c, t in [
                ("text_2_0", pf.CoreDataset2_0),
                ("text_3_0", pf.CoreDataset3_0),
                ("text_3_1", pf.CoreDataset3_1),
                ("text_3_2", pf.CoreDataset3_2),
            ]
        ],
    )
    def test_text_to_dataset_null(
        self, core: AnyCoreTEXT, target: type, series1: pl.Series, series2: pl.Series
    ) -> None:
        d = pl.DataFrame([[1, None]], {LINK_NAME1: pl.UInt32})
        with pytest.raises(pf.EventDataError):
            core.to_dataset(d, b"", [])

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    @pytest.mark.parametrize(
        "dtype", [pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]
    )
    def test_data_dtypes(self, core: AnyCoreDataset, dtype: Any) -> None:
        core.data = pl.DataFrame([[1, 2]], {LINK_NAME1: dtype})


class TestGating:
    def test_scale(self, blank_gated_meas: pf.GatedMeasurement) -> None:
        assert blank_gated_meas.scale is None
        blank_gated_meas.scale = ()
        assert blank_gated_meas.scale == ()
        blank_gated_meas.scale = (1.0, 2.0)
        assert blank_gated_meas.scale == (1.0, 2.0)
        with pytest.raises(TypeError):
            blank_gated_meas.scale = cast(tuple[()], "the new abnormal")

    def test_range(self, blank_gated_meas: pf.GatedMeasurement) -> None:
        assert blank_gated_meas.range is None
        blank_gated_meas.range = 1.0
        assert blank_gated_meas.range == 1.0
        with pytest.raises(ValueError):
            blank_gated_meas.range = cast(float, "hail stan")

    @pytest.mark.parametrize("attr", ["percent_emitted", "detector_voltage"])
    def test_floats(self, blank_gated_meas: pf.GatedMeasurement, attr: str) -> None:
        assert getattr(blank_gated_meas, attr) is None
        new = 1.0
        setattr(blank_gated_meas, attr, new)
        assert getattr(blank_gated_meas, attr) == new
        with pytest.raises(TypeError):
            setattr(blank_gated_meas, attr, "3.14...4...4...4...4...uuuuuuuuhhhhh")

    @pytest.mark.parametrize("attr", ["filter", "longname", "detector_type"])
    def test_strs(self, blank_gated_meas: pf.GatedMeasurement, attr: str) -> None:
        assert getattr(blank_gated_meas, attr) == ""
        new = "this is sweet revenge and karma's a"
        setattr(blank_gated_meas, attr, new)
        assert getattr(blank_gated_meas, attr) == new
        with pytest.raises(TypeError):
            setattr(blank_gated_meas, attr, 1.0)

    def test_shortname(self, blank_gated_meas: pf.GatedMeasurement) -> None:
        assert blank_gated_meas.shortname is None
        new = "shorty"
        blank_gated_meas.shortname = new
        blank_gated_meas.shortname == new
        with pytest.raises(TypeError):
            blank_gated_meas.shortname = cast(str, 1.0)

    def test_uvregion2_0(self) -> None:
        r = pf.UnivariateRegion2_0(0, (0.0, 1.0))
        assert r.index == 0
        assert r.gate == (0.0, 1.0)

    def test_uvregion3_0(self) -> None:
        # TODO this is confusing as ****, for the other two we get a 0-index
        # and here we get a 1-index
        r = pf.UnivariateRegion3_0("P1", (0.0, 1.0))
        assert r.index == "P1"
        assert r.gate == (0.0, 1.0)

    def test_uvregion3_2(self) -> None:
        r = pf.UnivariateRegion3_2(0, (0.0, 1.0))
        assert r.index == 0
        assert r.gate == (0.0, 1.0)

    def test_bvregion2_0(self) -> None:
        i = (0, 1)
        # TODO this should have 3 vertices minimum, a line gate makes no sense
        vs = [(0.0, 1.0), (1.0, 3.0)]
        r = pf.BivariateRegion2_0(i, vs)
        assert r.index == i
        assert r.vertices == vs

    def test_bvregion3_0(self) -> None:
        i = ("P1", "G2")
        vs = [(0.0, 1.0), (1.0, 3.0)]
        r = pf.BivariateRegion3_0(i, vs)
        assert r.index == i
        assert r.vertices == vs

    def test_bvregion3_2(self) -> None:
        i = (0, 1)
        vs = [(0.0, 1.0), (1.0, 3.0)]
        r = pf.BivariateRegion3_2(i, vs)
        assert r.index == i
        assert r.vertices == vs


class TestMeas:
    @all_blank_meas
    def test_longname(self, meas: AnyMeas) -> None:
        assert meas.longname == ""
        new = "Headbangeeeeeeeeeeeerrrrrrrrrrrrrrrrrrrrrrrrrrr!!!!!!"
        meas.longname = new
        assert meas.longname == new
        with pytest.raises(TypeError):
            meas.longname = cast(str, 666666666666666666666666)

    @all_blank_optical
    @pytest.mark.parametrize("attr", ["detector_voltage", "percent_emitted"])
    def test_optical_float(self, meas: AnyOptical, attr: str) -> None:
        assert getattr(meas, attr) is None
        new = 1.0
        setattr(meas, attr, new)
        assert getattr(meas, attr) == new
        with pytest.raises(TypeError):
            setattr(meas, attr, "the one")

    @all_blank_optical
    @pytest.mark.parametrize("attr", ["filter", "detector_type"])
    def test_optical_str(self, meas: AnyOptical, attr: str) -> None:
        assert getattr(meas, attr) == ""
        new = "punky bruster"
        setattr(meas, attr, new)
        assert getattr(meas, attr) == new
        with pytest.raises(TypeError):
            setattr(meas, attr, 13)

    @parameterize_versions("meas", ["3_1", "3_2"], ["blank_optical", "blank_temporal"])
    def test_display(
        self, meas: pf.Optical3_1 | pf.Optical3_2 | pf.Temporal3_1 | pf.Temporal3_2
    ) -> None:
        assert meas.display is None
        new = (False, 0.0, 1.0)
        meas.display = new
        assert meas.display == new
        with pytest.raises(TypeError):
            meas.display = 999  # type: ignore

    @parameterize_versions("meas", ["2_0"], ["blank_optical"])
    def test_scale(self, meas: pf.Optical2_0) -> None:
        assert meas.scale is None
        meas.scale = ()
        assert meas.scale == ()
        with pytest.raises(TypeError):
            meas.scale = "the summit"  # type: ignore

    @parameterize_versions("meas", ["3_0", "3_1", "3_2"], ["blank_optical"])
    def test_transform(
        self, meas: pf.Optical3_0 | pf.Optical3_1 | pf.Optical3_2
    ) -> None:
        assert meas.transform == 1.0
        new = (4.0, 0.5)
        meas.transform = new
        assert meas.transform == new
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.transform = 0.0
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.transform = (0.0, 0.0)

    @parameterize_versions("meas", ["3_0", "3_1", "3_2"], ["blank_temporal"])
    def test_timestep(
        self, meas: pf.Temporal3_0 | pf.Temporal3_1 | pf.Temporal3_2
    ) -> None:
        assert meas.timestep == 1.0
        meas.timestep = 2.0
        assert meas.timestep == 2.0
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.timestep = 0.0

    @parameterize_versions("meas", ["2_0", "3_0"], ["blank_optical"])
    def test_wavelength_2_0(self, meas: pf.Optical2_0 | pf.Optical3_0) -> None:
        assert meas.wavelength is None
        new = 1.0
        meas.wavelength = new
        assert meas.wavelength == new
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.wavelength = 0.0
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.wavelength = -1.0

    @parameterize_versions("meas", ["3_1", "3_2"], ["blank_optical"])
    def test_wavelength_3_1(self, meas: pf.Optical3_1 | pf.Optical3_2) -> None:
        assert meas.wavelengths == []
        new = [1.0, 2.0]
        meas.wavelengths = new
        assert meas.wavelengths == new
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.wavelengths = [-1.0]
        with pytest.raises(pf.InvalidKeywordValueError):
            meas.wavelengths = [0.0]

    @parameterize_versions("meas", ["3_1"], ["blank_optical"])
    def test_calibration_3_1(self, meas: pf.Optical3_1) -> None:
        assert meas.calibration is None
        new = (4.0, "imperial mega-amperes")
        meas.calibration = new
        assert meas.calibration == new
        with pytest.raises(TypeError):
            meas.calibration = "OOOOOOOO"  # type: ignore

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    def test_calibration_3_2(self, meas: pf.Optical3_2) -> None:
        assert meas.calibration is None
        new = (1.0, 0.0, "John Carmack Equivalents")
        meas.calibration = new
        assert meas.calibration == new
        with pytest.raises(TypeError):
            meas.calibration = "XYYXYXYYXYXYY"  # type: ignore

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    def test_feature_3_2(self, meas: pf.Optical3_2) -> None:
        assert meas.feature is None
        meas.feature = "Area"
        assert meas.feature == "Area"
        # this is also allowed for this attribute
        meas.feature = "under da curv"
        assert meas.feature == "under da curv"

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    def test_awh_feature_3_2(self, meas: pf.Optical3_2) -> None:
        assert meas.awh_feature is None
        meas.awh_feature = "Area"
        assert meas.awh_feature == "Area"
        with pytest.raises(pf.ParseKeywordValueError):
            meas.awh_feature = "under da curv"  # type: ignore

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    @pytest.mark.parametrize("attr", ["detector_name", "tag", "analyte"])
    def test_optical_3_2(self, meas: AnyOptical, attr: str) -> None:
        assert getattr(meas, attr) == ""
        new = "heavy metal kitten pix"
        setattr(meas, attr, new)
        assert getattr(meas, attr) == new
        with pytest.raises(TypeError):
            setattr(meas, attr, 555)

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    def test_optical_meas_type(self, meas: pf.Optical3_2) -> None:
        meas.measurement_type is None
        # maybe if I use enough caps, David Goggins will have mercy on my soul
        # and my problems will magically go away
        new = "TO THE THRESHOOOOOOOOLD!!!!!!!!!!"
        meas.measurement_type = new
        meas.measurement_type == new
        with pytest.raises(TypeError):
            meas.measurement_type = cast(str, 555)

    @parameterize_versions("meas", ["3_2"], ["blank_temporal"])
    def test_temporal_type(self, meas: pf.Temporal3_2) -> None:
        assert not meas.has_type
        meas.has_type = True
        assert meas.has_type

    @all_blank_meas
    def test_nonstandard(self, meas: AnyOptical) -> None:
        assert meas.nonstandard_keywords == {}
        with pytest.raises(pf.ParseKeyError):
            meas.nonstandard_keywords = {"$GOD": "MONEY"}
        k = "my bitwarden password"
        v0 = "SSBzb2xlbW5seSBzd2VhciBJIGFtIHVwIHRvIG5vIGdvb2QK"
        meas.nonstandard_keywords = {k: v0}


class TestLayouts:
    def test_ascii_fixed(self) -> None:
        ranges = [9, 99, 999]
        new = pf.FixedAsciiLayout(ranges)
        assert new.char_widths == [1, 2, 3]
        assert new.ranges == ranges
        assert new.datatype == "A"
        with pytest.raises(OverflowError):
            ranges = [1 * 10**20]
            new = pf.FixedAsciiLayout(ranges)

    def test_ascii_delim(self) -> None:
        ranges = [9, 99, 999]
        new = pf.DelimAsciiLayout(ranges)
        assert new.ranges == ranges
        assert new.datatype == "A"

    @pytest.mark.parametrize(
        "layout, width",
        [
            (pf.OrderedUint08Layout, 8),
            (pf.OrderedUint16Layout, 16),
            (pf.OrderedUint24Layout, 24),
            (pf.OrderedUint32Layout, 32),
            (pf.OrderedUint40Layout, 40),
            (pf.OrderedUint48Layout, 48),
            (pf.OrderedUint56Layout, 56),
            (pf.OrderedUint64Layout, 64),
        ],
    )
    def test_ordered_uint(self, layout: type, width: int) -> None:
        n = int(width / 8)
        bitmasks = [2 ** (8 * (b + 1)) - 1 for b in range(n)]
        new = layout(bitmasks)
        # NOTE ranges will be 1+ whatever we put in because the inputs to the
        # the layout are literal ints and the output below is whatever the $PnR
        # value will be, which is 1+ the actual number...thanks FCS
        if n > 2:
            assert new.byteord == "little"
        assert new.byte_width == n
        assert new.ranges == [r for r in bitmasks]
        assert new.datatype == "I"
        with pytest.raises(OverflowError):
            layout([2**width])

    @pytest.mark.parametrize(
        "layout, width, datatype",
        [
            (pf.OrderedF32Layout, 32, "F"),
            (pf.OrderedF64Layout, 64, "D"),
            (pf.EndianF32Layout, 32, "F"),
            (pf.EndianF64Layout, 64, "D"),
        ],
    )
    def test_float(self, layout: type, width: int, datatype: Datatype) -> None:
        n = 3
        new = layout([Decimal(1000.0)] * n)
        assert new.byte_width == width / 8
        assert new.ranges == [Decimal(1000.0)] * n
        assert new.datatype == datatype
        with pytest.raises(ValueError):
            layout([float("inf")])

    def test_endian_uint(self) -> None:
        ranges = [2**8 - 1, 2**16 - 1, 2**24 - 1]
        new = pf.EndianUintLayout(ranges)
        assert new.byte_widths == [1, 2, 3]
        assert new.ranges == ranges
        assert new.datatype == "I"

    def test_mixed(self) -> None:
        types: list[MixedType] = [
            ("F", Decimal(1000.0)),
            ("D", Decimal(2000.0)),
            ("I", 255),
        ]
        new = pf.MixedLayout(types)
        assert new.byte_widths == [4, 8, 1]
        assert new.typed_ranges == types


class TestApiFunctions:
    def test_flat_text_to_parent(self) -> None:
        conf = pfp.PyreflowReadFlatTEXTConfig().to_header_config()
        assert isinstance(conf, pfp.PyreflowReadHeaderConfig)

    def test_std_text_to_parent(self) -> None:
        conf = pfp.PyreflowReadStdTEXTConfig()
        conf0 = conf.to_header_config()
        assert isinstance(conf0, pfp.PyreflowReadHeaderConfig)
        conf1 = conf.to_flat_text_config()
        assert isinstance(conf1, pfp.PyreflowReadFlatTEXTConfig)

    def test_flat_dataset_to_parent(self) -> None:
        conf = pfp.PyreflowReadFlatDatasetConfig()
        assert isinstance(conf.to_header_config(), pfp.PyreflowReadHeaderConfig)
        assert isinstance(conf.to_flat_text_config(), pfp.PyreflowReadFlatTEXTConfig)

    def test_std_dataset_to_parent(self) -> None:
        conf = pfp.PyreflowReadStdDatasetConfig()
        assert isinstance(conf.to_header_config(), pfp.PyreflowReadHeaderConfig)
        assert isinstance(conf.to_flat_text_config(), pfp.PyreflowReadFlatTEXTConfig)
        assert isinstance(conf.to_std_text_config(), pfp.PyreflowReadStdTEXTConfig)

    def test_read_header(self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadHeaderConfig()
        _ = pf.api.fcs_read_header(p, **conf.model_dump(), dataset_offset=0)

    def test_read_header_pd(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pfp.PyreflowReadHeaderConfig().read_header(p)

    def test_read_flat_text(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadFlatTEXTConfig()
        _ = pf.api.fcs_read_flat_text(p, **conf.model_dump(), dataset_offset=0)
        _ = pf.api.fcs_read_flat_texts(p, **conf.model_dump())

    def test_read_flat_text_pd(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadFlatTEXTConfig()
        _ = conf.read_flat_text(p)
        _ = conf.read_flat_texts(p)

    def test_read_std_text(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadStdTEXTConfig()
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
            _ = pf.api.fcs_read_std_text(p, **conf.model_dump(), dataset_offset=0)
            _ = pf.api.fcs_read_std_texts(p, **conf.model_dump())

    def test_read_std_text_pd(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadStdTEXTConfig()
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
            _ = conf.read_std_text(p)
            _ = conf.read_std_texts(p)

    def test_read_flat_dataset(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadFlatDatasetConfig()
        _ = pf.api.fcs_read_flat_dataset(p, **conf.model_dump(), dataset_offset=0)
        _ = pf.api.fcs_read_flat_datasets(p, **conf.model_dump())

    def test_read_flat_dataset_pd(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadFlatDatasetConfig()
        _ = conf.read_flat_dataset(p)
        _ = conf.read_flat_datasets(p)

    def test_read_std_dataset(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadStdDatasetConfig()
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
            _ = pf.api.fcs_read_std_dataset(p, **conf.model_dump(), dataset_offset=0)
            _ = pf.api.fcs_read_std_datasets(p, **conf.model_dump())

    def test_read_std_dataset_pd(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        conf = pfp.PyreflowReadStdDatasetConfig()
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
            _ = conf.read_std_dataset(p)
            _ = conf.read_std_datasets(p)

    def test_other_width(self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_header(p, other_width=8)
        _ = pf.api.fcs_read_header(p, other_width=20)
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_header(p, other_width=7)
            _ = pf.api.fcs_read_header(p, other_width=21)

    def test_key_patterns(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_flat_text(p, ignore_standard_keys=["wood"])
        _ = pf.api.fcs_read_flat_text(p, ignore_standard_keys=["/lawnmower+spike/"])
        # TODO blank should be an error since it will match anything
        with pytest.raises(pf.ParseKeyError):
            _ = pf.api.fcs_read_flat_text(p, ignore_standard_keys=[""])
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_flat_text(p, ignore_standard_keys=["/((((/"])

    def test_rename_standard_keys(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_flat_text(p, rename_standard_keys={"dollar": "bitcoin"})
        with pytest.raises(pf.ParseKeyError):
            _ = pf.api.fcs_read_flat_text(p, rename_standard_keys={"": "notblank"})
            _ = pf.api.fcs_read_flat_text(p, rename_standard_keys={"notblank": ""})

    def test_replace_standard_key_values(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_flat_text(
            p, replace_standard_key_values={"meaning_of_life": "explosions"}
        )
        _ = pf.api.fcs_read_flat_text(
            p, replace_standard_key_values={"meaning_of_life": ""}
        )
        with pytest.raises(pf.ParseKeyError):
            _ = pf.api.fcs_read_flat_text(
                p, replace_standard_key_values={"": "notblank"}
            )

    def test_append_standard_keys(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_flat_text(
            p, append_standard_keywords={"meaning_of_life": "plutonium"}
        )
        _ = pf.api.fcs_read_flat_text(
            p, append_standard_keywords={"meaning_of_life": ""}
        )
        with pytest.raises(pf.ParseKeyError):
            _ = pf.api.fcs_read_flat_text(p, append_standard_keywords={"": "notblank"})

    def test_sub_patterns(
        self, tmp_path: Path, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        dataset2_3_2.write_text(p)
        _ = pf.api.fcs_read_flat_text(
            p, substitute_standard_key_values={"history": ("viking", "pirate", True)}
        )
        _ = pf.api.fcs_read_flat_text(
            p,
            substitute_standard_key_values={
                "/religion?/": ("odin+thor", "cannons+other stuff", False)
            },
        )
        _ = pf.api.fcs_read_flat_text(
            p,
            substitute_standard_key_values={
                "time": ("(10[0-9]+)AD", "16${1}AD", False)
            },
        )
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_flat_text(
                p,
                substitute_standard_key_values={
                    "drone": ("Sunn O)))))", "refrigerator motor", False)
                },
            )
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_flat_text(
                p,
                substitute_standard_key_values={
                    "spiral": ("1.61", "the meaning of life is ${1}", False)
                },
            )

    def test_time_meas_pattern(
        self, tmp_path: Path, blank_dataset_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        blank_dataset_3_2.write_text(p)
        _ = pf.api.fcs_read_std_text(p, time_meas_pattern="")
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_std_text(p, time_meas_pattern=")))))")

    def test_ns_meas_pattern(
        self, tmp_path: Path, blank_dataset_3_2: pf.CoreDataset3_2
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        blank_dataset_3_2.write_text(p)
        _ = pf.api.fcs_read_std_text(p, nonstandard_measurement_pattern="%n")
        with pytest.raises(pf.ConfigError):
            _ = pf.api.fcs_read_std_text(p, nonstandard_measurement_pattern="")
            _ = pf.api.fcs_read_std_text(p, nonstandard_measurement_pattern="n")

    def test_int_byteord_override(
        self, tmp_path: Path, blank_dataset_2_0: pf.CoreDataset2_0
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "nonempty_dataset.fcs"
        blank_dataset_2_0.write_text(p)
        _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[1])
        with pytest.raises(pf.InvalidKeywordValueError):
            _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[])
            _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[1, 1])
            _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[666])


def mock_header_text(
    v: str,
    t0: int = 58,
    t1: int = 0,
    d0: int = 0,
    d1: int = 0,
    a0: int = 0,
    a1: int = 0,
    other_width: int = 8,
    other_segs: list[tuple[int, int]] = [],
    text: str = "",
) -> str:
    other = "".join(
        [str(x).rjust(other_width) + str(y).rjust(other_width) for (x, y) in other_segs]
    )
    return f"{v}    {t0:>8}{t1:>8}{d0:>8}{d1:>8}{a0:>8}{a1:>8}{other}/"


class TestConfig:
    @pytest.mark.parametrize("version", ["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"])
    @pytest.mark.parametrize(
        "other_segs, other_corrections",
        [
            [[], []],
            [[(0, -1)], [(0, 1)]],
            [[(0, -1)], [(0, 1), (0, 10000000)]],
        ],
    )
    def test_other_corrections(
        self,
        version: str,
        other_segs: Any,
        other_corrections: list[tuple[int, int]],
        tmp_path: Path,
    ) -> None:
        other_segs = list(other_segs)  # for some reason these come in as tuple
        other_corrections = list(other_corrections)
        t0 = len(other_segs) * 2 * 8 + 58
        s = mock_header_text(version, t0=t0, t1=t0, text="/", other_segs=other_segs)
        p = tmp_path / "thing.fcs"
        with open(p, "w") as f:
            f.write(s)
        out = pf.api.fcs_read_header(p, other_corrections=other_corrections)
        if len(other_segs) == 0:
            assert out.segments.other_segs is None
        else:
            print(other_segs)
            os_out, _ = out.segments.other_segs
            norm_corrections = [
                (other_corrections[i] if i < len(other_corrections) else (0, 0))
                for i, _ in enumerate(other_segs)
            ]

            assert os_out == [
                (x + a, y + b) for ((x, y), (a, b)) in zip(other_segs, norm_corrections)
            ]

    @pytest.mark.parametrize("version", ["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"])
    @pytest.mark.parametrize("max_other", [None, 0, 1, 5])
    @pytest.mark.parametrize(
        "other_segs",
        [
            [],
            [(0, 0), (0, 0), (0, 0)],
        ],
    )
    def test_max_other(
        self,
        version: str,
        max_other: int | None,
        other_segs: Any,
        tmp_path: Path,
    ) -> None:
        other_segs = list(other_segs)  # for some reason these come in as tuple
        t0 = len(other_segs) * 2 * 8 + 58
        s = mock_header_text(version, t0=t0, t1=t0, text="/", other_segs=other_segs)
        p = tmp_path / "thing.fcs"
        with open(p, "w") as f:
            f.write(s)
        out = pf.api.fcs_read_header(p, max_other=max_other)
        if max_other == 0 or len(other_segs) == 0:
            assert out.segments.other_segs is None
        elif max_other is None:
            os_out, _ = out.segments.other_segs
            assert os_out == other_segs
        else:
            os_out, _ = out.segments.other_segs
            assert os_out == other_segs[0:max_other]


class TestReadWrite:
    @staticmethod
    def _assert_uncore_text_empty(
        uncore: pf.api.StdTEXTOutput,
    ) -> None:
        assert uncore.flat_diagnostics.delimiter == 30
        assert len(uncore.flat_diagnostics.byte_pairs) == 0
        assert len(uncore.std_diagnostics.pseudostandard) == 0
        assert len(uncore.std_diagnostics.hyper_par) == 0
        assert len(uncore.std_diagnostics.other_version) == 0

    @staticmethod
    def _assert_uncore_dataset_empty(
        uncore: pf.api.StdDatasetOutput,
    ) -> None:
        assert uncore.flat_diagnostics.delimiter == 30
        assert len(uncore.flat_diagnostics.byte_pairs) == 0
        assert len(uncore.dataset.std_diagnostics.pseudostandard) == 0
        assert len(uncore.dataset.std_diagnostics.hyper_par) == 0
        assert len(uncore.dataset.std_diagnostics.other_version) == 0

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["blank_text"])
    def test_text_empty(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "empty_text.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p)
        self._assert_uncore_text_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text"])
    def test_text_non_empty_1(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "text1.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p, time_meas_pattern="NoTime")
        self._assert_uncore_text_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text2"])
    def test_text_non_empty_2(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "text2.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p, time_meas_pattern=LINK_NAME2)
        self._assert_uncore_text_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["blank_dataset"])
    def test_dataset_empty(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "empty_dataset.fcs"
        core.write_dataset(p)
        nu_core, un_core = pf.api.fcs_read_std_dataset(p)
        self._assert_uncore_dataset_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_non_empty_1(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset1.fcs"
        core.write_dataset(p)
        nu_core, un_core = pf.api.fcs_read_std_dataset(
            p, time_meas_pattern="NoTime", warnings_are_errors=True
        )
        self._assert_uncore_dataset_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    def test_dataset_non_empty_2(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset2.fcs"
        core.write_dataset(p)
        nu_core, un_core = pf.api.fcs_read_std_dataset(
            p,
            time_meas_pattern=LINK_NAME2,
            warnings_are_errors=True,
        )
        self._assert_uncore_dataset_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_truncated(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_trunc.fcs"
        core.data = pl.DataFrame([[0.5, 0.5]], {LINK_NAME1: pl.Float32})
        assert not isinstance(core.layout, pf.MixedLayout)
        assert core.layout.datatype == "I"
        # this will attempt to write a float as an int
        with pytest.RaisesGroup(pf.DataLossError):
            core.write_dataset(p)
        # this will force the float to int with a warning
        with pytest.warns(pf.PyreflowWarning):
            core.write_dataset(p, skip_conversion_check=True)

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_different_type(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_trunc.fcs"
        core.data = pl.DataFrame([[1.0, 1.0]], {LINK_NAME1: pl.Float32})
        assert not isinstance(core.layout, pf.MixedLayout)
        assert core.layout.datatype == "I"
        # this should convert 1.0 to 1 losslessly despite the underlying type
        # being U32
        core.write_dataset(p)

    @parameterize_versions("core0", ["2_0", "3_0", "3_1", "3_2"], ["text2"])
    def test_texts_non_empty(self, tmp_path: Path, core0: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "texts.fcs"
        core1 = deepcopy(core0)
        core2 = deepcopy(core0)
        core1.sys = "Windows i^2"
        core2.sys = "Windows 9"
        type(core0).write_texts(p, [core0, core1, core2])  # type: ignore
        datasets = pf.api.fcs_read_std_texts(p, time_meas_pattern=LINK_NAME2)
        assert len(datasets) == 3
        nu_core0, un_core0 = datasets[0]
        nu_core1, un_core1 = datasets[1]
        nu_core2, un_core2 = datasets[2]
        self._assert_uncore_text_empty(un_core0)
        self._assert_uncore_text_empty(un_core1)
        self._assert_uncore_text_empty(un_core2)
        assert core0 == nu_core0
        assert core1 == nu_core1
        assert core2 == nu_core2

    @parameterize_versions("core0", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    def test_datasets_non_empty(self, tmp_path: Path, core0: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "datasets.fcs"
        core1 = deepcopy(core0)
        core2 = deepcopy(core0)
        core1.sys = "Windows i^2"
        core2.sys = "Windows 9"
        type(core0).write_datasets(p, [core0, core1, core2])  # type: ignore
        datasets = pf.api.fcs_read_std_datasets(p, time_meas_pattern=LINK_NAME2)
        assert len(datasets) == 3
        nu_core0, un_core0 = datasets[0]
        nu_core1, un_core1 = datasets[1]
        nu_core2, un_core2 = datasets[2]
        self._assert_uncore_dataset_empty(un_core0)
        self._assert_uncore_dataset_empty(un_core1)
        self._assert_uncore_dataset_empty(un_core2)
        assert core0 == nu_core0
        assert core1 == nu_core1
        assert core2 == nu_core2
        smry = pf.api.fcs_summarize(p)
        assert len(smry) == 3

    @parameterize_versions("core0", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    def test_summarize_pd(self, tmp_path: Path, core0: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "datasets.fcs"
        core1 = deepcopy(core0)
        core2 = deepcopy(core0)
        core1.sys = "Windows i^2"
        core2.sys = "Windows 9"
        type(core0).write_datasets(p, [core0, core1, core2])  # type: ignore
        conf = pfp.PyreflowReadFlatDatasetConfig()
        smry = conf.summarize(p)
        assert len(smry) == 3

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    @pytest.mark.parametrize(
        "layout", [pf.FixedAsciiLayout([1000, 1000]), pf.DelimAsciiLayout([1000, 1000])]
    )
    def test_ascii(
        self,
        tmp_path: Path,
        core: AnyCoreDataset,
        layout: pf.FixedAsciiLayout | pf.DelimAsciiLayout,
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p0 = d / "dataset_ascii_wrong.fcs"
        p1 = d / "dataset_ascii_right.fcs"
        core.write_dataset(p0)
        core.layout = layout
        core.write_dataset(p1)
        new_core0, _ = pf.api.fcs_read_std_dataset(p0, time_meas_pattern=LINK_NAME2)
        if core.version in ["FCS3.1", "FCS3.2"]:
            with pytest.warns(pf.PyreflowWarning):
                new_core1, _ = pf.api.fcs_read_std_dataset(
                    p1, time_meas_pattern=LINK_NAME2
                )
        else:
            new_core1, _ = pf.api.fcs_read_std_dataset(p1, time_meas_pattern=LINK_NAME2)
        assert new_core0 != core
        assert new_core1 == core

    @parameterize_versions("core", ["2_0", "3_0"], ["dataset2"])
    # make sure we can store and read a totally scrambled byteord (note the
    # first byte in the middle to make it extra weird)
    @pytest.mark.parametrize(
        "byteord", ["little", "big", [1, 2, 3, 4], [4, 3, 2, 1], [2, 4, 1, 3]]
    )
    def test_mixed_byteord(
        self,
        tmp_path: Path,
        core: pf.CoreDataset2_0 | pf.CoreDataset3_0,
        byteord: ByteOrd,
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p0 = d / "dataset_mixed_wrong.fcs"
        p1 = d / "dataset_mixed_right.fcs"
        core.write_dataset(p0)
        core.layout = pf.OrderedUint32Layout([1023, 1023], byteord=byteord)
        core.write_dataset(p1)
        new_core0, _ = pf.api.fcs_read_std_dataset(p0, time_meas_pattern=LINK_NAME2)
        new_core1, _ = pf.api.fcs_read_std_dataset(p1, time_meas_pattern=LINK_NAME2)
        assert new_core0 != core
        assert new_core1 == core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_conversion(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_conversion.fcs"
        ser = pl.Series("blub", [1.5, 2.5, 3.5], dtype=pl.Float32)
        core.data = pl.DataFrame([ser])
        # this should fail because we are trying to write a non-integer float
        # as an integer
        with pytest.RaisesGroup(pf.PyreflowError):
            core.write_dataset(p)
        with pytest.warns(pf.PyreflowWarning):
            core.write_dataset(p, skip_conversion_check=True)

    @parameterize_versions("core", ["3_0", "3_1", "3_2"], ["dataset2"])
    def test_dataset_supp_text(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_supp_text.fcs"
        # store an absurdly large value in primary TEXT to force the file to
        # be written with STEXT
        k = "info_dump"
        v = "I am a puppet." * 7500000
        core.nonstandard_keywords = {k: v}
        core.write_dataset(p)
        nu_core, un_core = pf.api.fcs_read_std_dataset(
            p,
            time_meas_pattern=LINK_NAME2,
            warnings_are_errors=True,
        )
        self._assert_uncore_dataset_empty(un_core)
        assert core == nu_core
        # supp text should have non-zero offsets in new file
        assert un_core.flat_diagnostics.header_supp.supp_text is not None


HEADER_ALIASES = ["OffsetCorrection", "GuessOtherWidth"]

FLAT_TEXT_ALIASES = [
    "VersionOverride",
    "TriFlag",
    "DelimEscapeMode",
    "TrimValueWhitespace",
    "KeyPatterns",
    "SubPatterns",
]

STD_TEXT_ALIASES = [
    "ForceLinearScale",
    "TemporalOpticalKey",
    "ProcessTimeOpticalKeys",
    "ProcessKeywordFailure",
    "SpilloverMeasurementMode",
    "AllowHeaderTextOffsetMismatch",
    "ByteOrd",
]

READ_EVENTS_ALIASES = ["TruncateEventValues", "AllowHeaderTextOffsetMismatch"]

ALL_ALIASES = (
    HEADER_ALIASES + FLAT_TEXT_ALIASES + STD_TEXT_ALIASES + READ_EVENTS_ALIASES
)


class StubMismatch(NamedTuple):
    argname: str
    pydantic_type: str
    pyi_type: str
    pydantic_default: str
    pyi_default: str

    def __str__(self) -> str:
        return (
            f"pyi is '{self.pyi_type}' with default '{self.pyi_default}' "
            f"and pydantic is '{self.pydantic_type}' with default "
            f"'{self.pydantic_default}' for {self.argname}"
        )


# Ensure pydantic classes match their corresponding API functions. This is
# somewhat tested elsewhere since we call these API functions internally in
# the pydantic classes, but this will ensure that the types/defaults/names match
# exactly. This assumes that the stub file is totally correct. This is
# guaranteed to be true for the default value and names but not necessarily the
# type (until we get proper introspection)
class TestPydantic:
    @pytest.mark.parametrize(
        "pydantic_class, fun_name",
        [
            (pfp.PyreflowReadHeaderConfig, "fcs_read_header"),
            (pfp.PyreflowReadFlatTEXTConfig, "fcs_read_flat_text"),
            (pfp.PyreflowReadStdTEXTConfig, "fcs_read_std_text"),
            (pfp.PyreflowReadFlatDatasetConfig, "fcs_read_flat_dataset"),
            (pfp.PyreflowReadStdDatasetConfig, "fcs_read_std_dataset"),
            (
                pfp.PyreflowReadFlatDatasetFromKeywordsConfig,
                "fcs_read_flat_dataset_with_keywords",
            ),
        ],
    )
    def test_fun_sig_vs_pydantic(self, pydantic_class: type, fun_name: str) -> None:
        only_in_pyi = []
        unequal = []
        pydantic_seen = []

        # ignore kw args that are not supposed to be in pydantic classes
        ignore = ["dataset_offset"]

        # get dict of pydantic attrs and types
        sig = ins.signature(pydantic_class)
        sigmap = {x: (y.annotation, y.default) for x, y in sig.parameters.items()}

        # import types that we might need to resolve
        import pyreflow.typing as pft

        resolved = {a: getattr(pft, a) for a in ALL_ALIASES}

        # find function we want (and puke if we can't find it)
        node = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == fun_name
        )

        # loop through all function args with defaults, which should mirror what
        # pydantic has
        all_args = node.args.args
        all_defaults = node.args.defaults
        diff = len(all_args) - len(all_defaults)

        for arg, default in zip(all_args[diff:], all_defaults):
            pyi_default = ast.unparse(default)
            name = arg.arg
            if arg.annotation and name not in ignore:
                try:
                    (t, d) = sigmap[name]
                    pydantic_type = str(t)
                    # strings from AST are single quoted, so match here with
                    # pydantic strings
                    pydantic_default = f"'{d}'" if isinstance(d, str) else str(d)
                    pyi_type = str(ast.unparse(arg.annotation))
                    def_eq = pyi_default == pydantic_default

                    if pydantic_type == pyi_type and def_eq:
                        pydantic_seen.append(name)
                        continue

                    # if names match but types do not, it might be because the
                    # AST has a type alias. Try to resolve it with eval and try
                    # the comparison again. This obviously assumes the name is
                    # in scope.
                    resolved_pyi_type = str(eval(pyi_type, resolved))
                    pyi_type = resolved_pyi_type
                    if pydantic_type == resolved_pyi_type and def_eq:
                        pydantic_seen.append(name)
                        continue

                    unequal.append(
                        StubMismatch(
                            name,
                            pydantic_type,
                            pyi_type,
                            pydantic_default,
                            pyi_default,
                        )
                    )

                except KeyError:
                    # lookup failed, tell user we couldn't find the argname from
                    # the function in the pydantic class
                    only_in_pyi.append(arg.arg)

        assert len(only_in_pyi) == 0, f"only in .pyi: {', '.join(only_in_pyi)}"

        for u in unequal:
            assert False, str(u)

        only_in_pydantic = set(sigmap) - set(pydantic_seen)
        assert len(only_in_pydantic) == 0, (
            f"only in pydantic: {', '.join(only_in_pydantic)}"
        )

    # all all alternative configurations and make sure we didn't mix up any
    # dictionaries
    @pytest.mark.parametrize(
        "pydantic_class",
        [
            pfp.PyreflowReadHeaderConfig,
            pfp.PyreflowReadFlatTEXTConfig,
            pfp.PyreflowReadStdTEXTConfig,
            pfp.PyreflowReadFlatDatasetConfig,
            pfp.PyreflowReadStdDatasetConfig,
            pfp.PyreflowReadFlatDatasetFromKeywordsConfig,
        ],
    )
    @pytest.mark.parametrize("method", ["new_scalpal", "new_sledgehammer"])
    def test_alt_configs(self, pydantic_class: type, method: str) -> None:
        getattr(pydantic_class, method)()
