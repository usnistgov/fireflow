import numpy as np
import inspect as ins
from itertools import product, chain
from typing import cast, Any, NamedTuple, TypeVar, Callable
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from copy import deepcopy

import pytest

import pyreflow.typing as pt

from pyreflow.typing import (
    Segment,
    TriFlag,
    Trigger,
    MixedRange,
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

X = TypeVar("X")


INTEGER_WIDTHS: list[pt.VariableBitmask] = [
    ("U08", 1),
    ("U16", 2),
    ("U24", 3),
    ("U32", 4),
    ("U40", 5),
    ("U48", 6),
    ("U56", 7),
    ("U64", 8),
]


MIXED_SCHEMAS: list[tuple[pt.AnyType, pt.AnyDataSchema3_2]] = [
    ("F32", pf.BigLittleF32DataSchema([255, 255])),
    ("F64", pf.BigLittleF64DataSchema([255, 255])),
    ("U32", pf.SingleUintDataSchema([255, 255], byte_width=4)),
    ("A", pf.FixedAsciiDataSchema([255, 255])),
]

LINK_NAME1 = "wubbalubbadubdub"
LINK_NAME2 = "maple latte"
LINK_NAME3 = "silent man"

DTYPE = pl.UInt8 | pl.UInt16 | pl.UInt32 | pl.UInt64 | pl.Float32 | pl.Float64

# used for testing the pydantic model against the types in the pyi file
with open("python/pyreflow/_pyreflow.pyi") as f:
    tree = ast.parse(f.read())


@pytest.fixture
def blank_gated_meas() -> pf.GatedMeasurement:
    return pf.GatedMeasurement()


@pytest.fixture
def blank_text_2_0() -> pf.CoreTEXT2_0:
    return pf.CoreTEXT2_0([], pf.OrderedUintDataSchema([]))


@pytest.fixture
def blank_text_3_0() -> pf.CoreTEXT3_0:
    return pf.CoreTEXT3_0([], pf.OrderedUintDataSchema([]))


@pytest.fixture
def blank_text_3_1() -> pf.CoreTEXT3_1:
    return pf.CoreTEXT3_1([], pf.SingleUintDataSchema([]))


@pytest.fixture
def blank_text_3_2() -> pf.CoreTEXT3_2:
    return pf.CoreTEXT3_2([], pf.SingleUintDataSchema([]), "Moca Emporium")


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
    blank_dataset_2_0.push_optical(LINK_NAME1, blank_optical_2_0, 9001, series1)
    return blank_dataset_2_0


@pytest.fixture
def dataset_3_0(
    blank_dataset_3_0: pf.CoreDataset3_0,
    blank_optical_3_0: pf.Optical3_0,
    series1: pl.Series,
) -> pf.CoreDataset3_0:
    blank_dataset_3_0.push_optical(LINK_NAME1, blank_optical_3_0, 9001, series1)
    return blank_dataset_3_0


@pytest.fixture
def dataset_3_1(
    blank_dataset_3_1: pf.CoreDataset3_1,
    blank_optical_3_1: pf.Optical3_1,
    series1: pl.Series,
) -> pf.CoreDataset3_1:
    blank_dataset_3_1.push_optical(LINK_NAME1, blank_optical_3_1, 9001, series1)
    return blank_dataset_3_1


@pytest.fixture
def dataset_3_2(
    blank_dataset_3_2: pf.CoreDataset3_2,
    blank_optical_3_2: pf.Optical3_2,
    series1: pl.Series,
) -> pf.CoreDataset3_2:
    blank_dataset_3_2.push_optical(LINK_NAME1, blank_optical_3_2, 9001, series1)
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
    dataset_2_0.push_temporal(LINK_NAME2, blank_temporal_2_0, 9002, series2)
    return dataset_2_0


@pytest.fixture
def dataset2_3_0(
    dataset_3_0: pf.CoreDataset3_0,
    blank_temporal_3_0: pf.Temporal3_0,
    series2: pl.Series,
) -> pf.CoreDataset3_0:
    dataset_3_0.push_temporal(LINK_NAME2, blank_temporal_3_0, 9002, series2)
    return dataset_3_0


@pytest.fixture
def dataset2_3_1(
    dataset_3_1: pf.CoreDataset3_1,
    blank_temporal_3_1: pf.Temporal3_1,
    series2: pl.Series,
) -> pf.CoreDataset3_1:
    dataset_3_1.push_temporal(LINK_NAME2, blank_temporal_3_1, 9002, series2)
    return dataset_3_1


@pytest.fixture
def dataset2_3_2(
    dataset_3_2: pf.CoreDataset3_2,
    blank_temporal_3_2: pf.Temporal3_2,
    series2: pl.Series,
) -> pf.CoreDataset3_2:
    dataset_3_2.push_temporal(LINK_NAME2, blank_temporal_3_2, 9002, series2)
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
    dataset2_2_0.push_optical(LINK_NAME3, blank_optical_2_0, 9003, series3)
    return dataset2_2_0


@pytest.fixture
def dataset3_3_0(
    dataset2_3_0: pf.CoreDataset3_0,
    blank_optical_3_0: pf.Optical3_0,
    series3: pl.Series,
) -> pf.CoreDataset3_0:
    dataset2_3_0.push_optical(LINK_NAME3, blank_optical_3_0, 9003, series3)
    return dataset2_3_0


@pytest.fixture
def dataset3_3_1(
    dataset2_3_1: pf.CoreDataset3_1,
    blank_optical_3_1: pf.Optical3_1,
    series3: pl.Series,
) -> pf.CoreDataset3_1:
    dataset2_3_1.push_optical(LINK_NAME3, blank_optical_3_1, 9003, series3)
    return dataset2_3_1


@pytest.fixture
def dataset3_3_2(
    dataset2_3_2: pf.CoreDataset3_2,
    blank_optical_3_2: pf.Optical3_2,
    series3: pl.Series,
) -> pf.CoreDataset3_2:
    dataset2_3_2.push_optical(LINK_NAME3, blank_optical_3_2, 9003, series3)
    return dataset2_3_2


def parameterize_versions(
    arg: str, versions: list[str], targets: list[str]
) -> pytest.MarkDecorator:
    return pytest.mark.parametrize(
        arg,
        [lazy_fixture(f"{t}_{v}") for v in versions for t in targets],
    )


all_versions = pytest.mark.parametrize(
    "version", ["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]
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
        with pytest.raises(pf.ParseKeywordValueError):
            core.all_shortnames = ["____", ""]

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
            "$P1R": "9002",
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
            "$P1R": "9002",
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
            "$P1B": "32",
            "$P1N": LINK_NAME1,
            "$P1R": "9002",
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
            "$P1B": "32",
            "$P1N": LINK_NAME1,
            "$P1R": "9002",
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
    def test_meas_3_2_measurement_types(
        self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2
    ) -> None:
        new = "--- --"
        core.all_measurement_types == ["", False]
        core.all_measurement_types = [new, True]
        core.all_measurement_types == [new, True]
        with pytest.raises(TypeError):
            core.all_measurement_types = [10000000000000000000000, None]  # type: ignore
        with pytest.RaisesGroup(pf.RelationalError):
            # relational error because a string (optical) is being assigned to
            # the temporal index
            core.all_measurement_types = ["-.--.----..", "false"]

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
    def test_text_remove_uint_meas_by_name(
        self, core: AnyCoreTEXT, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        ret = core.remove_measurement_by_name(LINK_NAME1)
        assert ret[0] == 0
        assert isinstance(ret[1], optical)
        assert ret[2] == 9001
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
    def test_dataset_remove_uint_meas_by_name(
        self, core: AnyCoreDataset, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        ret = core.remove_measurement_by_name(LINK_NAME1)
        assert ret[0] == 0
        assert isinstance(ret[1], optical)
        assert ret[2].equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert ret[3] == 9001
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
    def test_text_remove_uint_meas_by_index(
        self, core: AnyCoreTEXT, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        ret = core.remove_measurement_by_index(0)
        assert ret[0] == LINK_NAME1
        assert isinstance(ret[1], optical)
        assert ret[2] == 9001
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
    def test_dataset_remove_uint_meas_by_index(
        self, core: AnyCoreDataset, optical: type
    ) -> None:
        assert len(core.measurements) == 1
        ret = core.remove_measurement_by_index(0)
        assert ret[0] == LINK_NAME1
        assert isinstance(ret[1], optical)
        assert ret[2].equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert ret[3] == 9001
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), o, s)
            for (c, o), s in chain(
                product(
                    [("text_2_0", pf.Optical2_0), ("text_3_0", pf.Optical3_0)],
                    [pf.OrderedF32DataSchema, pf.OrderedF32DataSchema],
                ),
                product(
                    [("text_3_1", pf.Optical3_1), ("text_3_2", pf.Optical3_2)],
                    [pf.BigLittleF32DataSchema, pf.BigLittleF32DataSchema],
                ),
            )
        ],
    )
    def test_text_remove_float_meas_by_name(
        self,
        core: AnyCoreTEXT,
        optical: type,
        data_schema: type,
    ) -> None:
        assert len(core.measurements) == 1
        core.data_schema = data_schema([1000.0])
        ret = core.remove_measurement_by_name(LINK_NAME1)
        assert ret[0] == 0
        assert isinstance(ret[1], optical)
        assert ret[2] == 1000.0
        assert len(core.measurements) == 0
        with pytest.raises(KeyError):
            core.remove_measurement_by_name(LINK_NAME1)

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), o, s)
            for (c, o), s in chain(
                product(
                    [("dataset_2_0", pf.Optical2_0), ("dataset_3_0", pf.Optical3_0)],
                    [pf.OrderedF32DataSchema, pf.OrderedF32DataSchema],
                ),
                product(
                    [("dataset_3_1", pf.Optical3_1), ("dataset_3_2", pf.Optical3_2)],
                    [pf.BigLittleF32DataSchema, pf.BigLittleF32DataSchema],
                ),
            )
        ],
    )
    def test_dataset_remove_float_meas_by_name(
        self,
        core: AnyCoreDataset,
        optical: type,
        data_schema: type,
    ) -> None:
        assert len(core.measurements) == 1
        core.data_schema = data_schema([1000.0])
        ret = core.remove_measurement_by_name(LINK_NAME1)
        assert ret[0] == 0
        assert isinstance(ret[1], optical)
        # NOTE this will test true even though the left side is f32/f64, the
        # numeric values are the same
        assert ret[2].equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert ret[3] == 1000.0
        assert len(core.measurements) == 0
        with pytest.raises(KeyError):
            core.remove_measurement_by_name(LINK_NAME1)

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), o, s)
            for c, o in [
                ("text_2_0", pf.Optical2_0),
                ("text_3_0", pf.Optical3_0),
                ("text_3_1", pf.Optical3_1),
                ("text_3_2", pf.Optical3_2),
            ]
            for s in [pf.FixedAsciiDataSchema, pf.DelimAsciiDataSchema]
        ],
    )
    def test_text_remove_ascii_meas_by_index(
        self,
        core: AnyCoreTEXT,
        optical: type,
        data_schema: type,
    ) -> None:
        assert len(core.measurements) == 1
        core.data_schema = data_schema([1000])
        ret = core.remove_measurement_by_index(0)
        assert ret[0] == LINK_NAME1
        assert isinstance(ret[1], optical)
        assert ret[2] == 1000
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), o, s)
            for c, o in [
                ("dataset_2_0", pf.Optical2_0),
                ("dataset_3_0", pf.Optical3_0),
                ("dataset_3_1", pf.Optical3_1),
                ("dataset_3_2", pf.Optical3_2),
            ]
            for s in [pf.FixedAsciiDataSchema, pf.DelimAsciiDataSchema]
        ],
    )
    def test_dataset_remove_ascii_meas_by_index(
        self,
        core: AnyCoreDataset,
        optical: type,
        data_schema: type,
    ) -> None:
        assert len(core.measurements) == 1
        core.data_schema = data_schema([1000])
        ret = core.remove_measurement_by_index(0)
        assert ret[0] == LINK_NAME1
        assert isinstance(ret[1], optical)
        assert ret[2].equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert ret[3] == 1000
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical, temporal",
        [
            (lazy_fixture(c), o, t)
            for c, o, t in [
                ("text2_3_1", pf.Optical3_1, pf.Temporal3_1),
                ("text2_3_2", pf.Optical3_2, pf.Temporal3_2),
            ]
        ],
    )
    def test_text_remove_var_uint_meas_by_index(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2,
        optical: type,
        temporal: type,
    ) -> None:
        assert len(core.measurements) == 2
        core.data_schema = pf.VariableUintDataSchema([("U16", 1000), ("U32", 2000)])
        n0, m0, r0, t0 = core.remove_measurement_by_index(0)
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        n1, m1, r1, t1 = core.remove_measurement_by_index(0)
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        assert n0 == LINK_NAME1
        assert n1 == LINK_NAME2
        assert isinstance(m0, optical)
        assert isinstance(m1, temporal)
        assert r0 == 1000
        assert r1 == 2000
        assert t0 == "U16"
        assert t1 is None
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    @pytest.mark.parametrize(
        "core, optical, temporal",
        [
            (lazy_fixture(c), o, t)
            for c, o, t in [
                ("dataset2_3_1", pf.Optical3_1, pf.Temporal3_1),
                ("dataset2_3_2", pf.Optical3_2, pf.Temporal3_2),
            ]
        ],
    )
    def test_dataset_remove_var_uint_meas_by_index(
        self,
        core: pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: type,
        temporal: type,
    ) -> None:
        assert len(core.measurements) == 2
        core.data_schema = pf.VariableUintDataSchema([("U16", 1000), ("U32", 2000)])
        n0, m0, c0, r0, t0 = core.remove_measurement_by_index(0)
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        n1, m1, c1, r1, t1 = core.remove_measurement_by_index(0)
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        assert n0 == LINK_NAME1
        assert n1 == LINK_NAME2
        assert isinstance(m0, optical)
        assert isinstance(m1, temporal)
        assert r0 == 1000
        assert r1 == 2000
        assert c0.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert c1.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert t0 == "U16"
        assert t1 is None
        with pytest.raises(IndexError):
            core.remove_measurement_by_index(0)

    def test_text_remove_mixed_meas_by_index(self, text2_3_2: pf.CoreTEXT3_2) -> None:
        assert len(text2_3_2.measurements) == 2
        text2_3_2.data_schema = pf.MixedDataSchema([("F32", 1000.0), ("U32", 2000)])
        n0, m0, r0, t0 = text2_3_2.remove_measurement_by_index(0)
        assert isinstance(text2_3_2.data_schema, pf.SingleUintDataSchema)
        n1, m1, r1, t1 = text2_3_2.remove_measurement_by_index(0)
        assert isinstance(text2_3_2.data_schema, pf.SingleUintDataSchema)
        assert n0 == LINK_NAME1
        assert n1 == LINK_NAME2
        assert isinstance(m0, pf.Optical3_2)
        assert isinstance(m1, pf.Temporal3_2)
        assert r0 == 1000.0
        assert r1 == 2000
        assert t0 == "F32"
        assert t1 is None
        with pytest.raises(IndexError):
            text2_3_2.remove_measurement_by_index(0)

    def test_dataset_remove_mixed_meas_by_index(
        self, dataset2_3_2: pf.CoreDataset3_2
    ) -> None:
        assert len(dataset2_3_2.measurements) == 2
        dataset2_3_2.data_schema = pf.MixedDataSchema([("F32", 1000.0), ("U32", 2000)])
        n0, m0, c0, r0, t0 = dataset2_3_2.remove_measurement_by_index(0)
        assert isinstance(dataset2_3_2.data_schema, pf.SingleUintDataSchema)
        n1, m1, c1, r1, t1 = dataset2_3_2.remove_measurement_by_index(0)
        assert isinstance(dataset2_3_2.data_schema, pf.SingleUintDataSchema)
        assert n0 == LINK_NAME1
        assert n1 == LINK_NAME2
        assert isinstance(m0, pf.Optical3_2)
        assert isinstance(m1, pf.Temporal3_2)
        assert r0 == 1000.0
        # TODO this is a weird side effect of normalization; if we remove all
        # but the last in a mixed layout, the layout will simplify to a
        # non-mixed layout which means we don't get the type. This probably
        # isn't what the user expects.
        assert r1 == 2000
        assert c0.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert c1.equals(pl.Series("unnamed", [1, 2, 3], dtype=pl.UInt32))
        assert t0 == "F32"
        assert t1 is None
        with pytest.raises(IndexError):
            dataset2_3_2.remove_measurement_by_index(0)

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
        "core, optical, data_schema, method",
        [
            (lazy_fixture(c), lazy_fixture(o), t, m)
            for core in ["text", "dataset"]
            for meas in ["optical", "temporal"]
            for c, o, t in [
                (f"blank_{core}_2_0", f"blank_{meas}_2_0", pf.OrderedUintDataSchema),
                (f"blank_{core}_3_0", f"blank_{meas}_3_0", pf.OrderedUintDataSchema),
                (f"blank_{core}_3_1", f"blank_{meas}_3_1", pf.SingleUintDataSchema),
                (f"blank_{core}_3_2", f"blank_{meas}_3_2", pf.SingleUintDataSchema),
            ]
            for m in [f"insert_{meas}", f"push_{meas}"]
        ],
    )
    def test_insert_decimal_int32(
        self,
        core: AnyCoreTEXT | AnyCoreDataset,
        optical: Any,
        data_schema: type,
        method: str,
        series1: pl.Series,
    ) -> None:
        """Check int32 schema insertion into text.

        Schema should not change when inserting a decimal range.

        Test all push/insert, dataset/text, and optical/temporal combinations.
        """
        assert isinstance(core.data_schema, data_schema)
        if "insert" in method:
            if isinstance(core, AnyCoreTEXT):
                getattr(core, method)(0, LINK_NAME1, optical, 100)
            else:
                getattr(core, method)(0, LINK_NAME1, optical, 100, series1)
        else:
            if isinstance(core, AnyCoreTEXT):
                getattr(core, method)(LINK_NAME1, optical, 100)
            else:
                getattr(core, method)(LINK_NAME1, optical, 100, series1)
        assert isinstance(core.measurement_at(0), type(optical))
        assert isinstance(core.data_schema, data_schema)

    # all tests above are on 32-bit integer layouts, make sure we can insert
    # into weirder layouts as well

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), lazy_fixture(o), t)
            for c, o, t in [
                ("dataset_2_0", "blank_optical_2_0", pf.OrderedF32DataSchema),
                ("dataset_3_0", "blank_optical_3_0", pf.OrderedF32DataSchema),
                ("dataset_3_1", "blank_optical_3_1", pf.BigLittleF32DataSchema),
                ("dataset_3_2", "blank_optical_3_2", pf.BigLittleF32DataSchema),
                ("dataset_2_0", "blank_optical_2_0", pf.OrderedF64DataSchema),
                ("dataset_3_0", "blank_optical_3_0", pf.OrderedF64DataSchema),
                ("dataset_3_1", "blank_optical_3_1", pf.BigLittleF64DataSchema),
                ("dataset_3_2", "blank_optical_3_2", pf.BigLittleF64DataSchema),
            ]
        ],
    )
    def test_insert_decimal_float(
        self,
        core: AnyCoreDataset,
        optical: Any,
        data_schema: type,
        series1: pl.Series,
    ) -> None:
        """Check float schema insertion.

        Schema should not change when inserting a decimal range.
        """
        core.data_schema = data_schema([9001.0])
        assert isinstance(core.data_schema, data_schema)
        core.insert_optical(0, LINK_NAME2, optical, 9001, series1)
        assert isinstance(core.measurement_at(1), type(optical))
        assert isinstance(core.data_schema, data_schema)

    @pytest.mark.parametrize(
        "core, optical, data_schema",
        [
            (lazy_fixture(c), lazy_fixture(o), t)
            for c, o in [
                ("dataset_2_0", "blank_optical_2_0"),
                ("dataset_3_0", "blank_optical_3_0"),
                ("dataset_3_1", "blank_optical_3_1"),
                ("dataset_3_2", "blank_optical_3_2"),
            ]
            for t in [pf.DelimAsciiDataSchema, pf.FixedAsciiDataSchema]
        ],
    )
    def test_insert_decimal_ascii(
        self,
        core: AnyCoreDataset,
        optical: Any,
        data_schema: type,
        series1: pl.Series,
    ) -> None:
        """Check ASCII schema insertion.

        Schema should not change when inserting a decimal range.
        """
        core.data_schema = data_schema([255])
        assert isinstance(core.data_schema, data_schema)
        core.insert_optical(0, LINK_NAME2, optical, 1, series1)
        assert isinstance(core.measurement_at(1), type(optical))
        assert isinstance(core.data_schema, data_schema)

    @pytest.mark.parametrize(
        "core, optical, byte_width, right_type, wrong_type",
        [
            (lazy_fixture(c), lazy_fixture(o), right_width, right_type, wrong_type)
            for c, o in [
                ("dataset2_3_1", "blank_optical_3_1"),
                ("dataset2_3_2", "blank_optical_3_2"),
            ]
            for (right_type, right_width) in INTEGER_WIDTHS
            for (wrong_type, _) in INTEGER_WIDTHS
            if not wrong_type == right_type
        ],
    )
    def test_insert_typed_single_uint(
        self,
        core: pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
        series1: pl.Series,
        byte_width: int,
        right_type: pt.IntegerWidth,
        wrong_type: pt.IntegerWidth,
    ) -> None:
        """Check typed insertion into single uint width schema.

        If type matches current schema, the width should be the same. If type
        does not match, the schema should change to variable.
        """
        core.data_schema = pf.SingleUintDataSchema([255, 255], byte_width=byte_width)
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        core.insert_optical(0, "iloveyou", optical, (right_type, 1), series1)
        assert isinstance(core.measurement_at(1), type(optical))
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        assert core.data_schema.byte_width == byte_width
        core.insert_optical(0, "eyehateu", optical, (wrong_type, 1), series1)
        assert isinstance(core.data_schema, pf.VariableUintDataSchema)

    @pytest.mark.parametrize(
        "schema",
        [
            pf.BigLittleF32DataSchema([255, 255]),
            pf.BigLittleF64DataSchema([255, 255]),
            pf.FixedAsciiDataSchema([255, 255]),
        ],
    )
    def test_insert_typed_single_uint_wrongtype(
        self,
        dataset2_3_1: pf.CoreDataset3_1,
        blank_optical_3_1: pf.Optical3_1,
        series1: pl.Series,
        schema: pf.BigLittleF32DataSchema
        | pf.BigLittleF64DataSchema
        | pf.FixedAsciiDataSchema,
    ) -> None:
        """Check typed insertion into non-uint width schema.

        These should error because the type is over-specified.
        """
        dataset2_3_1.data_schema = schema
        assert isinstance(dataset2_3_1.data_schema, type(schema))
        with pytest.RaisesGroup(pf.RelationalError):
            dataset2_3_1.insert_optical(
                0, "pegasus", blank_optical_3_1, ("U08", 1), series1
            )

    @pytest.mark.parametrize(
        "schema, right_type, wrong_type",
        [
            (right_schema, right_type, wrong_type)
            for (right_type, right_schema) in MIXED_SCHEMAS
            for (wrong_type, _) in MIXED_SCHEMAS
            if not wrong_type == right_type
        ],
    )
    def test_insert_typed_nonmixed(
        self,
        dataset2_3_2: pf.CoreDataset3_2,
        blank_optical_3_2: Any,
        series1: pl.Series,
        schema: pt.AnyDataSchema3_2,
        right_type: pt.AnyType,
        wrong_type: pt.AnyType,
    ) -> None:
        """Check typed insertion into single uint width schema.

        If type matches current schema, the width should be the same. If type
        does not match, the schema should change to variable.
        """
        dataset2_3_2.data_schema = schema
        assert isinstance(dataset2_3_2.data_schema, type(schema))
        # TODO fix typing
        dataset2_3_2.insert_optical(
            0,
            "sql slammer",
            blank_optical_3_2,
            (right_type, 1),  # type: ignore
            series1,
        )
        assert isinstance(dataset2_3_2.measurement_at(1), type(blank_optical_3_2))
        assert isinstance(dataset2_3_2.data_schema, type(schema))
        dataset2_3_2.insert_optical(
            0,
            "sql slammer (the sequel)",
            blank_optical_3_2,
            (wrong_type, 1),  # type: ignore
            series1,
        )
        assert isinstance(dataset2_3_2.data_schema, pf.MixedDataSchema)

    @pytest.mark.parametrize(
        "core, optical",
        [
            (lazy_fixture(c), lazy_fixture(o))
            for c, o in [
                ("dataset2_3_1", "blank_optical_3_1"),
                ("dataset2_3_2", "blank_optical_3_2"),
            ]
        ],
    )
    def test_insert_var_uint(
        self,
        core: pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
        series1: pl.Series,
    ) -> None:
        """Check variable uint schema insertion.

        Inserting a plain decimal (without type) should result in error.
        Inserting decimal with type should not change the schema.
        """
        core.data_schema = pf.VariableUintDataSchema([("U16", 10000), ("U32", 10000)])
        assert isinstance(core.data_schema, pf.VariableUintDataSchema)
        core.insert_optical(0, "wannacry", optical, ("U64", 10000), series1)
        assert isinstance(core.measurement_at(1), type(optical))
        assert isinstance(core.data_schema, pf.VariableUintDataSchema)
        with pytest.RaisesGroup(pf.RelationalError):
            core.insert_optical(0, "gonnacry", optical, 10000, series1)

    def test_insert_mixed(
        self,
        dataset2_3_2: pf.CoreDataset3_2,
        blank_optical_3_2: pf.Optical3_2,
        series1: pl.Series,
    ) -> None:
        """Check mixed schema insertion.

        Inserting a plain decimal (without type) should result in error.
        Inserting decimal with type should not change the schema.
        """
        dataset2_3_2.data_schema = pf.MixedDataSchema([("F32", 10000), ("U64", 10000)])
        assert isinstance(dataset2_3_2.data_schema, pf.MixedDataSchema)
        dataset2_3_2.insert_optical(
            0, "notpetya", blank_optical_3_2, ("U16", 10000), series1
        )
        assert isinstance(dataset2_3_2.measurement_at(1), pf.Optical3_2)
        assert isinstance(dataset2_3_2.data_schema, pf.MixedDataSchema)
        with pytest.RaisesGroup(pf.RelationalError):
            dataset2_3_2.insert_optical(
                0, "gotpetya", blank_optical_3_2, 10000, series1
            )

    @pytest.mark.parametrize(
        "dtype, width, should_err",
        [
            # U8
            (pl.UInt8, "U08", False),
            (pl.UInt16, "U08", True),
            (pl.UInt32, "U08", True),
            (pl.UInt64, "U08", True),
            (pl.Float32, "U08", True),
            (pl.Float64, "U08", True),
            # U16
            (pl.UInt8, "U16", False),
            (pl.UInt16, "U16", False),
            (pl.UInt32, "U16", True),
            (pl.UInt64, "U16", True),
            (pl.Float32, "U16", True),
            (pl.Float64, "U16", True),
            # U24
            (pl.UInt8, "U24", False),
            (pl.UInt16, "U24", False),
            (pl.UInt32, "U24", True),
            (pl.UInt64, "U24", True),
            (pl.Float32, "U24", True),
            (pl.Float64, "U24", True),
            # U32
            (pl.UInt8, "U32", False),
            (pl.UInt16, "U32", False),
            (pl.UInt32, "U32", False),
            (pl.UInt64, "U32", True),
            (pl.Float32, "U32", True),
            (pl.Float64, "U32", True),
            # U40
            (pl.UInt8, "U40", False),
            (pl.UInt16, "U40", False),
            (pl.UInt32, "U40", False),
            (pl.UInt64, "U40", True),
            (pl.Float32, "U40", True),
            (pl.Float64, "U40", True),
            # U48
            (pl.UInt8, "U48", False),
            (pl.UInt16, "U48", False),
            (pl.UInt32, "U48", False),
            (pl.UInt64, "U48", True),
            (pl.Float32, "U48", True),
            (pl.Float64, "U48", True),
            # U56
            (pl.UInt8, "U56", False),
            (pl.UInt16, "U56", False),
            (pl.UInt32, "U56", False),
            (pl.UInt64, "U56", True),
            (pl.Float32, "U56", True),
            (pl.Float64, "U56", True),
            # U64
            (pl.UInt8, "U64", False),
            (pl.UInt16, "U64", False),
            (pl.UInt32, "U64", False),
            (pl.UInt64, "U64", False),
            (pl.Float32, "U64", True),
            (pl.Float64, "U64", True),
            # F32
            (pl.UInt8, "F32", False),
            (pl.UInt16, "F32", False),
            # this is a lucky fluke; u32::MAX just happens to be an exact f32
            (pl.UInt32, "F32", False),
            # this is a lucky fluke; u64::MAX just happens to be an exact f32
            (pl.UInt64, "F32", False),
            (pl.Float32, "F32", False),
            (pl.Float64, "F32", True),
            # F64
            (pl.UInt8, "F64", False),
            (pl.UInt16, "F64", False),
            (pl.UInt32, "F64", False),
            # this is a lucky fluke; u64::MAX just happens to be an exact f64
            (pl.UInt64, "F64", False),
            (pl.Float32, "F64", False),
            (pl.Float64, "F64", False),
        ],
    )
    def test_series_to_int(
        self,
        blank_dataset_3_2: pf.CoreDataset3_2,
        blank_optical_3_2: pf.Optical3_2,
        dtype: DTYPE,
        width: pt.IntegerWidth,
        should_err: bool,
    ) -> None:
        # zero should insert cleanly for any datatype
        ser0 = pl.Series("unnamed", [0], dtype=dtype)
        blank_dataset_3_2.insert_optical(
            0, "duqu", blank_optical_3_2, (width, 100), ser0
        )

        upper: float | int

        if dtype == pl.UInt8:
            upper = 255
        elif dtype == pl.UInt16:
            upper = 2**16 - 1
        elif dtype == pl.UInt32:
            upper = 2**32 - 1
        elif dtype == pl.UInt64:
            upper = 2**64 - 1
        elif dtype == pl.Float32:
            upper = 3.4e38
        elif dtype == pl.Float64:
            upper = 1.79e308

        # inserting the max of a given datatype might clip depending on the
        # desired target type
        ser1 = pl.Series("unnamed", [upper], dtype=dtype)
        if should_err:
            with pytest.raises(pf.DataLossError):
                blank_dataset_3_2.insert_optical(
                    0, "zeus", blank_optical_3_2, (width, 100), ser1
                )
        else:
            blank_dataset_3_2.insert_optical(
                0, "zeus", blank_optical_3_2, (width, 100), ser1
            )

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text"])
    def test_check_ranges(
        self,
        core: AnyCoreTEXT,
    ) -> None:
        df1 = pl.DataFrame([pl.Series("unnamed", [1], dtype=pl.UInt32)])
        cd = core.to_dataset(df1)
        # should not error
        cd.check_ranges()

        df2 = pl.DataFrame([pl.Series("unnamed", [100000], dtype=pl.UInt32)])

        def go(
            c: pt.CheckedRangeDatatypes,
            a: pt.OverRangeAction,
            res: list[None | int],
            val: int,
        ) -> None:
            cd.data = df2
            assert (
                cd.check_ranges(checked_range_datatypes=c, over_range_action=a) == res
            )
            assert cd.data[0, 0] == val

        go("none", "error", [None], 100000)
        go("none", "warn", [None], 100000)
        go("none", "silent", [None], 100000)
        go("none", "trunc_warn", [None], 100000)
        go("none", "trunc_silent", [None], 100000)

        with pytest.RaisesGroup(pf.DataLossError):
            go("bitmask_only", "error", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("bitmask_only", "warn", [0], 100000)
        go("bitmask_only", "silent", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("bitmask_only", "trunc_warn", [0], 16383)
        go("bitmask_only", "trunc_silent", [0], 16383)

        with pytest.RaisesGroup(pf.DataLossError):
            go("int_only", "error", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("int_only", "warn", [0], 100000)
        go("int_only", "silent", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("int_only", "trunc_warn", [0], 9001)
        go("int_only", "trunc_silent", [0], 9001)

        with pytest.RaisesGroup(pf.DataLossError):
            go("all", "error", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("all", "warn", [0], 100000)
        go("all", "silent", [0], 100000)
        with pytest.warns(pf.PyreflowWarning):
            go("all", "trunc_warn", [0], 9001)
        go("all", "trunc_silent", [0], 9001)

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
    def test_ordered_data_schema(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert isinstance(core.data_schema, pf.OrderedUintDataSchema)
        core.data_schema = pf.OrderedUintDataSchema([9002, 9003])
        assert isinstance(core.data_schema, pf.OrderedUintDataSchema)
        with pytest.raises(TypeError):
            core.data_schema = pf.VariableUintDataSchema([9002, 9003], False)  # type: ignore

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_endian_data_schema(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert isinstance(core.data_schema, pf.SingleUintDataSchema)
        core.data_schema = pf.BigLittleF32DataSchema([9002, 9003])
        assert isinstance(core.data_schema, pf.BigLittleF32DataSchema)
        with pytest.raises(TypeError):
            core.data_schema = pf.OrderedUintDataSchema([9002, 9003])  # type: ignore

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
    def test_ordered_set_measurements_and_data_schema(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
        optical: Any,
    ) -> None:
        new = pf.OrderedUintDataSchema([1], byte_width=8)
        core.set_named_measurements_and_data_schema(
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
    def test_endian_set_measurements_and_data_schema(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
        optical: Any,
    ) -> None:
        new = pf.BigLittleF32DataSchema([1])
        core.set_named_measurements_and_data_schema(
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


class TestDataSchema:
    def test_ascii_fixed(self) -> None:
        ranges = [9, 99, 999]
        new = pf.FixedAsciiDataSchema(ranges)
        assert new.char_widths == [1, 2, 3]
        assert new.ranges == ranges
        assert new.datatype == "A"
        with pytest.raises(OverflowError):
            ranges = [1 * 10**20]
            new = pf.FixedAsciiDataSchema(ranges)

    def test_ascii_delim(self) -> None:
        ranges = [9, 99, 999]
        new = pf.DelimAsciiDataSchema(ranges)
        assert new.ranges == ranges
        assert new.datatype == "A"

    @pytest.mark.parametrize(
        "width",
        [8, 16, 24, 32, 40, 48, 56, 64],
    )
    def test_ordered_uint(self, width: int) -> None:
        n = int(width / 8)
        bitmasks = [2 ** (8 * (b + 1)) - 1 for b in range(n)]
        new = pf.OrderedUintDataSchema(bitmasks, byte_width=n)
        assert new.byteord == "little"
        assert new.byte_width == n
        assert new.ranges == [r for r in bitmasks]
        assert new.datatype == "I"
        with pytest.raises(pf.RelationalError if n < 8 else OverflowError):
            pf.OrderedUintDataSchema([2**width], byte_width=n)

    @pytest.mark.parametrize(
        "data_schema, width, datatype",
        [
            (pf.OrderedF32DataSchema, 32, "F"),
            (pf.OrderedF64DataSchema, 64, "D"),
            (pf.BigLittleF32DataSchema, 32, "F"),
            (pf.BigLittleF64DataSchema, 64, "D"),
        ],
    )
    def test_float(self, data_schema: type, width: int, datatype: Datatype) -> None:
        n = 3
        new = data_schema([1000.0] * n)
        assert new.byte_width == width / 8
        assert new.ranges == [1000.0] * n
        assert new.datatype == datatype
        with pytest.raises(ValueError):
            data_schema([float("inf")])

    def test_variable_uint(self) -> None:
        ranges: list[pt.VariableBitmask] = [
            ("U08", 2**8 - 1),
            ("U16", 2**16 - 1),
            ("U32", 2**24 - 1),
        ]
        new = pf.VariableUintDataSchema(ranges)
        assert new.byte_widths == [1, 2, 4]
        assert new.ranges == ranges
        assert new.datatype == "I"

    def test_mixed(self) -> None:
        types: list[MixedRange] = [
            ("F32", 1000.0),
            ("F64", 2000.0),
            ("U08", 255),
        ]
        new = pf.MixedDataSchema(types)
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
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
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
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
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
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
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
        with pytest.RaisesGroup(pf.RelationalError, pf.ExtraKeywordError):
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
        with pytest.raises(pf.ConfigError):
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
        with pytest.raises(pf.ParseKeyError):
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
        with pytest.raises(pf.ParseKeywordValueError):
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
        with pytest.raises(pf.ParseKeywordValueError):
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
        with pytest.raises(pf.ConfigError):
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
        with pytest.raises(pf.InvalidKeywordValueError):
            _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[1, 1])
        with pytest.raises(OverflowError):
            _ = pf.api.fcs_read_std_text(p, integer_byteord_override=[666])


class TestConfig:
    @staticmethod
    def mock_fcs_file(path: Path, xs: bytes) -> None:
        with open(path, "wb") as f:
            f.write(xs)

    @staticmethod
    def mock_header(
        path: Path,
        v: str,
        t: tuple[int, int] = (0, 0),
        d: tuple[int, int] = (0, 0),
        a: tuple[int, int] = (0, 0),
        other_width: int = 8,
        other_segs: list[tuple[int, int]] = [],
        rest: bytes = b"",
    ) -> None:
        def fmt_offset(pair: tuple[int, int], width: int) -> str:
            return str(pair[0]).rjust(width) + str(pair[1]).rjust(width)

        req = [fmt_offset(t, 8), fmt_offset(d, 8), fmt_offset(a, 8)]
        offsets = "".join(req + [fmt_offset(s, other_width) for s in other_segs])
        TestConfig.mock_fcs_file(path, bytes(v + "    " + offsets, "utf-8") + rest)

    @staticmethod
    def mock_header_text(
        path: Path,
        v: str,
        text_diff: tuple[int, int] = (0, 0),
        header_data: Segment = (0, 0),
        header_analysis: Segment = (0, 0),
        other_width: int = 8,
        other_segs: list[tuple[int, int]] = [],
        delim: int = 47,
        kws: dict[str, str] = {},
        stext: Segment | None = (0, 0),
        nextdata: int | None = 0,
        rest: bytes = b"",
    ) -> None:
        # avoid mutating the default value
        flat_kws = list(kws.items())
        assert delim < 256, "delim must be one byte"
        delim_byte = delim.to_bytes(1)
        if nextdata is not None:
            flat_kws.append(("$NEXTDATA", str(nextdata)))
        if stext is not None:
            flat_kws.append(("$BEGINSTEXT", str(stext[0])))
            flat_kws.append(("$ENDSTEXT", str(stext[1])))
        text = (
            delim_byte
            + delim_byte.join(
                [
                    bytes(x, "utf-8") + delim_byte + bytes(y, "utf-8")
                    for (x, y) in flat_kws
                ]
            )
            + delim_byte
        )
        all_rest = text + rest
        t0 = 58 + len(other_segs) * 2 * other_width
        t = (t0 + text_diff[0], t0 + len(text) - 1 + text_diff[1])
        return TestConfig.mock_header(
            path,
            v,
            t,
            header_data,
            header_analysis,
            other_width,
            other_segs,
            all_rest,
        )

    @staticmethod
    def mock_header_std_text(
        path: Path,
        v: str,
        text_diff: tuple[int, int] = (0, 0),
        header_data: Segment = (0, 0),
        header_analysis: Segment = (0, 0),
        other_width: int = 8,
        other_segs: list[tuple[int, int]] = [],
        delim: int = 47,
        kws: dict[str, str] = {},
        stext: Segment | None = (0, 0),
        text_data: Segment | None = (0, 0),
        text_analysis: Segment | None = (0, 0),
        nextdata: int | None = 0,
        par: int | None = 0,
        tot: int | None = 0,
        cyt: str | None = "Orbatron",
        mode: pt.Mode | None = "L",
        datatype: Datatype | None = "I",
        byteord: ByteOrd | None = [1, 2, 3, 4],
        rest: bytes = b"",
    ) -> None:
        _kws = {**kws}
        td = None if text_data is None or v == "FCS2.0" else text_data
        ta = None if text_analysis is None or v == "FCS2.0" else text_analysis
        st = None if stext is None or v == "FCS2.0" else stext
        if tot is not None:
            _kws["$TOT"] = str(tot)
        if par is not None:
            _kws["$PAR"] = str(par)
        if cyt is not None:
            _kws["$CYT"] = str(cyt)
        if mode is not None:
            _kws["$MODE"] = mode
        if datatype is not None:
            _kws["$DATATYPE"] = datatype
        if isinstance(byteord, list):
            _kws["$BYTEORD"] = ",".join(map(str, byteord))
        elif isinstance(byteord, str):
            _kws["$BYTEORD"] = byteord
        if td is not None:
            _kws["$BEGINDATA"] = str(td[0])
            _kws["$ENDDATA"] = str(td[1])
        if ta is not None:
            _kws["$BEGINANALYSIS"] = str(ta[0])
            _kws["$ENDANALYSIS"] = str(ta[1])
        return TestConfig.mock_header_text(
            path,
            v,
            text_diff,
            header_data,
            header_analysis,
            other_width,
            other_segs,
            delim,
            kws=_kws,
            stext=st,
            rest=rest,
        )

    @staticmethod
    def _test_tri_flag(f: Callable[[TriFlag], X], comp: X, err: list[type]) -> None:
        if len(err) == 0:
            assert f("false") == comp
            assert f("true") == comp
            assert f("silent") == comp
        else:
            with pytest.RaisesGroup(*err):
                f("false")

            with pytest.warns(pf.PyreflowWarning):
                assert f("true") == comp

            assert f("silent") == comp

    @staticmethod
    def _test_inverted_tri_flag(
        f: Callable[[TriFlag], X], comp: X, err: list[type]
    ) -> None:
        with pytest.warns(pf.PyreflowWarning):
            assert f("false") == comp

        with pytest.RaisesGroup(*err):
            f("true")

        assert f("silent") == comp

    @staticmethod
    def _test_config_flag(f: Callable[[bool], X], comp: X, err: list[type]) -> None:
        with pytest.RaisesGroup(*err):
            f(False)

        assert f(True) == comp

    @staticmethod
    def _test_tri_flag_nofail(f: Callable[[TriFlag], X], comp: X) -> None:
        assert f("false") == comp
        assert f("true") == comp
        assert f("silent") == comp

    @staticmethod
    def _test_config_flag_nofail(f: Callable[[bool], X], comp: X) -> None:
        assert f(False) == comp
        assert f(True) == comp

    @staticmethod
    def _test_process_kw_fail_flag(
        f: Callable[[pt.ProcessKeywordFailure], X],
        comp_demote: X,
        comp_drop: X,
        err: list[type],
    ) -> None:
        with pytest.RaisesGroup(*err):
            f("error")

        with pytest.warns(pf.PyreflowWarning):
            assert f("demote_warn") == comp_demote
        with pytest.warns(pf.PyreflowWarning):
            assert f("drop_warn") == comp_drop

        assert f("demote_silent") == comp_demote
        assert f("drop_silent") == comp_drop

    @staticmethod
    def _test_allow_header_text_offset_mismatch(
        path: Path, version: pt.FCSVersion, is_analysis: bool
    ) -> None:
        if version == "FCS2.0":
            header = (0, 0)
            # this should trigger an error if it is read at all since it starts
            # in the header
            text = (0, 1)
        else:
            header = (222, 225)
            text = (222, 224)
        if is_analysis:
            hd = (0, 0)
            td = (0, 0)
            ha = header
            ta = text
        else:
            hd = header
            td = text
            ha = (0, 0)
            ta = (0, 0)
        TestConfig.mock_header_std_text(
            path,
            version,
            header_data=hd,
            text_data=td,
            header_analysis=ha,
            text_analysis=ta,
            rest=b"\0\0\0\0",
        )

        def go(f: pt.AllowHeaderTextOffsetMismatch) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                path,
                allow_header_text_offset_mismatch=f,
                time_meas_pattern=None,
            )
            if is_analysis:
                return uncore.dataset_segs.analysis_seg
            else:
                return uncore.dataset_segs.data_seg

        if version == "FCS2.0":
            assert go("error") == header
            assert go("header_warn") == header
            assert go("header_silent") == header
            assert go("text_warn") == header
            assert go("text_silent") == header
        else:
            with pytest.RaisesGroup(pf.FileLayoutError):
                assert go("error") == header
            with pytest.warns(pf.PyreflowWarning):
                assert go("header_warn") == header
            with pytest.warns(pf.PyreflowWarning):
                assert go("text_warn") == text
            assert go("header_silent") == header
            assert go("text_silent") == text

    @all_versions
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
        version: pt.FCSVersion,
        other_segs: Any,
        other_corrections: list[tuple[int, int]],
        tmp_path: Path,
    ) -> None:
        other_segs = list(other_segs)  # for some reason these come in as tuple
        other_corrections = list(other_corrections)
        t0 = len(other_segs) * 2 * 8 + 58
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(t0, t0), rest=b"/", other_segs=other_segs)
        out = pf.api.fcs_read_header(p, other_corrections=other_corrections)
        if len(other_segs) == 0:
            assert out.segments.other_segs is None
        else:
            os_out, _ = out.segments.other_segs
            norm_corrections = [
                (other_corrections[i] if i < len(other_corrections) else (0, 0))
                for i, _ in enumerate(other_segs)
            ]

            assert os_out == [
                (x + a, y + b) for ((x, y), (a, b)) in zip(other_segs, norm_corrections)
            ]

    @all_versions
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
        version: pt.FCSVersion,
        max_other: int | None,
        other_segs: Any,
        tmp_path: Path,
    ) -> None:
        other_segs = list(other_segs)  # for some reason these come in as tuple
        t0 = len(other_segs) * 2 * 8 + 58
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(t0, t0), rest=b"/", other_segs=other_segs)
        out = pf.api.fcs_read_header(p, max_other=max_other)
        if max_other == 0 or len(other_segs) == 0:
            assert out.segments.other_segs is None
        elif max_other is None:
            os_out, _ = out.segments.other_segs
            assert os_out == other_segs
        else:
            os_out, _ = out.segments.other_segs
            assert os_out == other_segs[0:max_other]

    @all_versions
    @pytest.mark.parametrize("other_width", [8, 11, 13, 17, 20])
    def test_guess_other_width(
        self,
        version: pt.FCSVersion,
        other_width: int,
        tmp_path: Path,
    ) -> None:
        other_segs = [(0, 0), (0, 0)]
        t0 = len(other_segs) * 2 * other_width + 58
        p = tmp_path / "thing.fcs"
        self.mock_header(
            p,
            version,
            t=(t0, t0),
            rest=b"/",
            other_width=other_width,
            other_segs=other_segs,
        )

        # without guessing, all but default (which is 8) will emit exceptions
        # for every segment piece they try and fail to parse
        if other_width == 8:
            out = pf.api.fcs_read_header(p, guess_other_width="none")
            assert out.segments.other_segs[1] == other_width
        elif other_width == 11:
            with pytest.RaisesGroup(
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
            ):
                pf.api.fcs_read_header(p, guess_other_width="none")
        elif other_width in [13, 17, 20]:
            with pytest.RaisesGroup(
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
                pf.FileLayoutError,
            ):
                pf.api.fcs_read_header(p, guess_other_width="none")
        else:
            assert False, "unknown width"

        # none of these will emit warnings/errors since the guess succeeds
        out = pf.api.fcs_read_header(p, guess_other_width="error")
        assert out.segments.other_segs[1] == other_width

        out = pf.api.fcs_read_header(p, guess_other_width="warn")
        assert out.segments.other_segs[1] == other_width

        out = pf.api.fcs_read_header(p, guess_other_width="silent")
        assert out.segments.other_segs[1] == other_width

    @all_versions
    def test_squish_offsets(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, 58), d=(59, 0), rest=b"/")

        with pytest.RaisesGroup(pf.FileLayoutError):
            pf.api.fcs_read_header(p, squish_offsets=False)

        if version == "FCS2.0":
            # version 2.0 doesn't allow squishing
            with pytest.RaisesGroup(pf.FileLayoutError):
                pf.api.fcs_read_header(p, squish_offsets=True)
        else:
            out = pf.api.fcs_read_header(p, squish_offsets=True)
            assert out.segments.data_seg == (0, 0)

    @all_versions
    @pytest.mark.parametrize("data_end, analysis_end", [(0, -1), (-1, 0)])
    def test_allow_pseudoempty_req_header(
        self, version: pt.FCSVersion, data_end: int, analysis_end: int, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header(
            p, version, t=(58, 58), d=(0, data_end), a=(0, analysis_end), rest=b"/"
        )

        with pytest.RaisesGroup(pf.FileLayoutError):
            pf.api.fcs_read_header(p, allow_pseudoempty=False)

        out = pf.api.fcs_read_header(p, allow_pseudoempty=True)
        assert out.segments.data_seg == (0, 0)
        assert out.segments.analysis_seg == (0, 0)

    @all_versions
    @pytest.mark.parametrize("other_end", [0, -1])
    def test_allow_pseudoempty_other(
        self, version: pt.FCSVersion, other_end: int, tmp_path: Path
    ) -> None:
        t0 = 58 + 8 * 2
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(t0, t0), other_segs=[(0, other_end)], rest=b"/")

        if other_end == 0:
            out = pf.api.fcs_read_header(p, allow_pseudoempty=False)
            assert out.segments.other_segs[0][0] == (0, 0)
        else:
            with pytest.RaisesGroup(pf.FileLayoutError):
                pf.api.fcs_read_header(p, allow_pseudoempty=False)

        out = pf.api.fcs_read_header(p, allow_pseudoempty=True)
        assert out.segments.other_segs[0][0] == (0, 0)

    @all_versions
    def test_truncate_offset_limit(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, 59), rest=b"/")

        with pytest.RaisesGroup(pf.FileLayoutError):
            pf.api.fcs_read_header(p, truncate_offset_limit=0)

        out = pf.api.fcs_read_header(p, truncate_offset_limit=1)
        assert out.segments.text_seg == (58, 58)

    @all_versions
    def test_overlap_correction_limit(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, 59), d=(59, 62), rest=b"/data")

        with pytest.RaisesGroup(pf.FileLayoutError):
            pf.api.fcs_read_header(p, overlap_correction_limit=0)

        out = pf.api.fcs_read_header(p, overlap_correction_limit=1)
        assert out.segments.text_seg == (58, 58)
        assert out.segments.data_seg == (59, 62)

    # TODO test data_remainder_limit

    # TODO test version_override

    @all_versions
    def test_supp_text_correction(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, stext=(0, -1))

        def go(corr: tuple[int, int]) -> tuple[Segment | None, Segment] | None:
            out = pf.api.fcs_read_flat_text(p, supp_text_correction=corr)
            return out.flat_diagnostics.header_supp.supp_text

        if version == "FCS2.0":
            # 2.0 shouldn't parse supp text at all
            assert go((0, 0)) is None
            assert go((0, 1)) is None
        elif version == "FCS3.2":
            # supp text is optional for 3.2 so it emits warning
            with pytest.warns(pf.PyreflowWarning):
                assert go((0, 0)) is None
            assert go((0, 1)) == ((0, 0), (0, -1))
        else:
            with pytest.RaisesGroup(pf.FileLayoutError):
                go((0, 0))
            assert go((0, 1)) == ((0, 0), (0, -1))

    @all_versions
    def test_nextdata_correction(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, nextdata=-1)

        with pytest.RaisesGroup(pf.ParseKeywordValueError):
            pf.api.fcs_read_flat_text(p, nextdata_correction=0)

        pf.api.fcs_read_flat_text(p, nextdata_correction=1)

    @all_versions
    def test_allow_dup_supp_text_exact(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        # exactly equal to TEXT
        text_coords = (58, 98)
        self.mock_header_text(p, version, stext=text_coords)

        def go(f: TriFlag) -> tuple[Segment | None, Segment] | None:
            out = pf.api.fcs_read_flat_text(p, allow_duplicated_supp_text=f)
            return out.flat_diagnostics.header_supp.supp_text

        # no supp text in 2.0 so no error
        if version == "FCS2.0":
            self._test_tri_flag_nofail(go, None)
        else:
            comp: tuple[Segment | None, Segment] | None = (None, text_coords)
            self._test_tri_flag(go, comp, [pf.FileLayoutError])

            out = pf.api.fcs_read_flat_text(p, ignore_supp_text=True)
            assert out.flat_diagnostics.header_supp.supp_text == comp

    @all_versions
    def test_allow_dup_supp_text_other(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        # STEXT and OTHER are duplicated, keep STEXT
        p = tmp_path / "thing.fcs"
        stext_coords = (117, 163)
        stext = b"/This/is/what/it/sounds/like/when/devs/cry/.../"
        self.mock_header_text(
            p,
            version,
            other_segs=[stext_coords],
            stext=stext_coords,
            rest=stext,
        )

        Supp = tuple[Segment | None, Segment] | None
        Ret = tuple[Supp, list[Segment]]

        def go(f: TriFlag) -> Ret:
            out = pf.api.fcs_read_flat_text(p, allow_duplicated_supp_text=f)
            h = out.flat_diagnostics.header_supp
            o = h.header.segments.other_segs
            return (h.supp_text, None if o is None else o[0])

        # no supp text in 2.0 so no error
        if version == "FCS2.0":
            comp0: Ret = (None, [stext_coords])
            self._test_tri_flag_nofail(go, comp0)
        else:
            comp1: Ret = ((stext_coords, stext_coords), [(0, 0)])
            self._test_tri_flag(go, comp1, [pf.FileLayoutError])

            out = pf.api.fcs_read_flat_text(p, ignore_supp_text=True)
            assert out.flat_diagnostics.header_supp.supp_text == (None, stext_coords)
            assert (
                out.flat_diagnostics.header_supp.header.segments.other_segs is not None
            )
            assert out.flat_diagnostics.header_supp.header.segments.other_segs[0] == [
                stext_coords
            ]

    @all_versions
    def test_delim_escaped(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        # NOTE more cases are tested internally in rust, this is to ensure the
        # python api works as indended
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/aaa//bbb/bbb/ccc/ddd/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: pt.DelimEscapeMode, n_nonstd: int, n_blank: int) -> None:
            out = pf.api.fcs_read_flat_text(p, delim_escape_mode=f)
            assert len(out.kws.nonstd) == n_nonstd
            blank = out.flat_diagnostics.primary_split.keys_with_blank_values
            assert len(blank) == n_blank

        go("escaped", 2, 0)
        go("guess_escaped", 2, 1)
        go("guess_unescaped", 2, 1)
        go("unescaped", 2, 1)

    @all_versions
    def test_non_ascii_delim(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, delim=0)

        def go(f: TriFlag) -> int:
            out = pf.api.fcs_read_flat_text(p, allow_non_ascii_delim=f)
            return out.flat_diagnostics.primary_split.delimiter

        self._test_tri_flag(go, 0, [pf.FileLayoutError])

    # TODO repeat this for supp

    @all_versions
    def test_allow_non_unique(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/$NEXTDATA/666/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> list[tuple[str, str]]:
            out = pf.api.fcs_read_flat_text(p, allow_nonunique=f)
            return out.flat_diagnostics.non_unique_std_keywords

        self._test_tri_flag(go, [("$NEXTDATA", "666")], [pf.ParseKeyError])

    @all_versions
    def test_allow_non_unique_nonstd(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/slayer/42/slayer/420/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> list[tuple[str, str]]:
            out = pf.api.fcs_read_flat_text(p, allow_nonunique=f)
            return out.flat_diagnostics.non_unique_nonstd_keywords

        self._test_tri_flag(go, [("slayer", "420")], [pf.ParseKeyError])

    @all_versions
    @pytest.mark.parametrize(
        "text_end, allow_odd_token, allow_even_delim, comp, n_errors",
        [
            (b"", False, True, ("", True), 1),
            (b"/", False, False, ("", False), 0),
            (b"/xxx", True, False, ("xxx", False), 1),
            (b"/xxx/", True, True, ("xxx", True), 2),
        ],
    )
    @pytest.mark.parametrize("mode", ["escaped", "unescaped"])
    def test_text_end_extra(
        self,
        version: pt.FCSVersion,
        tmp_path: Path,
        text_end: bytes,
        allow_odd_token: bool,
        allow_even_delim: bool,
        comp: tuple[str | bytes, bool],
        n_errors: int,
        mode: pt.DelimEscapeMode,
    ) -> None:
        """Ensure that extra stuff at the end of TEXT is handled correctly.

        This will test two flags at once, allowing an odd number of tokens
        and allowing an even number of delimiters. Both flags are independent
        and control independent errors. Furthermore, each flag should do
        the same thing regardless of which delimiter escape mode is used.
        """
        # the extra a/b/c stuff at the end will prevent the parser from choosing
        # unescaped mode in all cases since otherwise there would be no
        # consecutive delimiters to deal with
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/a/b/c//d/e" + text_end
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> tuple[str | bytes, bool]:
            out = pf.api.fcs_read_flat_text(
                p,
                allow_odd_tokens=f if allow_odd_token else "false",
                allow_even_delims=f if allow_even_delim else "false",
                delim_escape_mode=mode,
            )
            token = out.flat_diagnostics.primary_split.last_odd_token
            delim = out.flat_diagnostics.primary_split.has_even_delims
            return (token, delim)

        self._test_tri_flag(go, comp, [pf.FileLayoutError] * n_errors)

    @all_versions
    def test_allow_empty_keys(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0//herman/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> list[str | bytes]:
            out = pf.api.fcs_read_flat_text(
                p, allow_empty_keys=f, delim_escape_mode="unescaped"
            )
            return out.flat_diagnostics.primary_split.values_with_blank_keys

        comp: list[str | bytes] = ["herman"]
        self._test_tri_flag(go, comp, [pf.FileLayoutError])

    @all_versions
    def test_allow_empty_pairs(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0///"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> int:
            out = pf.api.fcs_read_flat_text(
                p, allow_empty_keys=f, delim_escape_mode="unescaped"
            )
            return out.flat_diagnostics.primary_split.skipped_pairs

        self._test_tri_flag(go, 1, [pf.FileLayoutError])

    @all_versions
    @pytest.mark.parametrize(
        "text, comp",
        [
            (b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0///", (["0"], 0)),
            (b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA///0/", (["0"], 0)),
            (b"//$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/", ([], 1)),
            (b"///$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/", ([], 2)),
        ],
    )
    def test_allow_delim_at_boundary(
        self,
        version: pt.FCSVersion,
        tmp_path: Path,
        text: bytes,
        comp: tuple[list[str | bytes], int],
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> tuple[list[str | bytes], int]:
            out = pf.api.fcs_read_flat_text(
                p, allow_delim_at_boundary=f, delim_escape_mode="escaped"
            )
            tokens = out.flat_diagnostics.primary_split.tokens_with_boundary_delims
            leading = out.flat_diagnostics.primary_split.extra_leading_delims
            return (tokens, leading)

        comp0 = list(comp[0])
        fixed_comp = (comp0, comp[1])

        self._test_tri_flag(go, fixed_comp, [pf.FileLayoutError])

    @all_versions
    def test_use_latin1(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/tool/\xc6nima/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        with pytest.RaisesGroup(pf.ParseKeyError):
            pf.api.fcs_read_flat_text(p, use_latin1=False)

        out = pf.api.fcs_read_flat_text(p, use_latin1=True)
        out.kws.nonstd["tool"] == "Ænima"

    @all_versions
    def test_allow_non_ascii_keys(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/t\0\0l/Aenima/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> list[tuple[str | bytes, str | bytes]]:
            out = pf.api.fcs_read_flat_text(p, allow_non_ascii_keys=f)
            return out.flat_diagnostics.byte_pairs

        comp: list[tuple[str | bytes, str | bytes]] = [(b"t\0\0l", "Aenima")]
        self._test_tri_flag(go, comp, [pf.ParseKeyError])

    @all_versions
    def test_allow_non_utf8_values(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        text = b"/$BEGINSTEXT/0/$ENDSTEXT/0/$NEXTDATA/0/tool/\xc6nima/"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, len(text) + 57), rest=text)

        def go(f: TriFlag) -> list[tuple[str | bytes, str | bytes]]:
            out = pf.api.fcs_read_flat_text(p, allow_non_utf8_values=f)
            return out.flat_diagnostics.byte_pairs

        comp: list[tuple[str | bytes, str | bytes]] = [("tool", b"\xc6nima")]
        self._test_tri_flag(go, comp, [pf.ParseKeyError])

    @all_versions
    def test_allow_missing_supp_text(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, stext=None)

        def go(f: TriFlag) -> tuple[Segment | None, Segment] | None:
            out = pf.api.fcs_read_flat_text(p, allow_missing_supp_text=f)
            return out.flat_diagnostics.header_supp.supp_text

        if version in ["FCS2.0", "FCS3.2"]:
            # supp text doesn't exist in 2.0 and is optional in 3.2, so no
            # error for these two
            self._test_tri_flag_nofail(go, None)
        else:
            self._test_tri_flag(go, None, [pf.ParseKeywordValueError] * 2)

    @all_versions
    def test_allow_supp_text_own_delim(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        text = b"/$BEGINSTEXT/101/$ENDSTEXT/118/$NEXTDATA/0/\\microsoft\\ntfs:(\\"
        p = tmp_path / "thing.fcs"
        self.mock_header(p, version, t=(58, 100), rest=text)

        def go(f: TriFlag) -> tuple[dict[str, str], int, int | None]:
            out = pf.api.fcs_read_flat_text(p, allow_supp_text_own_delim=f)
            prim_delim = out.flat_diagnostics.primary_split.delimiter
            supp = out.flat_diagnostics.supp_split
            supp_delim = None if supp is None else supp.delimiter
            return (out.kws.nonstd, prim_delim, supp_delim)

        if version == "FCS2.0":
            comp0: tuple[dict[str, str], int, int | None] = ({}, 47, None)
            self._test_tri_flag_nofail(go, comp0)
        else:
            nstd = {"microsoft": "ntfs:("}
            comp1: tuple[dict[str, str], int, int | None] = (nstd, 47, 92)
            self._test_tri_flag(go, comp1, [pf.FileLayoutError])

    @all_versions
    def test_allow_missing_nextdata(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, nextdata=None)

        def go(f: TriFlag) -> int | None:
            out = pf.api.fcs_read_flat_text(p, allow_missing_nextdata=f)
            return out.flat_diagnostics.header_supp.nextdata

        self._test_tri_flag(go, None, [pf.ParseKeywordValueError])

    @all_versions
    def test_trim_value_whitespace(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$CYT": " "})

        def go(
            f: pt.TrimValueWhitespace,
        ) -> tuple[list[tuple[str | bytes, str | bytes]], list[str | bytes]]:
            out = pf.api.fcs_read_flat_text(p, trim_value_whitespace=f)
            trimmed = out.flat_diagnostics.keys_with_trimmed_values
            empty = out.flat_diagnostics.keys_with_empty_trimmed_values
            return (trimmed, empty)

        # no error if trimming isn't desired, blank $CYT is perfectly valid by
        # FCS standard (although not very smart)
        assert go("notrim") == ([], [])

        with pytest.RaisesGroup(pf.ParseKeyError):
            assert go("trim") == ([], ["$CYT"])

        with pytest.warns(pf.PyreflowWarning):
            assert go("trim_blank_warn") == ([], ["$CYT"])

        assert go("trim_blank_silent") == ([], ["$CYT"])

    @all_versions
    def test_ignore_standard_keys(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$CYT": "T1000"})

        out = pf.api.fcs_read_flat_text(p, ignore_standard_keys=["CYT"])
        assert out.flat_diagnostics.ignored_standard_keywords == [("$CYT", "T1000")]

    @all_versions
    def test_rename_standard_keys(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        pub = "eprint.iacr.org/2025/1237.pdf"
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$CYT": pub}, delim=10)

        out = pf.api.fcs_read_flat_text(p, rename_standard_keys={"CYT": "CITE"})
        assert out.kws.std["$CITE"] == pub

    @all_versions
    def test_promote_to_standard(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"PLUTO": "planet"})

        out = pf.api.fcs_read_flat_text(p, promote_to_standard=["PLUTO"])
        assert out.kws.std["$PLUTO"] == "planet"

    @all_versions
    def test_demote_from_standard(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$BLUETOOTH": "reliable"})

        out = pf.api.fcs_read_flat_text(p, demote_from_standard=["BLUETOOTH"])
        assert out.kws.nonstd["BLUETOOTH"] == "reliable"

    @all_versions
    def test_replace_std_key_vals(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$DARTH_VADER": "evil"})

        out = pf.api.fcs_read_flat_text(
            p, replace_standard_key_values={"DARTH_VADER": "misunderstood"}
        )
        assert out.kws.std["$DARTH_VADER"] == "misunderstood"

    @all_versions
    def test_append_std_kws(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version)

        out = pf.api.fcs_read_flat_text(p, append_standard_keywords={"CRAZY": "genius"})
        assert out.kws.std["$CRAZY"] == "genius"

    @all_versions
    def test_sub_standard_keys(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_text(p, version, kws={"$OP": "Megadeath"})

        out = pf.api.fcs_read_flat_text(
            p, substitute_standard_key_values={"OP": ("death", "deth", False)}
        )
        assert out.kws.std["$OP"] == "Megadeth"

    @all_versions
    def test_dedup_meas_names(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "poppy",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$P2N": "poppy",
            "$P2E": "0,0",
            "$P2B": "32",
            "$P2R": "32",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=2, tot=0)

        def go(f: bool) -> list[str]:
            core, _ = pf.api.fcs_read_std_text(
                p,
                dedup_measurement_names=f,
                time_meas_pattern=None,
            )
            return core.all_shortnames

        self._test_config_flag(go, ["poppy~0", "poppy~1"], [pf.RelationalError])

    @all_versions
    def test_trim_intra_value_whitespace(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        kws = {"$P1N": "BTC", "$P1E": "0, 0", "$P1B": "32", "$P1R": "32"}
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: bool) -> None:
            core, _ = pf.api.fcs_read_std_text(
                p,
                trim_intra_value_whitespace=f,
                time_meas_pattern=None,
            )

        self._test_config_flag(go, None, [pf.ParseKeywordValueError])

    @all_versions
    def test_time_meas_pattern(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {"$P1N": "T!ME", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        if version != "FCS2.0":
            kws["$TIMESTEP"] = "1.0"
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str | None) -> str | None:
            core, _ = pf.api.fcs_read_std_text(
                p,
                time_meas_pattern=f,
                process_extra_timestep="drop_silent",
            )
            t = core.temporal
            return None if t is None else t[1]

        with pytest.RaisesGroup(pf.RelationalError):
            assert go("TIME") == "TIME"
        assert go(None) is None
        assert go("T!ME") == "T!ME"

    @all_versions
    def test_allow_missing_time(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {"$P1N": "nottimeatall", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: TriFlag) -> bool:
            core, _ = pf.api.fcs_read_std_text(
                p,
                allow_missing_time=f,
            )
            return core.temporal is None

        self._test_tri_flag(go, True, [pf.RelationalError])

    @all_versions
    def test_add_missing_timestep(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {"$P1N": "TIME", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: float | None) -> tuple[float | None, bool]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                add_missing_timestep=f,
            )
            assert core.temporal is not None
            t = core.temporal[2]
            if isinstance(t, pf.Temporal2_0):
                return (None, uncore.std_diagnostics.timestep_added)
            else:
                return (t.timestep, uncore.std_diagnostics.timestep_added)

        if version == "FCS2.0":
            assert go(None) == (None, False)
            assert go(1.0) == (None, False)
        else:
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert go(None) == (None, False)
            assert go(1.0) == (1.0, True)

    @all_versions
    @pytest.mark.parametrize("datatype", ["A", "F", "D", "I"])
    def test_force_linear_scale(
        self, version: pt.FCSVersion, datatype: pt.Datatype, tmp_path: Path
    ) -> None:
        if datatype == "A":
            width = "10"
        elif datatype == "D":
            width = "64"
        else:
            width = "32"

        if datatype == "D" and version in ["FCS2.0", "FCS3.0"]:
            byteord = list(range(1, 9))
        else:
            byteord = list(range(1, 5))

        kws = {
            "$P1N": "dgx",
            "$P1E": "1,2",
            "$P1B": width,
            "$P1R": "32",
            "$P2N": "mac_mini",
            "$P2E": "1,3",
            "$P2B": width,
            "$P2R": "32",
            "$P3N": "TIME",
            "$P3E": "1,4",
            "$P3B": width,
            "$P3R": "32",
        }
        if version != "FCS2.0":
            kws["$TIMESTEP"] = "1.0"
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(
            p,
            version,
            kws=kws,
            par=3,
            tot=0,
            byteord=byteord,
            datatype=datatype,
        )

        Scales = list[tuple[float, float] | float]
        Diags = list[pt.MeasScaleDiagnostic]

        def go(f: pt.ForceLinearScale) -> tuple[Scales, Diags]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                force_linear_scale=f,
            )
            ss = (
                [
                    s
                    if isinstance(s, tuple)
                    and len(s) == 2
                    and isinstance(s[0], float)
                    and isinstance(s[1], float)
                    else (s if isinstance(s, float) else 1.0)
                    for s in core.all_scales
                ]
                if isinstance(core, pf.CoreTEXT2_0)
                else core.all_scale_transforms
            )
            ds = uncore.std_diagnostics.scale
            return (ss, ds)

        # All combinations should trigger one error when parsing TIME; this is
        # a keyword parsing error which precludes checking the other two
        # measurements in the case of non-int and F/D/A datatype since this only
        # happens after all keywords are successfully parsed and we have a full
        # layout to check
        with pytest.RaisesGroup(pf.ParseKeywordValueError):
            go("none")

        int_result = ([(1.0, 2.0), (1.0, 3.0), 1.0], [None, None, ("1,4", "forced")])
        non_int_result = (
            [1.0, 1.0, 1.0],
            [("1,2", "forced"), ("1,3", "forced"), ("1,4", "forced")],
        )

        if datatype == "I":
            # Simply fixing time for integer layouts should result in a valid
            # layout
            assert go("time_only") == int_result
        else:
            # For non-int layouts, the non-time measurement will trigger
            # relational errors (one for each measurement) since these datatype
            # do not allow log-scaling
            with pytest.RaisesGroup(
                pf.RelationalError, pf.RelationalError, flatten_subgroups=True
            ):
                assert go("time_only")

        if datatype == "I":
            # This should do the same thing as just fixing time above
            assert go("all_non_int") == int_result
        else:
            # This should result in valid layout by forcing all scales to linear
            assert go("all_non_int") == non_int_result

        # This should work in all cases; everything should be forced
        assert go("all") == non_int_result

    @all_versions
    def test_ignore_time_optical_keys(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        jiggawatt = "10000000000000000000000000000"
        kws = {
            "$P1N": "TIME",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$P1O": jiggawatt,
        }
        if version != "FCS2.0":
            kws["$TIMESTEP"] = "1.0"
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(
            f: list[pt.TemporalOpticalKey], g: pt.ProcessTimeOpticalKeys
        ) -> tuple[dict[str, str], dict[str, str]]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                ignore_time_optical_keys=f,
                process_time_optical_keys=g,
            )
            ps = uncore.std_diagnostics.pseudostandard
            ns = core.measurements[0].nonstandard_keywords
            return (ps, ns)

        with pytest.RaisesGroup(pf.RelationalError):
            # dummy assertions which should all fail at the error catch
            assert go([], "demote_warn") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go([], "demote_silent") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go([], "drop_warn") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go([], "drop_silent") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go(["L"], "demote_warn") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go(["L"], "demote_silent") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go(["L"], "drop_warn") == ({}, {})
        with pytest.RaisesGroup(pf.RelationalError):
            assert go(["L"], "drop_silent") == ({}, {})
        with pytest.warns(pf.PyreflowWarning):
            assert go(["O"], "demote_warn") == ({}, {"P1O": jiggawatt})
        with pytest.warns(pf.PyreflowWarning):
            assert go(["O"], "drop_warn") == ({}, {})
        go(["O"], "demote_silent") == ({}, {"$P1O": jiggawatt})
        go(["O"], "drop_silent") == ({}, {})

    @all_versions
    @pytest.mark.parametrize(
        "p1n, p2n, spillover, named_error, indexed_error",
        [
            ("x", "y", "2,x,y,0,0,0,0", False, True),
            ("x", "y", "2,1,2,0,0,0,0", True, False),
            ("1", "2", "2,1,2,0,0,0,0", False, False),
        ],
    )
    def test_spillover_meas_mode(
        self,
        version: pt.FCSVersion,
        spillover: str,
        p1n: str,
        p2n: str,
        named_error: bool,
        indexed_error: bool,
        tmp_path: Path,
    ) -> None:
        kws = {
            "$P1N": p1n,
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$P2N": p2n,
            "$P2E": "0,0",
            "$P2B": "32",
            "$P2R": "32",
            "$SPILLOVER": spillover,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=2, tot=0)

        def go(f: pt.SpilloverMeasurementMode) -> None:
            _, _ = pf.api.fcs_read_std_text(
                p,
                spillover_measurement_mode=f,
                time_meas_pattern=None,
            )

        if version in ["FCS2.0", "FCS3.0"]:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                go("named")
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                go("guess")
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                go("indexed")
        else:
            go("guess")
            if named_error:
                with pytest.RaisesGroup(pf.RelationalError):
                    go("named")
            else:
                go("named")
            if indexed_error:
                with pytest.RaisesGroup(pf.ParseKeywordValueError):
                    go("indexed")
            else:
                go("indexed")

    @all_versions
    def test_date_pattern(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$DATE": "01.19.2038",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str | None) -> bool:
            core, _ = pf.api.fcs_read_std_text(
                p,
                date_pattern=f,
                time_meas_pattern=None,
            )
            return core.date is not None

        with pytest.RaisesGroup(pf.ParseKeywordValueError):
            assert go(None)
        assert go("%m.%d.%Y")

    @all_versions
    def test_time_pattern(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$BTIM": "23_59_15",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str | None) -> bool:
            core, _ = pf.api.fcs_read_std_text(
                p,
                time_pattern=f,
                time_meas_pattern=None,
            )
            return core.btim is not None

        with pytest.RaisesGroup(pf.ParseKeywordValueError):
            assert go(None)
        assert go("%H_%M_%S")

    @all_versions
    def test_datetime_pattern(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$BEGINDATETIME": "2112_01_01_00_00_00.0+0001",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str | None) -> bool:
            core, _ = pf.api.fcs_read_std_text(
                p,
                datetime_pattern=f,
                time_meas_pattern=None,
            )
            if isinstance(core, pf.CoreTEXT3_2):
                return core.begindatetime is not None
            else:
                return False

        if version == "FCS3.2":
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert go(None)
            assert go("%Y_%m_%d_%H_%M_%S.%f%z")
        else:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go(None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("%Y_%m_%d_%H_%M_%S.%f%z")

    @all_versions
    def test_last_modified(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$LAST_MODIFIED": "2112_01_01_00_00_00",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str | None) -> bool:
            core, _ = pf.api.fcs_read_std_text(
                p,
                last_modified_pattern=f,
                time_meas_pattern=None,
            )
            if isinstance(core, pf.CoreTEXT3_2 | pf.CoreTEXT3_1):
                return core.last_modified is not None
            else:
                return False

        if version in ["FCS3.1", "FCS3.2"]:
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert go(None)
            assert go("%Y_%m_%d_%H_%M_%S")
        else:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go(None)
                assert go("%Y_%m_%d_%H_%M_%S")

    @all_versions
    def test_allow_other_feature(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        feat = "black_hole_density"
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$P1FEATURE": feat,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        Ret = tuple[str | tuple[()] | None, str | tuple[()] | None]

        def go(f: bool) -> Ret:
            core, _ = pf.api.fcs_read_std_text(
                p,
                allow_other_feature=f,
                time_meas_pattern=None,
            )
            if isinstance(core, pf.CoreTEXT3_2):
                return (core.all_awh_features[0], core.all_features[0])
            else:
                return (None, None)

        if version == "FCS3.2":
            comp: Ret = (None, feat)
            self._test_config_flag(go, comp, [pf.ParseKeywordValueError])
        else:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go(False) == (None, None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go(True) == (None, None)

    @all_versions
    def test_process_pseudostandard(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        val = "I mean, camaraderie"
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$COMMENT": val,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: pt.ProcessKeywordFailure) -> tuple[dict[str, str], dict[str, str]]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                process_pseudostandard=f,
                time_meas_pattern=None,
            )
            return (core.nonstandard_keywords, uncore.std_diagnostics.pseudostandard)

        self._test_process_kw_fail_flag(
            go,
            ({"COMMENT": val}, {}),
            ({}, {"$COMMENT": val}),
            [pf.ExtraKeywordError],
        )

    # TODO also test $GATE hyper_par

    @all_versions
    def test_process_hyper_par(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        val = "uae_sightings"
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$P2N": val,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: pt.ProcessKeywordFailure) -> tuple[dict[str, str], dict[str, str]]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                process_hyper_par=f,
                time_meas_pattern=None,
            )
            return (core.nonstandard_keywords, uncore.std_diagnostics.hyper_par)

        self._test_process_kw_fail_flag(
            go,
            ({"P2N": val}, {}),
            ({}, {"$P2N": val}),
            [pf.ExtraKeywordError],
        )

    @all_versions
    def test_process_other_version(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        val = "42,bla,blaa,blaaa"
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$UNICODE": val,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: pt.ProcessKeywordFailure) -> tuple[dict[str, str], dict[str, str]]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                process_other_version=f,
                time_meas_pattern=None,
            )
            return (core.nonstandard_keywords, uncore.std_diagnostics.other_version)

        if version != "FCS3.0":
            self._test_process_kw_fail_flag(
                go,
                ({"UNICODE": val}, {}),
                ({}, {"$UNICODE": val}),
                [pf.ExtraKeywordError],
            )
        else:
            assert go("error") == ({}, {})
            assert go("demote_warn") == ({}, {})
            assert go("demote_silent") == ({}, {})
            assert go("drop_warn") == ({}, {})
            assert go("drop_silent") == ({}, {})

    @all_versions
    def test_process_extra_timestep(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        val = "1.618033988749"
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$TIMESTEP": val,
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: pt.ProcessKeywordFailure) -> tuple[dict[str, str], None | str]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                process_extra_timestep=f,
                time_meas_pattern=None,
            )
            return (core.nonstandard_keywords, uncore.std_diagnostics.timestep)

        if version != "FCS2.0":
            self._test_process_kw_fail_flag(
                go,
                ({"TIMESTEP": val}, None),
                ({}, val),
                [pf.ExtraKeywordError],
            )
        else:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("error") == ({}, None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("demote_warn") == ({}, None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("demote_silent") == ({}, None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("drop_warn") == ({}, None)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert go("drop_silent") == ({}, None)

    @all_versions
    def test_fix_log_scale_offsets(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "2,0",
            "$P1B": "32",
            "$P1R": "32",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: bool) -> bool:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                fix_log_scale_offsets=f,
                time_meas_pattern=None,
            )
            return True

        self._test_config_flag(go, True, [pf.ParseKeywordValueError])

    @all_versions
    def test_disallow_localtime(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        kws = {
            "$P1N": "xyz",
            "$P1E": "0,0",
            "$P1B": "32",
            "$P1R": "32",
            "$BEGINDATETIME": "2112-01-01T00:00:00.0",
        }
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: bool) -> bool:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                disallow_localtime=f,
                time_meas_pattern=None,
            )
            return True

        if version == "FCS3.2":
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert not go(True)
            assert go(False)
        else:
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert not go(False)
            with pytest.RaisesGroup(pf.ExtraKeywordError):
                assert not go(True)

    @all_versions
    def test_non_std_meas_pat(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        extra = {"#P1LASER": "42pm"}
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "32", **extra}
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, kws=kws, par=1, tot=0)

        def go(f: str) -> tuple[dict[str, str], dict[str, str]]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                nonstandard_measurement_pattern=f,
                time_meas_pattern=None,
            )
            return (
                core.nonstandard_keywords,
                core.measurements[0].nonstandard_keywords,
            )

        assert go("P%n") == (extra, {})
        assert go("#P%n") == ({}, extra)
        assert go("/^#P%n/") == ({}, extra)

    @all_versions
    def test_text_data_correction(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_data=(0, -1))

        def go(f: tuple[int, int]) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                text_data_correction=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.data_seg

        if version == "FCS2.0":
            assert go((0, 0)) == (0, 0)
            assert go((0, 1)) == (0, 0)
        else:
            with pytest.RaisesGroup(pf.FileLayoutError):
                assert go((0, 0)) == (0, 0)
            assert go((0, 1)) == (0, 0)

    @all_versions
    def test_text_analysis_correction(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_analysis=(0, -1))

        def go(f: tuple[int, int]) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                text_analysis_correction=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.analysis_seg

        if version == "FCS2.0":
            assert go((0, 0)) == (0, 0)
            assert go((0, 1)) == (0, 0)
        elif version == "FCS3.2":
            # TODO shouldn't this be an error?
            with pytest.warns(pf.PyreflowWarning):
                assert go((0, 0)) == (0, 0)
            assert go((0, 1)) == (0, 0)
        else:
            with pytest.RaisesGroup(pf.FileLayoutError):
                assert go((0, 0)) == (0, 0)
            assert go((0, 1)) == (0, 0)

    @all_versions
    def test_ignore_text_data_offsets(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_data=(0, -1))

        def go(f: bool) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                ignore_text_data_offsets=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.data_seg

        if version == "FCS2.0":
            self._test_config_flag_nofail(go, (0, 0))
        else:
            self._test_config_flag(go, (0, 0), [pf.FileLayoutError])

    @all_versions
    def test_ignore_text_analysis_offsets(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_data=(0, 0), text_analysis=(0, -1))

        def go(f: bool) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                ignore_text_analysis_offsets=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.analysis_seg

        if version == "FCS2.0":
            self._test_config_flag_nofail(go, (0, 0))
        elif version == "FCS3.2":
            # TODO shouldn't this be an error?
            with pytest.warns(pf.PyreflowWarning):
                assert go(False) == (0, 0)
            assert go(True) == (0, 0)
        else:
            self._test_config_flag(go, (0, 0), [pf.FileLayoutError])

    @all_versions
    def test_allow_header_text_offset_mismatch_data(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"

        self._test_allow_header_text_offset_mismatch(p, version, False)

    @all_versions
    def test_allow_header_text_offset_mismatch_analysis(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"

        self._test_allow_header_text_offset_mismatch(p, version, True)

    @all_versions
    def test_allow_missing_required_offsets_data(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_data=None)

        def go(f: TriFlag) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                allow_missing_required_offsets=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.data_seg

        if version == "FCS2.0":
            assert go("false") == (0, 0)
            assert go("true") == (0, 0)
            assert go("silent") == (0, 0)
        else:
            with pytest.RaisesGroup(
                pf.ParseKeywordValueError, pf.ParseKeywordValueError
            ):
                assert go("false") == (0, 0)
            with pytest.warns(
                pf.PyreflowWarning, match="missing required key|could not obtain"
            ):
                assert go("true") == (0, 0)
            # TODO this is misleading because emitting a warning isn't exactly "silent"
            with pytest.warns(pf.PyreflowWarning, match="could not obtain"):
                assert go("silent") == (0, 0)

    @all_versions
    def test_allow_missing_required_offsets_analysis(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        self.mock_header_std_text(p, version, text_analysis=None)

        def go(f: TriFlag) -> Segment:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                allow_missing_required_offsets=f,
                time_meas_pattern=None,
            )
            return uncore.dataset_segs.analysis_seg

        if version in ["FCS2.0", "FCS3.2"]:
            assert go("false") == (0, 0)
            assert go("true") == (0, 0)
            assert go("silent") == (0, 0)
        else:
            with pytest.RaisesGroup(
                pf.ParseKeywordValueError, pf.ParseKeywordValueError
            ):
                assert go("false") == (0, 0)
            with pytest.warns(
                pf.PyreflowWarning, match="missing required key|could not obtain"
            ):
                assert go("true") == (0, 0)
            # TODO this is misleading because emitting a warning isn't exactly "silent"
            with pytest.warns(pf.PyreflowWarning, match="could not obtain"):
                assert go("silent") == (0, 0)

    @all_versions
    def test_process_optional_failure(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        val = "January Nine-teen twenty12"
        kws = {"$DATE": val}
        self.mock_header_std_text(p, version, kws=kws)

        def go(f: pt.ProcessKeywordFailure) -> dict[str, str]:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                process_optional_failure=f,
                time_meas_pattern=None,
            )
            return core.nonstandard_keywords

        self._test_process_kw_fail_flag(
            go, {"DATE": val}, {}, [pf.ParseKeywordValueError]
        )

    @all_versions
    def test_int_widths_from_byteord(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "24", "$P1R": "32"}
        self.mock_header_std_text(p, version, kws=kws, par=1)

        def go(f: bool) -> int:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                integer_widths_from_byteord=f,
                time_meas_pattern=None,
            )
            lt = core.data_schema
            if isinstance(lt, pf.OrderedUintDataSchema | pf.SingleUintDataSchema):
                return lt.byte_width
            else:
                assert False

        if version in ["FCS2.0", "FCS3.0"]:
            self._test_config_flag(go, 4, [pf.RelationalError])
        else:
            assert go(True) == 3
            assert go(False) == 3

    @all_versions
    def test_int_byteord_override(self, version: pt.FCSVersion, tmp_path: Path) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        self.mock_header_std_text(p, version, kws=kws, par=1, byteord=[1, 2, 3])

        def go(f: pt.ByteOrd | None) -> int:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                integer_byteord_override=f,
                time_meas_pattern=None,
            )
            lt = core.data_schema
            if isinstance(lt, pf.OrderedUintDataSchema):
                return lt.byte_width
            elif isinstance(lt, pf.VariableUintDataSchema):
                assert len(lt.byte_widths) == 1
                return lt.byte_widths[0]
            else:
                assert False

        if version in ["FCS2.0", "FCS3.0"]:
            with pytest.RaisesGroup(pf.RelationalError):
                assert go(None)
            assert go([1, 2, 3, 4])
        else:
            # this option does nothing for 3.1+ so these should just fail via
            # bad parse for $BYTEORD
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert go(None)
            with pytest.RaisesGroup(pf.ParseKeywordValueError):
                assert go([1, 2, 3, 4])

    @all_versions
    def test_disallow_range_truncation(
        self, version: pt.FCSVersion, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        val = "10000000000000000000000000000000000000000000000000000000000000000"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": val}
        self.mock_header_std_text(p, version, kws=kws, par=1)

        def go(f: TriFlag) -> bool:
            core, uncore = pf.api.fcs_read_std_text(
                p,
                disallow_range_truncation=f,
                time_meas_pattern=None,
            )
            # TODO return diagnostics for ranges that were trimmed
            return True

        self._test_inverted_tri_flag(go, True, [pf.RelationalError])

    @pytest.mark.parametrize(
        "version, data_seg",
        [
            ("FCS2.0", (170, 182)),
            ("FCS3.0", (256, 268)),
            ("FCS3.1", (256, 268)),
            ("FCS3.2", (256, 268)),
        ],
    )
    def test_allow_uneven_event_width(
        self, version: pt.FCSVersion, data_seg: Segment, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        tot = 3
        remainder = 1
        data = b"\0" * (4 * tot + remainder)
        self.mock_header_std_text(
            p,
            version,
            header_data=data_seg,
            text_data=data_seg,
            kws=kws,
            par=1,
            tot=tot,
            rest=data,
        )

        def go(f: TriFlag) -> int | None:
            out = pf.api.fcs_read_flat_dataset(p, allow_uneven_event_width=f)
            return out.dataset.events_diagnostics.event_data_remainder

        comp: int | None = remainder
        self._test_tri_flag(go, comp, [pf.FileLayoutError])

    @pytest.mark.parametrize(
        "version, data_seg",
        [
            ("FCS2.0", (170, 181)),
            ("FCS3.0", (256, 267)),
            ("FCS3.1", (256, 267)),
            ("FCS3.2", (256, 267)),
        ],
    )
    def test_allow_tot_mismatch(
        self, version: pt.FCSVersion, data_seg: Segment, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "32"}
        tot = 3
        data = b"\0" * (4 * tot)
        self.mock_header_std_text(
            p,
            version,
            header_data=data_seg,
            text_data=data_seg,
            kws=kws,
            par=1,
            tot=tot + 1,
            rest=data,
        )

        def go(f: TriFlag) -> bool | None:
            out = pf.api.fcs_read_flat_dataset(p, allow_tot_mismatch=f)
            return out.dataset.events_diagnostics.tot_event_mismatch

        comp: bool | None = True
        self._test_tri_flag(go, comp, [pf.FileLayoutError])

    @pytest.mark.parametrize(
        "version, data_seg",
        [
            ("FCS2.0", (170, 181)),
            ("FCS3.0", (256, 267)),
            ("FCS3.1", (256, 267)),
            ("FCS3.2", (256, 267)),
        ],
    )
    def test_truncate_range_datatypes_int(
        self, version: pt.FCSVersion, data_seg: Segment, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "16"}
        tot = 3
        data = b"\f" * (4 * tot)
        self.mock_header_std_text(
            p,
            version,
            header_data=data_seg,
            text_data=data_seg,
            kws=kws,
            par=1,
            tot=tot,
            rest=data,
        )

        def go(
            f: pt.CheckedRangeDatatypes,
            g: pt.OverRangeAction,
        ) -> list[tuple[int, bool] | None]:
            out = pf.api.fcs_read_flat_dataset(
                p,
                checked_range_datatypes=f,
                over_range_action=g,
            )
            return out.dataset.events_diagnostics.overrange_columns

        assert go("none", "warn") == [None]
        assert go("none", "error") == [None]
        assert go("none", "silent") == [None]
        assert go("none", "trunc_warn") == [None]
        assert go("none", "trunc_silent") == [None]

        with pytest.warns(pf.PyreflowWarning):
            assert go("bitmask_only", "warn") == [(0, False)]
        with pytest.RaisesGroup(pf.DataLossError, flatten_subgroups=True):
            assert go("bitmask_only", "error") == [(0, False)]
        assert go("bitmask_only", "silent") == [(0, False)]
        with pytest.warns(pf.PyreflowWarning):
            assert go("bitmask_only", "trunc_warn") == [(0, True)]
        assert go("bitmask_only", "trunc_silent") == [(0, True)]

        with pytest.warns(pf.PyreflowWarning):
            assert go("int_only", "warn") == [(0, False)]
        with pytest.RaisesGroup(pf.DataLossError, flatten_subgroups=True):
            assert go("int_only", "error") == [(0, False)]
        assert go("int_only", "silent") == [(0, False)]
        with pytest.warns(pf.PyreflowWarning):
            assert go("int_only", "trunc_warn") == [(0, True)]
        assert go("int_only", "trunc_silent") == [(0, True)]

        with pytest.warns(pf.PyreflowWarning):
            assert go("all", "warn") == [(0, False)]
        with pytest.RaisesGroup(pf.DataLossError, flatten_subgroups=True):
            assert go("all", "error") == [(0, False)]
        assert go("all", "silent") == [(0, False)]
        with pytest.warns(pf.PyreflowWarning):
            assert go("all", "trunc_warn") == [(0, True)]
        assert go("all", "trunc_silent") == [(0, True)]

    @pytest.mark.parametrize(
        "version, data_seg",
        [
            ("FCS2.0", (170, 181)),
            ("FCS3.0", (256, 267)),
            ("FCS3.1", (256, 267)),
            ("FCS3.2", (256, 267)),
        ],
    )
    def test_truncate_range_datatypes_float(
        self, version: pt.FCSVersion, data_seg: Segment, tmp_path: Path
    ) -> None:
        p = tmp_path / "thing.fcs"
        kws = {"$P1N": "xyz", "$P1E": "0,0", "$P1B": "32", "$P1R": "16"}
        tot = 3
        data = b"\x7f\x7f\xff\xff" * tot  # ~3.4e38 in big endian
        self.mock_header_std_text(
            p,
            version,
            header_data=data_seg,
            text_data=data_seg,
            kws=kws,
            byteord=[4, 3, 2, 1],
            datatype="F",
            par=1,
            tot=tot,
            rest=data,
        )

        def go(
            f: pt.CheckedRangeDatatypes, g: pt.OverRangeAction
        ) -> list[tuple[int, bool] | None]:
            out = pf.api.fcs_read_flat_dataset(
                p,
                checked_range_datatypes=f,
                over_range_action=g,
            )
            return out.dataset.events_diagnostics.overrange_columns

        assert go("none", "warn") == [None]
        assert go("none", "error") == [None]
        assert go("none", "silent") == [None]
        assert go("none", "trunc_warn") == [None]
        assert go("none", "trunc_silent") == [None]

        assert go("bitmask_only", "warn") == [None]
        assert go("bitmask_only", "error") == [None]
        assert go("bitmask_only", "silent") == [None]
        assert go("bitmask_only", "trunc_warn") == [None]
        assert go("bitmask_only", "trunc_silent") == [None]

        assert go("int_only", "warn") == [None]
        assert go("int_only", "error") == [None]
        assert go("int_only", "silent") == [None]
        assert go("int_only", "trunc_warn") == [None]
        assert go("int_only", "trunc_silent") == [None]

        with pytest.warns(pf.PyreflowWarning):
            assert go("all", "warn") == [(0, False)]
        with pytest.RaisesGroup(pf.DataLossError, flatten_subgroups=True):
            assert go("all", "error") == [(0, False)]
        assert go("all", "silent") == [(0, False)]
        with pytest.warns(pf.PyreflowWarning):
            assert go("all", "trunc_warn") == [(0, True)]
        assert go("all", "trunc_silent") == [(0, True)]


class TestReadWrite:
    @staticmethod
    def _assert_uncore_text_empty(
        uncore: pf.api.StdTEXTOutput,
    ) -> None:
        assert uncore.flat_diagnostics.primary_split.delimiter == 30
        assert len(uncore.flat_diagnostics.byte_pairs) == 0
        assert len(uncore.std_diagnostics.pseudostandard) == 0
        assert len(uncore.std_diagnostics.hyper_par) == 0
        assert len(uncore.std_diagnostics.other_version) == 0

    @staticmethod
    def _assert_uncore_dataset_empty(
        uncore: pf.api.StdDatasetOutput,
    ) -> None:
        assert uncore.flat_diagnostics.primary_split.delimiter == 30
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
        nu_core, un_core = pf.api.fcs_read_std_text(p, time_meas_pattern=None)
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
            p, time_meas_pattern=None, warnings_are_errors=True
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

    # @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    # def test_dataset_truncated(self, tmp_path: Path, core: AnyCoreDataset) -> None:
    #     d = tmp_path
    #     d.mkdir(exist_ok=True)
    #     p = d / "dataset_trunc.fcs"
    #     core.data = pl.DataFrame([[0.5, 0.5]], {LINK_NAME1: pl.Float32})
    #     assert not isinstance(core.data_schema, pf.MixedDataSchema)
    #     assert core.data_schema.datatype == "I"
    #     # this will attempt to write a float as an int
    #     with pytest.RaisesGroup(pf.DataLossError):
    #         core.write_dataset(p)
    #     # this will force the float to int with a warning
    #     with pytest.warns(pf.PyreflowWarning):
    #         core.write_dataset(p)

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_different_type(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_trunc.fcs"
        core.data = pl.DataFrame([[1.0, 1.0]], {LINK_NAME1: pl.Float32})
        assert not isinstance(core.data_schema, pf.MixedDataSchema)
        assert core.data_schema.datatype == "I"
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
        pf.api.fcs_write_datasets(p, [core0, core1, core2])  # type: ignore
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
        pf.api.fcs_write_datasets(p, [core0, core1, core2])  # type: ignore
        conf = pfp.PyreflowReadFlatDatasetConfig()
        smry = conf.summarize(p)
        assert len(smry) == 3

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset2"])
    @pytest.mark.parametrize(
        "data_schema",
        [pf.FixedAsciiDataSchema([1000, 1000]), pf.DelimAsciiDataSchema([1000, 1000])],
    )
    def test_ascii(
        self,
        tmp_path: Path,
        core: AnyCoreDataset,
        data_schema: pf.FixedAsciiDataSchema | pf.DelimAsciiDataSchema,
    ) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p0 = d / "dataset_ascii_wrong.fcs"
        p1 = d / "dataset_ascii_right.fcs"
        core.write_dataset(p0)
        core.data_schema = data_schema
        core.write_dataset(p1)
        new_core0, _ = pf.api.fcs_read_std_dataset(p0, time_meas_pattern=LINK_NAME2)
        if core.version in ["FCS3.1", "FCS3.2"]:
            new_core1, _ = pf.api.fcs_read_std_dataset(p1, time_meas_pattern=LINK_NAME2)
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
        core.data_schema = pf.OrderedUintDataSchema([1023, 1023], byteord=byteord)
        core.write_dataset(p1)
        new_core0, _ = pf.api.fcs_read_std_dataset(p0, time_meas_pattern=LINK_NAME2)
        new_core1, _ = pf.api.fcs_read_std_dataset(p1, time_meas_pattern=LINK_NAME2)
        assert new_core0.data.equals(core.data)
        assert new_core1.data.equals(core.data)

    #     @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    #     def test_dataset_conversion(self, tmp_path: Path, core: AnyCoreDataset) -> None:
    #         assert False, "FIXME"
    #         # d = tmp_path
    #         # d.mkdir(exist_ok=True)
    #         # p = d / "dataset_conversion.fcs"
    #         # ser = pl.Series("blub", [1.5, 2.5, 3.5], dtype=pl.Float32)
    #         # core.data = pl.DataFrame([ser])
    #         # # this should fail because we are trying to write a non-integer float
    #         # # as an integer
    #         # with pytest.RaisesGroup(pf.PyreflowError):
    #         #     core.write_dataset(p)
    #         # with pytest.warns(pf.PyreflowWarning):
    #         #     core.write_dataset(p, skip_conversion_check=True)

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

        only_in_pyi = []
        unequal = []
        pydantic_seen = []

        # ignore kw args that are not supposed to be in pydantic classes
        ignore = ["dataset_offset"]

        # get dict of pydantic attrs and types
        sig = ins.signature(pydantic_class)
        sigmap = {x: (y.annotation, y.default) for x, y in sig.parameters.items()}

        # Import types that we might need to resolve. ASSUME that all the type
        # annotations in the stub file are prefixed with 'pft'. The alternative
        # is to resolve all names individually, which is a total pain
        import pyreflow.typing as pft

        resolved = {"pft": pft}

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
