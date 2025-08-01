from typing import cast
from datetime import date, datetime, time

import pytest

from pyreflow.typing import Trigger, AnyCoreTEXT, AnyCoreDataset, AnyOptical, AnyCore
import pyreflow as pf
import polars as pl

from .conftest import lazy_fixture


@pytest.fixture
def blank_text_2_0() -> pf.CoreTEXT2_0:
    return pf.CoreTEXT2_0("L", "I")


@pytest.fixture
def blank_text_3_0() -> pf.CoreTEXT3_0:
    return pf.CoreTEXT3_0("L", "I")


@pytest.fixture
def blank_text_3_1() -> pf.CoreTEXT3_1:
    return pf.CoreTEXT3_1("L", "I")


@pytest.fixture
def blank_text_3_2() -> pf.CoreTEXT3_2:
    return pf.CoreTEXT3_2("Moca Emporium", "I")


@pytest.fixture
def blank_dataset_2_0(blank_text_2_0: pf.CoreTEXT2_0) -> pf.CoreDataset2_0:
    return blank_text_2_0.to_dataset([], b"", [])


@pytest.fixture
def blank_dataset_3_0(blank_text_3_0: pf.CoreTEXT3_0) -> pf.CoreDataset3_0:
    return blank_text_3_0.to_dataset([], b"", [])


@pytest.fixture
def blank_dataset_3_1(blank_text_3_1: pf.CoreTEXT3_1) -> pf.CoreDataset3_1:
    return blank_text_3_1.to_dataset([], b"", [])


@pytest.fixture
def blank_dataset_3_2(blank_text_3_2: pf.CoreTEXT3_2) -> pf.CoreDataset3_2:
    return blank_text_3_2.to_dataset([], b"", [])


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
    return pf.Temporal3_0(1.0)


@pytest.fixture
def blank_temporal_3_1() -> pf.Temporal3_1:
    return pf.Temporal3_1(1.0)


@pytest.fixture
def blank_temporal_3_2() -> pf.Temporal3_2:
    return pf.Temporal3_2(1.0)


LINK_NAME1 = "wubbalubbadubdub"
LINK_NAME2 = "maple latte"


@pytest.fixture
def series1() -> pl.Series:
    return pl.Series("blub", [1, 2, 3], dtype=pl.UInt64)


@pytest.fixture
def series2() -> pl.Series:
    return pl.Series("blubby", [1, 2, 3], dtype=pl.UInt64)


@pytest.fixture
def text_2_0(
    blank_text_2_0: pf.CoreTEXT2_0, blank_optical_2_0: pf.Optical2_0
) -> pf.CoreTEXT2_0:
    blank_text_2_0.push_optical(blank_optical_2_0, LINK_NAME1, 9001)
    return blank_text_2_0


@pytest.fixture
def text_3_0(
    blank_text_3_0: pf.CoreTEXT3_0, blank_optical_3_0: pf.Optical3_0
) -> pf.CoreTEXT3_0:
    blank_text_3_0.push_optical(blank_optical_3_0, LINK_NAME1, 9001)
    return blank_text_3_0


@pytest.fixture
def text_3_1(
    blank_text_3_1: pf.CoreTEXT3_1, blank_optical_3_1: pf.Optical3_1
) -> pf.CoreTEXT3_1:
    blank_text_3_1.push_optical(blank_optical_3_1, LINK_NAME1, 9001)
    return blank_text_3_1


@pytest.fixture
def text_3_2(
    blank_text_3_2: pf.CoreTEXT3_2, blank_optical_3_2: pf.Optical3_2
) -> pf.CoreTEXT3_2:
    blank_text_3_2.push_optical(blank_optical_3_2, LINK_NAME1, 9001)
    return blank_text_3_2


@pytest.fixture
def dataset_2_0(
    blank_dataset_2_0: pf.CoreDataset2_0,
    blank_optical_2_0: pf.Optical2_0,
    series1: pl.Series,
) -> pf.CoreDataset2_0:
    blank_dataset_2_0.push_optical(blank_optical_2_0, series1, LINK_NAME1, 9001)
    return blank_dataset_2_0


@pytest.fixture
def dataset_3_0(
    blank_dataset_3_0: pf.CoreDataset3_0,
    blank_optical_3_0: pf.Optical3_0,
    series1: pl.Series,
) -> pf.CoreDataset3_0:
    blank_dataset_3_0.push_optical(blank_optical_3_0, series1, LINK_NAME1, 9001)
    return blank_dataset_3_0


@pytest.fixture
def dataset_3_1(
    blank_dataset_3_1: pf.CoreDataset3_1,
    blank_optical_3_1: pf.Optical3_1,
    series1: pl.Series,
) -> pf.CoreDataset3_1:
    blank_dataset_3_1.push_optical(blank_optical_3_1, series1, LINK_NAME1, 9001)
    return blank_dataset_3_1


@pytest.fixture
def dataset_3_2(
    blank_dataset_3_2: pf.CoreDataset3_2,
    blank_optical_3_2: pf.Optical3_2,
    series1: pl.Series,
) -> pf.CoreDataset3_2:
    blank_dataset_3_2.push_optical(blank_optical_3_2, series1, LINK_NAME1, 9001)
    return blank_dataset_3_2


@pytest.fixture
def text2_2_0(
    text_2_0: pf.CoreTEXT2_0, blank_temporal_2_0: pf.Temporal2_0
) -> pf.CoreTEXT2_0:
    text_2_0.push_temporal(blank_temporal_2_0, LINK_NAME2, 9001)
    return text_2_0


@pytest.fixture
def text2_3_0(
    text_3_0: pf.CoreTEXT3_0, blank_temporal_3_0: pf.Temporal3_0
) -> pf.CoreTEXT3_0:
    text_3_0.push_temporal(blank_temporal_3_0, LINK_NAME2, 9001)
    return text_3_0


@pytest.fixture
def text2_3_1(
    text_3_1: pf.CoreTEXT3_1, blank_temporal_3_1: pf.Temporal3_1
) -> pf.CoreTEXT3_1:
    text_3_1.push_temporal(blank_temporal_3_1, LINK_NAME2, 9001)
    return text_3_1


@pytest.fixture
def text2_3_2(
    text_3_2: pf.CoreTEXT3_2, blank_temporal_3_2: pf.Temporal3_2
) -> pf.CoreTEXT3_2:
    text_3_2.push_temporal(blank_temporal_3_2, LINK_NAME2, 9001)
    return text_3_2


@pytest.fixture
def dataset2_2_0(
    dataset_2_0: pf.CoreDataset2_0,
    blank_temporal_2_0: pf.Temporal2_0,
    series2: pl.Series,
) -> pf.CoreDataset2_0:
    dataset_2_0.push_temporal(blank_temporal_2_0, series2, LINK_NAME2, 9001)
    return dataset_2_0


@pytest.fixture
def dataset2_3_0(
    dataset_3_0: pf.CoreDataset3_0,
    blank_temporal_3_0: pf.Temporal3_0,
    series2: pl.Series,
) -> pf.CoreDataset3_0:
    dataset_3_0.push_temporal(blank_temporal_3_0, series2, LINK_NAME2, 9001)
    return dataset_3_0


@pytest.fixture
def dataset2_3_1(
    dataset_3_1: pf.CoreDataset3_1,
    blank_temporal_3_1: pf.Temporal3_1,
    series2: pl.Series,
) -> pf.CoreDataset3_1:
    dataset_3_1.push_temporal(blank_temporal_3_1, series2, LINK_NAME2, 9001)
    return dataset_3_1


@pytest.fixture
def dataset2_3_2(
    dataset_3_2: pf.CoreDataset3_2,
    blank_temporal_3_2: pf.Temporal3_2,
    series2: pl.Series,
) -> pf.CoreDataset3_2:
    dataset_3_2.push_temporal(blank_temporal_3_2, series2, LINK_NAME2, 9001)
    return dataset_3_2


all_blank_core = pytest.mark.parametrize(
    "core",
    [
        lazy_fixture("blank_text_2_0"),
        lazy_fixture("blank_text_3_0"),
        lazy_fixture("blank_text_3_1"),
        lazy_fixture("blank_text_3_2"),
        lazy_fixture("blank_dataset_2_0"),
        lazy_fixture("blank_dataset_3_0"),
        lazy_fixture("blank_dataset_3_1"),
        lazy_fixture("blank_dataset_3_2"),
    ],
)

all_core = pytest.mark.parametrize(
    "core",
    [
        lazy_fixture("text_2_0"),
        lazy_fixture("text_3_0"),
        lazy_fixture("text_3_1"),
        lazy_fixture("text_3_2"),
        lazy_fixture("dataset_2_0"),
        lazy_fixture("dataset_3_0"),
        lazy_fixture("dataset_3_1"),
        lazy_fixture("dataset_3_2"),
    ],
)

all_text2 = pytest.mark.parametrize(
    "core",
    [
        lazy_fixture("text2_2_0"),
        lazy_fixture("text2_3_0"),
        lazy_fixture("text2_3_1"),
        lazy_fixture("text2_3_2"),
        lazy_fixture("dataset2_2_0"),
        lazy_fixture("dataset2_3_0"),
        lazy_fixture("dataset2_3_1"),
        lazy_fixture("dataset2_3_2"),
    ],
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
        assert getattr(core, attr) is None
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
        assert core.trigger is None
        tr = (LINK_NAME1, 0)
        core.trigger = tr
        assert core.trigger == tr

    @all_core
    def test_trigger_threshold(self, core: AnyCore) -> None:
        tr = (LINK_NAME1, 0)
        core.trigger = tr
        assert core.trigger == tr
        core.set_trigger_threshold(1)
        assert core.trigger == (LINK_NAME1, 1)

    @all_blank_core
    def test_trigger_bad(self, core: AnyCore) -> None:
        with pytest.raises(TypeError):
            core.trigger = cast(Trigger, "over,9000")

    @all_blank_core
    def test_trigger_nolink(self, core: AnyCore) -> None:
        with pytest.raises(pf.PyreflowException):
            core.trigger = ("harold", 0)

    @all_core
    def test_par(self, core: AnyCore) -> None:
        assert core.par == 1

    @all_core
    def test_shortnames(self, core: AnyCore) -> None:
        assert core.all_shortnames == [LINK_NAME1]
        new_name = "I can haz IP"
        core.all_shortnames = [new_name]
        assert core.all_shortnames == [new_name]
        with pytest.raises(ValueError):
            core.all_shortnames = ["I,can,haz,IP"]

    @all_core
    def test_shortnames_maybe(self, core: AnyCore) -> None:
        assert core.shortnames_maybe == [LINK_NAME1]

    @all_core
    def test_longnames(self, core: AnyCore) -> None:
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
    def test_meas_opt_strs(self, get: str, set: str, core: AnyCore) -> None:
        assert getattr(core, get) == [(0, None)]
        new = "bla"
        getattr(core, set)([new])
        assert getattr(core, get) == [(0, new)]
        with pytest.raises(TypeError):
            getattr(core, set)([42])

    # each of these should be a non-negative float
    @all_core
    @pytest.mark.parametrize(
        "get, set",
        [(x, f"set_{x}") for x in ["powers", "detector_voltages"]],
    )
    def test_meas_opt_floats(self, get: str, set: str, core: AnyCore) -> None:
        assert getattr(core, get) == [(0, None)]
        new = 0.5
        getattr(core, set)([new])
        assert getattr(core, get) == [(0, new)]
        newer = 0.0
        getattr(core, set)([newer])
        assert getattr(core, get) == [(0, newer)]
        with pytest.raises(ValueError):
            getattr(core, set)([-1.0])
        with pytest.raises(TypeError):
            getattr(core, set)(["pickle rick"])

    # TODO add raw_keywords test

    @all_core
    def test_nonstandard(self, core: AnyCore) -> None:
        k = "midnight"
        v = "rowhammer"
        # trying to get key from empty list should return None
        assert core.get_nonstandard(k) is None
        # ditto if we try to remove it
        assert core.remove_nonstandard(k) is None
        # insert should succeed
        core.insert_nonstandard(k, v)
        # now the key should be present
        assert core.get_nonstandard(k) == v
        # if we remove it we should also get the key
        assert core.remove_nonstandard(k) == v
        # no the key shouldn't be present again
        assert core.get_nonstandard(k) is None
        # and it shouldn't return anything if we try to remove it a 2nd time
        assert core.remove_nonstandard(k) is None

    @pytest.mark.parametrize(
        "core",
        [lazy_fixture("text_2_0"), lazy_fixture("dataset_2_0")],
    )
    def test_temporal_no_timestep(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0
    ) -> None:
        assert core.temporal is None
        core.set_temporal(LINK_NAME1, False)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1
        assert core.unset_temporal(False) is True
        assert core.temporal is None
        assert core.unset_temporal(False) is False

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture("text_3_0"),
            lazy_fixture("text_3_1"),
            lazy_fixture("text_3_2"),
            lazy_fixture("dataset_3_0"),
            lazy_fixture("dataset_3_1"),
            lazy_fixture("dataset_3_2"),
        ],
    )
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
        core.set_temporal(LINK_NAME1, ts, False)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1
        assert core.unset_temporal(False) == ts
        assert core.temporal is None
        assert core.unset_temporal(False) is None

    @pytest.mark.parametrize(
        "core",
        [lazy_fixture("text_2_0"), lazy_fixture("dataset_2_0")],
    )
    def test_temporal_no_timestep_at(
        self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0
    ) -> None:
        assert core.temporal is None
        core.set_temporal_at(0, False)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture("text_3_0"),
            lazy_fixture("text_3_1"),
            lazy_fixture("text_3_2"),
            lazy_fixture("dataset_3_0"),
            lazy_fixture("dataset_3_1"),
            lazy_fixture("dataset_3_2"),
        ],
    )
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
        core.set_temporal_at(0, ts, False)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1

    @all_core
    def test_remove_meas_by_name(self, core: AnyCore) -> None:
        assert len(core.measurements) == 1
        assert core.remove_measurement_by_name(LINK_NAME1) is not None
        assert len(core.measurements) == 0
        assert core.remove_measurement_by_name(LINK_NAME1) is None

    @all_core
    def test_remove_meas_by_index(self, core: AnyCore) -> None:
        assert len(core.measurements) == 1
        core.remove_measurement_by_index(0)
        with pytest.raises(IndexError):
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
    def test_replace_optical_at(self, core: AnyCore, optical) -> None:
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
    def test_replace_optical_named(self, core: AnyCore, optical) -> None:
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
    def test_replace_temporal_at(self, core: AnyCore, temporal) -> None:
        ln = "show me wut u got"
        temporal.longname = ln
        core.replace_temporal_at(1, temporal, False)
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
    def test_replace_temporal_named(self, core: AnyCore, temporal) -> None:
        ln = "the combination is... 1. 2. 3. 4. 5."
        temporal.longname = ln
        core.replace_temporal_named(LINK_NAME2, temporal, False)
        core.measurement_at(1).longname == ln

    @all_text2
    def test_rename_temporal(self, core: AnyCore) -> None:
        new = "they've gone plaid"
        assert core.rename_temporal(new) == LINK_NAME2

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
    def test_text_insert_optical(self, core: AnyCoreTEXT, optical) -> None:
        core.insert_optical(0, optical, LINK_NAME1, 9001)
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
    def test_text_insert_temporal(self, core: AnyCoreTEXT, temporal) -> None:
        core.insert_temporal(0, temporal, LINK_NAME1, 9001)
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
        self, core: AnyCoreDataset, optical, series1: pl.Series
    ) -> None:
        core.insert_optical(0, optical, series1, LINK_NAME1, 9001)
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
        self, core: AnyCoreDataset, temporal, series1: pl.Series
    ) -> None:
        core.insert_temporal(0, temporal, series1, LINK_NAME1, 9001)
        assert isinstance(core.measurement_at(0), type(temporal))

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_2_0",
                "text2_3_0",
                "text2_3_1",
                "text2_3_2",
            ]
        ],
    )
    def test_unset_measurements(self, core: AnyCoreTEXT) -> None:
        assert len(core.measurements) == 2
        core.unset_measurements()
        assert len(core.measurements) == 0

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "dataset2_2_0",
                "dataset2_3_0",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
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

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_2_0",
                "text2_3_0",
                "dataset2_2_0",
                "dataset2_3_0",
            ]
        ],
    )
    def test_ordered_layout(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert isinstance(core.layout, pf.OrderedUint32Layout)
        core.layout = pf.OrderedUint64Layout([9002, 9003], False)
        assert isinstance(core.layout, pf.OrderedUint64Layout)
        with pytest.raises(TypeError):
            core.layout = pf.EndianUintLayout([9002, 9003], False)  # type: ignore

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
    def test_endian_layout(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert isinstance(core.layout, pf.EndianUintLayout)
        core.layout = pf.EndianF32Layout([9002, 9003], False)
        assert isinstance(core.layout, pf.EndianF32Layout)
        with pytest.raises(TypeError):
            core.layout = pf.OrderedUint64Layout([9002, 9003], False)  # type: ignore

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
        optical,
    ) -> None:
        core.set_measurements([(LINK_NAME1, optical)], prefix="_")

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
        optical,
    ) -> None:
        core.set_measurements([(LINK_NAME1, optical)])

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
        optical,
    ) -> None:
        new = pf.OrderedUint64Layout([1], False)
        core.set_measurements_and_layout([(LINK_NAME1, optical)], new, prefix="_")

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
        optical,
    ) -> None:
        new = pf.EndianF32Layout([1], False)
        core.set_measurements_and_layout([(LINK_NAME1, optical)], new)

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
        optical,
        series2: pl.Series,
    ) -> None:
        core.set_measurements_and_data([(LINK_NAME1, optical)], [series2], prefix="_")

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
        optical,
        series2: pl.Series,
    ) -> None:
        core.set_measurements_and_data([(LINK_NAME1, optical)], [series2])

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_2_0",
                "text2_3_0",
                "dataset2_2_0",
                "dataset2_3_0",
            ]
        ],
    )
    def test_set_shortnames_maybe(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert len(core.measurements) == 2
        # note this will only set the measurements, since the time name is
        # never None
        core.set_measurement_shortnames_maybe([None])
        assert core.shortnames_maybe == [None, "maple latte"]
        with pytest.raises(pf.PyreflowException):
            core.set_measurement_shortnames_maybe([None, None])

    @pytest.mark.parametrize(
        "core",
        [lazy_fixture(c) for c in ["text2_2_0", "dataset2_2_0"]],
    )
    def test_all_scales(self, core: pf.CoreTEXT2_0 | pf.CoreDataset2_0) -> None:
        assert core.all_scales == [None, ()]

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_0",
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_0",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
    def test_all_transforms(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.all_transforms == [1.0, 1.0]

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_0",
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_0",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
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

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
    @pytest.mark.parametrize(
        "attr,value",
        [
            ("originality", "Original"),
            ("last_modified", datetime(2112, 1, 1, 0, 0)),
            ("last_modifier", "you, obviously"),
            ("platename", "juice malouse"),
            ("plateid", "666"),
            ("wellid", "9.75"),
        ],
    )
    def test_modified_plate(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
        attr: str,
        value,
    ) -> None:
        assert getattr(core, attr) is None
        setattr(core, attr, value)
        assert getattr(core, attr) == value
        with pytest.raises(TypeError):
            setattr(core, attr, 1.61)

    # TODO add comp
    # TODO add spillover

    @pytest.mark.parametrize(
        "core",
        [lazy_fixture(c) for c in ["text2_3_0", "dataset2_3_0"]],
    )
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

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
    def test_vol(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.vol is None
        core.vol = 0.0
        assert core.vol == 0.0
        core.vol = 1.0
        assert core.vol == 1.0
        with pytest.raises(ValueError):
            core.vol = -1.0

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in [
                "text2_3_0",
                "text2_3_1",
                "text2_3_2",
                "dataset2_3_0",
                "dataset2_3_1",
                "dataset2_3_2",
            ]
        ],
    )
    def test_cytsn(
        self,
        core: pf.CoreTEXT3_0
        | pf.CoreTEXT3_1
        | pf.CoreTEXT3_2
        | pf.CoreDataset3_0
        | pf.CoreDataset3_1
        | pf.CoreDataset3_2,
    ) -> None:
        assert core.cytsn is None
        new = "12345"
        core.cytsn = new
        assert core.cytsn == new
        with pytest.raises(TypeError):
            core.cytsn = cast(str, 0.0)

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in ["text2_2_0", "text2_3_0", "dataset2_2_0", "dataset2_3_0"]
        ],
    )
    def test_wavelengths_singleton(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreDataset2_0 | pf.CoreDataset3_0,
    ) -> None:
        assert core.wavelengths == [(0, None)]
        core.set_wavelengths([1.0])
        assert core.wavelengths == [(0, 1.0)]
        with pytest.raises(ValueError):
            core.set_wavelengths([0.0])
        with pytest.raises(ValueError):
            core.set_wavelengths([-1.0])

    @pytest.mark.parametrize(
        "core",
        [
            lazy_fixture(c)
            for c in ["text2_3_1", "text2_3_2", "dataset2_3_1", "dataset2_3_2"]
        ],
    )
    def test_wavelengths_vector(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.wavelengths == [(0, None)]
        new = [1.0, 2.0]
        core.set_wavelengths([new])
        assert core.wavelengths == [(0, new)]
        with pytest.raises(ValueError):
            core.set_wavelengths([[0.0]])
        with pytest.raises(ValueError):
            core.set_wavelengths([[-1.0]])
        with pytest.raises(ValueError):
            core.set_wavelengths([[]])
