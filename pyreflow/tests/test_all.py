from typing import cast, Any
from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal
from pathlib import Path

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
)
import pyreflow as pf
import polars as pl

from .conftest import lazy_fixture


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


LINK_NAME1 = "wubbalubbadubdub"
LINK_NAME2 = "maple latte"


@pytest.fixture
def series1() -> pl.Series:
    return pl.Series("blub", [1, 2, 3], dtype=pl.UInt32)


@pytest.fixture
def series2() -> pl.Series:
    return pl.Series("blubby", [1, 2, 3], dtype=pl.UInt32)


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
    text_2_0.push_temporal(LINK_NAME2, blank_temporal_2_0, 9001)
    return text_2_0


@pytest.fixture
def text2_3_0(
    text_3_0: pf.CoreTEXT3_0, blank_temporal_3_0: pf.Temporal3_0
) -> pf.CoreTEXT3_0:
    text_3_0.push_temporal(LINK_NAME2, blank_temporal_3_0, 9001)
    return text_3_0


@pytest.fixture
def text2_3_1(
    text_3_1: pf.CoreTEXT3_1, blank_temporal_3_1: pf.Temporal3_1
) -> pf.CoreTEXT3_1:
    text_3_1.push_temporal(LINK_NAME2, blank_temporal_3_1, 9001)
    return text_3_1


@pytest.fixture
def text2_3_2(
    text_3_2: pf.CoreTEXT3_2, blank_temporal_3_2: pf.Temporal3_2
) -> pf.CoreTEXT3_2:
    text_3_2.push_temporal(LINK_NAME2, blank_temporal_3_2, 9001)
    return text_3_2


@pytest.fixture
def dataset2_2_0(
    dataset_2_0: pf.CoreDataset2_0,
    blank_temporal_2_0: pf.Temporal2_0,
    series2: pl.Series,
) -> pf.CoreDataset2_0:
    dataset_2_0.push_temporal(LINK_NAME2, blank_temporal_2_0, series2, 9001)
    return dataset_2_0


@pytest.fixture
def dataset2_3_0(
    dataset_3_0: pf.CoreDataset3_0,
    blank_temporal_3_0: pf.Temporal3_0,
    series2: pl.Series,
) -> pf.CoreDataset3_0:
    dataset_3_0.push_temporal(LINK_NAME2, blank_temporal_3_0, series2, 9001)
    return dataset_3_0


@pytest.fixture
def dataset2_3_1(
    dataset_3_1: pf.CoreDataset3_1,
    blank_temporal_3_1: pf.Temporal3_1,
    series2: pl.Series,
) -> pf.CoreDataset3_1:
    dataset_3_1.push_temporal(LINK_NAME2, blank_temporal_3_1, series2, 9001)
    return dataset_3_1


@pytest.fixture
def dataset2_3_2(
    dataset_3_2: pf.CoreDataset3_2,
    blank_temporal_3_2: pf.Temporal3_2,
    series2: pl.Series,
) -> pf.CoreDataset3_2:
    dataset_3_2.push_temporal(LINK_NAME2, blank_temporal_3_2, series2, 9001)
    return dataset_3_2


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
        with pytest.raises(pf.PyreflowException):
            core.tr = ("harold", 0)

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
        assert core.all_shortnames_maybe == [None, "maple latte"]
        with pytest.raises(pf.PyreflowException):
            core.all_shortnames_maybe = [None, None]

    @all_core
    def test_longnames(self, core: AnyCore) -> None:
        assert core.all_longnames == [None]
        new_name = "I can haz IP"
        core.all_longnames = [new_name]
        assert core.all_longnames == [new_name]
        with pytest.raises(TypeError):
            core.all_longnames = [cast(str, 42)]

    # TODO add raw_keywords test

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
        value: Any,
    ) -> None:
        assert getattr(core, attr) is None
        setattr(core, attr, value)
        assert getattr(core, attr) == value
        with pytest.raises(TypeError):
            setattr(core, attr, 1.61)

    # TODO add comp
    # TODO add spillover

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
        with pytest.raises(ValueError):
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
        assert core.cytsn is None
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
        with pytest.raises(ValueError):
            core.mode = "fart"  # type: ignore

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_mode3_2(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        assert core.mode is None
        core.mode = "L"
        assert core.mode == "L"
        with pytest.raises(ValueError):
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
        assert core.cyt is None
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

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    @pytest.mark.parametrize(
        "attr, good, bad",
        [
            ("flowrate", "plaid", 0.5),
            ("unstainedinfo", "(redacted)", 1.61),
            ("carriertype", "pigeon", -39),
            ("carrierid", "bloodwing", 0xDEADBEEF),
            ("locationid", "0", 3),
            (
                "begindatetime",
                datetime(2112, 1, 1, tzinfo=timezone(timedelta(hours=-5))),
                "root",
            ),
            (
                "enddatetime",
                datetime(2112, 1, 2, tzinfo=timezone(timedelta(hours=-5))),
                "octave",
            ),
        ],
    )
    def test_metaroot_3_2_opt(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
        attr: str,
        good: Any,
        bad: Any,
    ) -> None:
        assert getattr(core, attr) is None
        setattr(core, attr, good)
        assert getattr(core, attr) == good
        with pytest.raises(TypeError):
            setattr(core, attr, bad)

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_unstained_centers(
        self,
        core: pf.CoreTEXT3_2 | pf.CoreDataset3_2,
    ) -> None:
        assert core.unstainedcenters is None
        core.unstainedcenters = {LINK_NAME1: 42}
        assert core.unstainedcenters == {LINK_NAME1: 42}
        core.unstainedcenters = None
        assert core.unstainedcenters is None

    @parameterize_versions("core", ["2_0"], ["text2", "dataset2"])
    def test_applied_gates_2_0(
        self,
        core: pf.CoreTEXT2_0 | pf.CoreDataset2_0,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion2_0(0, (0.0, 1.0))
        ag: AppliedGates2_0 = ([blank_gated_meas], {0: ur}, None)
        core.applied_gates = ag

    @parameterize_versions("core", ["3_0", "3_1"], ["text2", "dataset2"])
    def test_applied_gates_3_0(
        self,
        core: pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreDataset3_0 | pf.CoreDataset3_1,
        blank_gated_meas: pf.GatedMeasurement,
    ) -> None:
        ur = pf.UnivariateRegion3_0("P1", (0.0, 1.0))
        ag: AppliedGates3_0 = ([], {0: ur}, None)
        core.applied_gates = ag
        with pytest.raises(pf.PyreflowException):
            ur_bad = pf.UnivariateRegion3_0("P3", (0.0, 1.0))
            ag_bad = cast(AppliedGates3_0, ([], {0: ur_bad}, None))
            core.applied_gates = ag_bad

    @parameterize_versions("core", ["3_2"], ["text2", "dataset2"])
    def test_applied_gates_3_2(self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2) -> None:
        ur = pf.UnivariateRegion3_2(0, (0.0, 1.0))
        ag: AppliedGates3_2 = ({0: ur}, None)
        core.applied_gates = ag
        with pytest.raises(pf.PyreflowException):
            ur_bad = pf.UnivariateRegion3_2(2, (0.0, 1.0))
            ag_bad = cast(AppliedGates3_2, ({0: ur_bad}, None))
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
        "attr", [f"all_{x}" for x in ["filters", "percents_emitted", "detector_types"]]
    )
    def test_meas_opt_strs(self, attr: str, core: AnyCore) -> None:
        assert getattr(core, attr) == [None, ()]
        new = ["bla", ()]
        setattr(core, attr, new)
        assert getattr(core, attr) == new
        with pytest.raises(TypeError):
            setattr(core, attr, [42, ()])

    # each of these should be a non-negative float
    @all_core2
    @pytest.mark.parametrize(
        "attr", [f"all_{x}" for x in ["powers", "detector_voltages"]]
    )
    def test_meas_opt_floats(self, attr: str, core: AnyCore) -> None:
        assert getattr(core, attr) == [None, ()]
        new = 0.5
        setattr(core, attr, [new, ()])
        assert getattr(core, attr) == [new, ()]
        newer = 0.0
        setattr(core, attr, [newer, ()])
        assert getattr(core, attr) == [newer, ()]
        with pytest.raises(ValueError):
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
        [
            f"all_{x}"
            for x in ["detector_names", "tags", "analytes", "measurement_types"]
        ],
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
    def test_meas_3_2_feature(self, core: pf.CoreTEXT3_2 | pf.CoreDataset3_2) -> None:
        core.all_features == [None, ()]
        core.all_features = ["Area", ()]
        assert core.all_features == ["Area", ()]
        with pytest.raises(ValueError):
            core.all_features = ["Earth Minutes", ()]  # type: ignore

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
        with pytest.raises(ValueError):
            core.all_wavelengths = [0.0, ()]
        with pytest.raises(ValueError):
            core.all_wavelengths = [-1.0, ()]

    @parameterize_versions("core", ["3_1", "3_2"], ["text2", "dataset2"])
    def test_meas_wavelengths_vector(
        self,
        core: pf.CoreTEXT3_1 | pf.CoreTEXT3_2 | pf.CoreDataset3_1 | pf.CoreDataset3_2,
    ) -> None:
        assert core.all_wavelengths == [None, ()]
        new = [1.0, 2.0]
        core.all_wavelengths = [new, ()]
        assert core.all_wavelengths == [new, ()]
        with pytest.raises(ValueError):
            core.all_wavelengths = [[0.0], ()]
        with pytest.raises(ValueError):
            core.all_wavelengths = [[-1.0], ()]
        with pytest.raises(ValueError):
            core.all_wavelengths = [[], ()]

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
        with pytest.raises(ValueError):
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
        core.set_temporal_at(0, False)
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
        core.set_temporal_at(0, ts, False)
        assert core.temporal is not None
        assert core.temporal[1] == LINK_NAME1

    @all_core
    def test_remove_meas_by_name(self, core: AnyCore) -> None:
        assert len(core.measurements) == 1
        assert core.remove_measurement_by_name(LINK_NAME1) is not None
        assert len(core.measurements) == 0
        with pytest.raises(IndexError):
            core.remove_measurement_by_name(LINK_NAME1)

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
        core.set_measurements([(LINK_NAME1, optical)], False, False)

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
        core.set_measurements([(LINK_NAME1, optical)], False, False)

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
        core.set_measurements_and_layout([(LINK_NAME1, optical)], new, False, False)

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
        core.set_measurements_and_layout([(LINK_NAME1, optical)], new, False, False)

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
        core.set_measurements_and_data(
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
        core.set_measurements_and_data(
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
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_0()
        # and should still fail when forced since $PnE is missing
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_0(True)
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
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_1()
        # and should still fail when forced since $PnE is missing
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_1(True)
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
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2()
        # and should still fail if we force since $CYT and $PnE are missing
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2(True)
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
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2()
        # and should still fail if forced since $CYT is missing
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2(True)
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
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2()
        # should still fail when forced
        with pytest.raises(pf.PyreflowException):
            core.to_version_3_2(True)
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
        with pytest.raises(pf.PyreflowException):
            core.to_dataset(pl.DataFrame([series1]), b"", [])
        new = core.to_dataset(pl.DataFrame([series1, series2]), b"", [])
        assert isinstance(new, target)


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

    def test_voltage(self, blank_gated_meas: pf.GatedMeasurement) -> None:
        assert blank_gated_meas.detector_voltage is None
        blank_gated_meas.detector_voltage = 1.0
        assert blank_gated_meas.detector_voltage == 1.0
        with pytest.raises(ValueError):
            blank_gated_meas.detector_voltage = cast(float, -1.0)

    @pytest.mark.parametrize(
        "attr", ["filter", "shortname", "percent_emitted", "longname", "detector_type"]
    )
    def test_floats(self, blank_gated_meas: pf.GatedMeasurement, attr: str) -> None:
        assert getattr(blank_gated_meas, attr) is None
        new = "this is sweet revenge and karma's a"
        setattr(blank_gated_meas, attr, new)
        assert getattr(blank_gated_meas, attr) == new
        with pytest.raises(TypeError):
            setattr(blank_gated_meas, attr, 1.0)

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
        assert meas.longname is None
        new = "Headbangeeeeeeeeeeeerrrrrrrrrrrrrrrrrrrrrrrrrrr!!!!!!"
        meas.longname = new
        assert meas.longname == new
        with pytest.raises(TypeError):
            meas.longname = cast(str, 666666666666666666666666)

    @all_blank_optical
    @pytest.mark.parametrize("attr", ["filter", "detector_type", "percent_emitted"])
    def test_optical_str(self, meas: AnyOptical, attr: str) -> None:
        assert getattr(meas, attr) is None
        new = "punky bruster"
        setattr(meas, attr, new)
        assert getattr(meas, attr) == new
        with pytest.raises(TypeError):
            meas.longname = cast(str, 13)

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
        with pytest.raises(ValueError):
            meas.transform = 0.0
        with pytest.raises(ValueError):
            meas.transform = (0.0, 0.0)

    @parameterize_versions("meas", ["3_0", "3_1", "3_2"], ["blank_temporal"])
    def test_timestep(
        self, meas: pf.Temporal3_0 | pf.Temporal3_1 | pf.Temporal3_2
    ) -> None:
        assert meas.timestep == 1.0
        meas.timestep = 2.0
        assert meas.timestep == 2.0
        with pytest.raises(ValueError):
            meas.timestep = 0.0

    @parameterize_versions("meas", ["2_0", "3_0"], ["blank_optical"])
    def test_wavelength_2_0(self, meas: pf.Optical2_0 | pf.Optical3_0) -> None:
        assert meas.wavelength is None
        new = 1.0
        meas.wavelength = new
        assert meas.wavelength == new
        with pytest.raises(ValueError):
            meas.wavelength = 0.0
        with pytest.raises(ValueError):
            meas.wavelength = -1.0

    @parameterize_versions("meas", ["3_1", "3_2"], ["blank_optical"])
    def test_wavelength_3_1(self, meas: pf.Optical3_1 | pf.Optical3_2) -> None:
        assert meas.wavelengths is None
        new = [1.0, 2.0]
        meas.wavelengths = new
        assert meas.wavelengths == new
        with pytest.raises(ValueError):
            meas.wavelengths = []
        with pytest.raises(ValueError):
            meas.wavelengths = [-1.0]
        with pytest.raises(ValueError):
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
        with pytest.raises(ValueError):
            meas.feature = "under da curv"  # type: ignore

    @parameterize_versions("meas", ["3_2"], ["blank_optical"])
    @pytest.mark.parametrize(
        "attr", ["detector_name", "tag", "measurement_type", "analyte"]
    )
    def test_optical_3_2(self, meas: AnyOptical, attr: str) -> None:
        assert getattr(meas, attr) is None
        new = "heavy metal kitten pix"
        setattr(meas, attr, new)
        assert getattr(meas, attr) == new
        with pytest.raises(TypeError):
            meas.longname = cast(str, 555)

    @parameterize_versions("meas", ["3_2"], ["blank_temporal"])
    def test_temporal_type(self, meas: pf.Temporal3_2) -> None:
        assert not meas.has_type
        meas.has_type = True
        assert meas.has_type

    @all_blank_meas
    def test_nonstandard(self, meas: AnyOptical) -> None:
        assert meas.nonstandard_keywords == {}
        with pytest.raises(ValueError):
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
        assert new.byte_widths == [8, 16, 24]
        assert new.ranges == ranges
        assert new.datatype == "I"

    def test_mixed(self) -> None:
        types: list[MixedType] = [
            ("F", Decimal(1000.0)),
            ("D", Decimal(2000.0)),
            ("I", 255),
        ]
        new = pf.MixedLayout(types)
        assert new.byte_widths == [32, 64, 8]
        assert new.typed_ranges == types


class TestReadWrite:
    @staticmethod
    def _assert_uncore_empty(
        uncore: pf.api.StdTEXTOutput | pf.api.StdDatasetOutput,
    ) -> None:
        assert uncore.parse.nextdata == 0
        assert uncore.parse.delimiter == 30
        assert len(uncore.parse.non_ascii) == 0
        assert len(uncore.parse.byte_pairs) == 0
        assert len(uncore.pseudostandard) == 0
        assert len(uncore.unused) == 0

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["blank_text"])
    def test_text_empty(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "empty_text.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p)
        self._assert_uncore_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text"])
    def test_text_non_empty_1(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "text1.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p, time_meas_pattern=None)
        self._assert_uncore_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["text2"])
    def test_text_non_empty_2(self, tmp_path: Path, core: AnyCoreTEXT) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "text2.fcs"
        core.write_text(p)
        nu_core, un_core = pf.api.fcs_read_std_text(p, time_meas_pattern=LINK_NAME2)
        self._assert_uncore_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["blank_dataset"])
    def test_dataset_empty(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "empty_dataset.fcs"
        core.write_dataset(p)
        nu_core, un_core = pf.api.fcs_read_std_dataset(p)
        self._assert_uncore_empty(un_core)
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
        self._assert_uncore_empty(un_core)
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
        self._assert_uncore_empty(un_core)
        assert core == nu_core

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
        self._assert_uncore_empty(un_core)
        assert core == nu_core

    @parameterize_versions("core", ["2_0", "3_0", "3_1", "3_2"], ["dataset"])
    def test_dataset_conversion(self, tmp_path: Path, core: AnyCoreDataset) -> None:
        d = tmp_path
        d.mkdir(exist_ok=True)
        p = d / "dataset_conversion.fcs"
        ser = pl.Series("blub", [1.5, 2.5, 3.5], dtype=pl.Float32)
        core.data = pl.DataFrame([ser])
        # this should fail because we are trying to write a non-integer float
        # as an integer
        with pytest.raises(pf.PyreflowException):
            core.write_dataset(p)
        # TODO shouldn't this emit a warning?
        core.write_dataset(p, skip_conversion_check=True)
