import csv
import sys
import flowio as fi  # type: ignore
import fcsparser as fp  # type: ignore
from typing import NamedTuple, assert_never, Literal
from pathlib import Path
from decimal import Decimal
from random import randrange, shuffle
from time import perf_counter_ns
from enum import Enum

import polars as pl
import numpy as np

import pyreflow as pf
import pyreflow.typing as pft

# column names

BENCH_NAME = "name"
BYTEORD = "byteord"
VERSION = "version"
BIT_WIDTHS = "bit_widths"
DATATYPES = "datatypes"
WIDTH = "width"
HEIGHT = "height"
N_KEYWORDS = "n_keywords"
TEXT_NBYTES = "text_nbytes"
DATA_NBYTES = "data_nbytes"

MEAN_READ_TEXT_NS = "mean_r_text_ns"
MEAN_READ_TEXT_NS_PER_KW = "mean_r_text_ns_per_kw"
MEAN_READ_TEXT_NS_PER_KB = "mean_r_text_ns_per_kB"
SERR_READ_TEXT_NS = "serr_r_text_ns"
SERR_READ_TEXT_NS_PER_KW = "serr_r_text_ns_per_kw"
SERR_READ_TEXT_NS_PER_KB = "serr_r_text_ns_per_kB"

MEAN_READ_DATA_NS = "mean_r_data_ns"
MEAN_READ_DATA_DIFF_NS = "mean_r_data_diff_ns"
MEAN_READ_DATA_DIFF_NS_PER_KB = "mean_r_data_diff_ns_per_kb"
MEAN_READ_DATA_DIFF_NS_PER_VAL = "mean_r_data_diff_ns_per_value"

SERR_READ_DATA_NS = "serr_r_data_ns"
SERR_READ_DATA_DIFF_NS = "serr_r_data_diff_ns"
SERR_READ_DATA_DIFF_NS_PER_KB = "serr_r_data_diff_ns_per_kb"
SERR_READ_DATA_DIFF_NS_PER_VAL = "serr_r_data_diff_ns_per_value"

MEAN_WRITE_TEXT_NS = "mean_w_text_ns"
MEAN_WRITE_TEXT_NS_PER_KW = "mean_w_text_ns_per_kw"
MEAN_WRITE_TEXT_NS_PER_KB = "mean_w_text_ns_per_kB"
SERR_WRITE_TEXT_NS = "serr_w_text_ns"
SERR_WRITE_TEXT_NS_PER_KW = "serr_w_text_ns_per_kw"
SERR_WRITE_TEXT_NS_PER_KB = "serr_w_text_ns_per_kB"

MEAN_WRITE_DATA_NS = "mean_w_data_ns"
MEAN_WRITE_DATA_DIFF_NS = "mean_w_data_diff_ns"
MEAN_WRITE_DATA_DIFF_NS_PER_KB = "mean_w_data_diff_ns_per_kB"
MEAN_WRITE_DATA_DIFF_NS_PER_VAL = "mean_w_data_diff_ns_per_value"

SERR_WRITE_DATA_NS = "serr_w_data_ns"
SERR_WRITE_DATA_DIFF_NS = "serr_w_data_diff_ns"
SERR_WRITE_DATA_DIFF_NS_PER_KB = "serr_w_data_diff_ns_per_kb"
SERR_WRITE_DATA_DIFF_NS_PER_VAL = "serr_w_data_diff_ns_per_value"

MEAN_READ_STD_NS = "mean_r_std_ns"
SERR_READ_STD_NS = "serr_r_std_ns"

MEAN_READ_STD_DIFF_NS_PER_KW = "mean_r_std_diff_ns_per_kw"
SERR_READ_STD_DIFF_NS_PER_KW = "serr_r_std_diff_ns_per_kw"

MEAN_READ_DATA_RNG_NS = "mean_r_data_rng_ns"
MEAN_READ_DATA_RNG_DIFF_NS = "mean_r_data_rng_diff_ns"
MEAN_READ_DATA_RNG_DIFF_NS_PER_VAL = "mean_r_data_rng_diff_ns_per_val"
SERR_READ_DATA_RNG_NS = "serr_r_data_rng_ns"
SERR_READ_DATA_RNG_DIFF_NS = "serr_r_data_rng_diff_ns"
SERR_READ_DATA_RNG_DIFF_NS_PER_VAL = "serr_r_data_rng_diff_ns_per_val"

MEAN_READ_DATA_CRC_NS = "mean_r_data_crc_ns"
MEAN_READ_DATA_CRC_DIFF_NS = "mean_r_data_crc_diff_ns"
MEAN_READ_DATA_CRC_DIFF_NS_PER_KB = "mean_r_data_crc_diff_ns_per_kB"
SERR_READ_DATA_CRC_NS = "serr_r_data_crc_ns"
SERR_READ_DATA_CRC_DIFF_NS = "serr_r_data_crc_diff_ns"
SERR_READ_DATA_CRC_DIFF_NS_PER_KB = "serr_r_data_crc_diff_ns_per_kB"


class FFBenchKey(Enum):
    """Testing modes for fireflow.

    Unlike other libraries which only support reading/writing TEXT/DATA,
    fireflow additionally supports keyword standardization, CRC checks, and $PnR
    checks/truncation. Each of these are extra steps which may be optionally
    applied that don't exist elsewhere. Therefore they are measured separately.

    """

    READ_FLAT = "read_flat"
    READ_STD = "read_std"
    READ_DATA = "read_data"
    READ_DATA_RNG = "read_data_rng"
    READ_DATA_CRC = "read_data_crc"
    WRITE_TEXT = "write_text"
    WRITE_DATA = "write_data"


class FlowIOBenchKey(Enum):
    """Testing modes for flowio.

    This library supports both reading and writing TEXT and DATA.
    """

    READ_TEXT = "read_text"
    READ_DATA = "read_data"
    WRITE_TEXT = "write_text"
    WRITE_DATA = "write_data"


class FCSParserBenchKey(Enum):
    """Testing modes for fcsparser.

    This library only supports reading TEXT and DATA (hence name).
    """

    READ_TEXT = "read_text"
    READ_DATA = "read_data"


BENCH_FILES_NAME = "bench_files.tsv"


FF_TRIAL_NUMBER = {
    FFBenchKey.READ_FLAT: 100,
    FFBenchKey.READ_STD: 100,
    FFBenchKey.READ_DATA: 30,
    FFBenchKey.READ_DATA_RNG: 30,
    FFBenchKey.READ_DATA_CRC: 30,
    FFBenchKey.WRITE_TEXT: 100,
    FFBenchKey.WRITE_DATA: 10,
}

FLOWIO_TRIAL_NUMBER = {
    FlowIOBenchKey.READ_TEXT: 10,
    FlowIOBenchKey.READ_DATA: 10,
    FlowIOBenchKey.WRITE_TEXT: 10,
    FlowIOBenchKey.WRITE_DATA: 10,
}

FCSPARSER_TRIAL_NUMBER = {
    FCSParserBenchKey.READ_TEXT: 10,
    FCSParserBenchKey.READ_DATA: 10,
}


DType = (
    type[pl.UInt8]
    | type[pl.UInt16]
    | type[pl.UInt32]
    | type[pl.UInt64]
    | type[pl.Float32]
    | type[pl.Float64]
)

Range = tuple[Literal["I", "A"], int] | tuple[Literal["F", "D"], Decimal]


class BenchResult[X](NamedTuple):
    name: str
    key: X
    value: float


type FCSParserBenchResult = BenchResult[FCSParserBenchKey]
type FlowIOBenchResult = BenchResult[FlowIOBenchKey]
type FFBenchResult = BenchResult[FFBenchKey]


class BenchFile(NamedTuple):
    name: str
    version: pft.FCSVersion
    height: int
    width: int
    byteord: str
    datatypes: str
    bit_widths: str
    n_keywords: int
    text_nbytes: int
    data_nbytes: int


class BenchRun[X](NamedTuple):
    name: str
    key: X

    def fcs_name(self, suffix: str | None = None) -> Path:
        if suffix is not None:
            return Path(f"{self.name}_{suffix}.fcs")
        return Path(f"{self.name}.fcs")


class FlowIOBenchRun(BenchRun[FlowIOBenchKey]):
    """A benchmark run for flowio."""

    def read_text(self, root: Path) -> float:
        start = perf_counter_ns()
        fi.FlowData(root / self.fcs_name(), only_text=True)
        return perf_counter_ns() - start

    def read_data(self, root: Path) -> float:
        start = perf_counter_ns()
        fd = fi.FlowData(root / self.fcs_name())
        assert fd.events is not None, f"DATA could not be read for {self.fcs_name}"
        # This is the fairest comparison with fireflow, since flowio by default
        # will parse DATA as a 1D list, and fireflow will do an on-the-fly
        # transposition to put each column in a separate vector. If we want to
        # be even more pedantic this should actually be put into a pandas
        # dataframe since that will involve moving each column to its own memory
        # location, which will probably be slower than simply coercing a vector
        # into a 2D array.
        _ = fd.as_array(preprocess=False)
        end = perf_counter_ns()
        return end - start

    def write_text(self, input_root: Path, scratch_root: Path) -> float:
        fd = fi.FlowData(input_root / self.fcs_name(), only_text=True)
        # flowio will complain if events is None, which will happen if the file
        # is read without reading DATA. Fool it by setting to an empty list; it
        # apparently can't tell the difference ;)
        fd.events = []
        start = perf_counter_ns()
        fd.write_fcs(scratch_root / self.fcs_name("flowio_write_text"))
        end = perf_counter_ns()
        return end - start

    def write_data(self, input_root: Path, scratch_root: Path) -> float:
        fd = fi.FlowData(input_root / self.fcs_name())
        start = perf_counter_ns()
        fd.write_fcs(scratch_root / self.fcs_name("flowio_write_data"))
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> FlowIOBenchResult:
        if self.key == FlowIOBenchKey.READ_TEXT:
            value = self.read_text(input_root)
        elif self.key == FlowIOBenchKey.READ_DATA:
            value = self.read_data(input_root)
        elif self.key == FlowIOBenchKey.WRITE_TEXT:
            value = self.write_text(input_root, scratch_root)
        elif self.key == FlowIOBenchKey.WRITE_DATA:
            value = self.write_data(input_root, scratch_root)
        else:
            assert_never(self.key)
        return BenchResult(name=self.name, key=self.key, value=value)


class FCSParserBenchRun(BenchRun[FCSParserBenchKey]):
    """A benchmark run for fcsparser."""

    def read_text(self, root: Path) -> float:
        start = perf_counter_ns()
        fp.parse(root / self.fcs_name(), meta_data_only=True)
        return perf_counter_ns() - start

    def read_data(self, root: Path) -> float:
        start = perf_counter_ns()
        # set reformat to false to do less work and get a cleaner measurement of
        # how fast the DATA parser really is
        meta, data = fp.parse(root / self.fcs_name(), reformat_meta=False)
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> FCSParserBenchResult:
        if self.key == FCSParserBenchKey.READ_TEXT:
            value = self.read_text(input_root)
        elif self.key == FCSParserBenchKey.READ_DATA:
            value = self.read_data(input_root)
        else:
            assert_never(self.key)
        return BenchResult(name=self.name, key=self.key, value=value)


class FFBenchRun(BenchRun[FFBenchKey]):
    """A benchmark run for fireflow."""

    @property
    def tsv_name(self) -> Path:
        return Path(f"{self.name}.tsv")

    def read_flat(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_flat_text(root / self.fcs_name())
        return perf_counter_ns() - start

    def read_std(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_std_text(root / self.fcs_name(), time_meas_pattern=None)
        return perf_counter_ns() - start

    def read_flat_data(
        self,
        root: Path,
        check_range: bool,
        compute_crc: bool,
    ) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_flat_dataset(
            root / self.fcs_name(),
            over_bitmask_action="none",
            over_range_action="warn" if check_range else "none",
            compute_crc="always" if compute_crc else "never",
        )
        end = perf_counter_ns()
        return end - start

    def write_text(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_text(
            input_root / self.fcs_name(), time_meas_pattern=None
        )
        start = perf_counter_ns()
        core.write_text(scratch_root / self.fcs_name("ff_write_text"))
        end = perf_counter_ns()
        return end - start

    def write_data(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name(), time_meas_pattern=None
        )
        start = perf_counter_ns()
        core.write_dataset(scratch_root / self.fcs_name("ff_write_data"))
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> FFBenchResult:
        if self.key == FFBenchKey.READ_FLAT:
            value = self.read_flat(input_root)
        elif self.key == FFBenchKey.READ_STD:
            value = self.read_std(input_root)
        elif self.key == FFBenchKey.READ_DATA:
            value = self.read_flat_data(input_root, False, False)
        elif self.key == FFBenchKey.READ_DATA_RNG:
            value = self.read_flat_data(input_root, True, False)
        elif self.key == FFBenchKey.READ_DATA_CRC:
            value = self.read_flat_data(input_root, False, True)
        elif self.key == FFBenchKey.WRITE_TEXT:
            value = self.write_text(input_root, scratch_root)
        elif self.key == FFBenchKey.WRITE_DATA:
            value = self.write_data(input_root, scratch_root)
        else:
            assert_never(self.key)
        return BenchResult(name=self.name, key=self.key, value=value)

    def check_data(self, input_root: Path, scratch_root: Path) -> None:
        """Ensure DATA didn't get screwed up during optimization.

        DATA will be compared against a TSV file which was generated
        in parallel to the FCS file directly from the polars dataframe.

        Read test will be successful if reading the FCS file produces the same
        dataframe as that in the TSV file. Note that the schema for the FCS file
        needs to be used when reading the TSV file. We are implicitly testing
        the initial write of the FCS input file, although this will likely not
        match the current commit to the code being used to read. Obviously the
        write has to be correct, although this is directly tested for the
        current commit next.

        Write test will succeed if an FCS file that is written and read again
        produces the same dataframe.

        Note that these tests only check the DATA segment. Everything else
        is assumed to be correct given the rest of the test suite. DATA is
        easier to test here where is more appropriate to produce large layouts
        of different varieties.

        This is important because when optimizing, often this entails
        specializing code to different situations. In this case, the read/write
        loops are tailored to each data layout. This means that when improving
        any one of these loops we could produce a bug for a given data layout.
        Since there are separate loops for both reading and writing, this means
        that either loop might also be out of sync with the other.

        The only way this test could produce a false positive is if the read and
        write logic both have bugs that perfectly cancel each other out at the
        file level; ie they have identical data to the dataframe/TSV file but
        when written produce the wrong file. This is extremely unlucky and
        unlikely.
        """

        # test that reading FCS file is the same as TSV file
        core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name(), time_meas_pattern=None
        )
        tsv = pl.read_csv(
            input_root / self.tsv_name,
            separator="\t",
            schema=core.data.schema,
        )
        assert core.data.equals(tsv)

        # test that writing FCS file produces same data as the input FCS file
        core.write_dataset(scratch_root / self.fcs_name("ff_write_check"))
        nu_core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name(), time_meas_pattern=None
        )

        assert core.data.equals(nu_core.data)


def core_to_benchfile(name: str, core: pft.AnyCoreDataset) -> BenchFile:
    def sum_dict(xs: dict[str, str]) -> int:
        return sum(len(k) + len(v) for k, v in xs.items())

    version = core.version
    height = core.data.height
    width = core.data.width

    n_values = width * height

    lt = core.data_schema

    bit_widths: str

    if isinstance(lt, pf.MixedDataSchema) or isinstance(lt, pf.VariableUintDataSchema):
        data_nbytes = sum(lt.byte_widths) * height
        bit_widths = ",".join(str(i * 8) for i in sorted(set(lt.byte_widths)))
    elif isinstance(lt, pf.BigLittleF32DataSchema) or isinstance(
        lt, pf.OrderedF32DataSchema
    ):
        data_nbytes = 4 * n_values
        bit_widths = "32"
    elif isinstance(lt, pf.BigLittleF64DataSchema) or isinstance(
        lt, pf.OrderedF64DataSchema
    ):
        data_nbytes = 8 * n_values
        bit_widths = "64"
    elif isinstance(lt, pf.OrderedUintDataSchema | pf.SingleUintDataSchema):
        data_nbytes = lt.byte_width * n_values
        bit_widths = str(lt.byte_width * 8)
    else:
        assert False, "invalid layout"

    datatypes: str

    if isinstance(lt, pf.MixedDataSchema):
        datatypes = ",".join(sorted(set(t for (t, _) in lt.typed_ranges)))
    elif isinstance(lt, pf.BigLittleF32DataSchema | pf.OrderedF32DataSchema):
        datatypes = "F"
    elif isinstance(lt, pf.BigLittleF64DataSchema | pf.OrderedF64DataSchema):
        datatypes = "D"
    elif isinstance(
        lt,
        pf.OrderedUintDataSchema | pf.SingleUintDataSchema | pf.VariableUintDataSchema,
    ):
        datatypes = "I"
    else:
        assert False, "invalid layout"

    byteord: str

    def endian_to_order(e: pft.Endian) -> str:
        return "1,2,3,4" if e == "little" else "4,3,2,1"

    if isinstance(
        lt,
        pf.BigLittleF32DataSchema
        | pf.BigLittleF64DataSchema
        | pf.MixedDataSchema
        | pf.VariableUintDataSchema
        | pf.SingleUintDataSchema,
    ):
        byteord = endian_to_order(lt.endian)
    else:
        byteord = (
            ",".join(map(str, lt.byteord))
            if isinstance(lt.byteord, list)
            else endian_to_order(lt.byteord)
        )

    std_keywords = core.standard_keywords("both", "both")

    n_keywords = len(std_keywords) + len(core.nonstandard_keywords)

    n_delimiters = n_keywords * 2 + 1
    text_nbytes = (
        n_delimiters + sum_dict(std_keywords) + sum_dict(core.nonstandard_keywords)
    )

    return BenchFile(
        name,
        version,
        height=height,
        width=width,
        byteord=byteord,
        bit_widths=bit_widths,
        datatypes=datatypes,
        text_nbytes=text_nbytes,
        data_nbytes=data_nbytes,
        n_keywords=n_keywords,
    )


def nonstd_keywords(i: int) -> dict[str, str]:
    return {
        f"P{i + 1}_{k}": v
        for k, v in [
            ("AAA", "mr-hashemi"),
            ("BBB", "Ook!"),
            ("CCC", "arnoldC"),
            ("DDD", "LOLCODE"),
            ("EEE", "Malbolge"),
            ("FFF", "VBA"),
        ]
    }


def meas_3_0(i: int) -> pft.Measurement3_0:
    return (
        f"C{i + 1}",
        pf.Optical3_0(
            longname=f"Column{i + 1}",
            wavelength=randrange(500, 700),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
        ),
        1.0,
    )


def meas_3_1(i: int) -> pft.Measurement3_1:
    return (
        f"C{i + 1}",
        pf.Optical3_1(
            longname=f"Column{i + 1}",
            wavelengths=[randrange(500, 700)],
            display=(False, randrange(0, 10), randrange(11, 20)),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
        ),
        1.0,
    )


def meas_3_2(i: int) -> pft.Measurement3_2:
    return (
        f"C{i + 1}",
        pf.Optical3_2(
            longname=f"Column{i + 1}",
            wavelengths=[randrange(500, 700)],
            display=(False, randrange(0, 10), randrange(11, 20)),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
            measurement_type="phy",
            tag=f"Tag{i + 1}",
        ),
        1.0,
    )


def core_3_0_pdp11(
    height: int,
    width: int,
) -> pf.CoreDataset3_0:
    ms: pft.Measurements3_0 = [meas_3_0(i) for i in range(0, width)]
    rs = [2**32 - 1 for _ in range(0, width)]
    # wonky byteord...
    layout = pf.OrderedUintDataSchema(rs, byteord=[3, 4, 1, 2])
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**32 - 1, size=height),
                dtype=pl.UInt32,
            )
            for _ in range(0, width)
        ]
    )
    core = pf.CoreDataset3_0(ms, layout, data)
    return core


# TODO there is no way to save a file without truncating bits first, which
# may or may not be what we want. Some files "use" these upper bits for things
def core_3_1(
    width: int,
    layout: pf.SingleUintDataSchema
    | pf.VariableUintDataSchema
    | pf.BigLittleF32DataSchema
    | pf.BigLittleF64DataSchema,
    data: pl.DataFrame,
) -> pf.CoreDataset3_1:
    ms: pft.Measurements3_1 = [meas_3_1(i) for i in range(0, width)]
    core = pf.CoreDataset3_1(ms, layout, data)
    return core


def core_3_1_int(
    height: int, width: int, byte_width: int, big_endian: bool
) -> pf.CoreDataset3_1:
    upper = 2 ** (8 * byte_width) - 1
    rs = [upper for _ in range(0, width)]
    layout = pf.SingleUintDataSchema(
        rs,
        byte_width=byte_width,
        endian="big" if big_endian else "little",
    )
    dtype: type[pl.UInt8] | type[pl.UInt16] | type[pl.UInt32] | type[pl.UInt64]
    if byte_width == 1:
        dtype = pl.UInt8
    elif byte_width == 2:
        dtype = pl.UInt16
    elif byte_width < 5:
        dtype = pl.UInt32
    else:
        dtype = pl.UInt64
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=dtype,
            )
            for _ in range(0, width)
        ]
    )
    return core_3_1(width, layout, data)


def core_3_1_float(height: int, width: int, is64: bool) -> pf.CoreDataset3_1:
    upper = 1e10
    rs = [upper for _ in range(0, width)]
    layout = pf.BigLittleF64DataSchema(rs) if is64 else pf.BigLittleF32DataSchema(rs)
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=pl.Float64 if is64 else pl.Float32,
            )
            for _ in range(0, width)
        ]
    )
    return core_3_1(width, layout, data)


def core_3_1_cube(height: int, big_endian: bool) -> pf.CoreDataset3_1:
    # per https://github.com/RGLab/flowCore/issues/46, 4x16+32+8
    rs: list[pft.VariableBitmask] = [
        ("U16", 2**16 - 1),
        ("U16", 2**16 - 1),
        ("U16", 2**16 - 1),
        ("U16", 2**16 - 1),
        ("U32", 2**32 - 1),
        ("U08", 2**8 - 1),
    ]
    layout = pf.VariableUintDataSchema(
        rs,
        endian="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**16 - 1, size=height), dtype=pl.UInt16
            )
            for _ in range(0, 4)
        ]
        + [
            pl.Series(
                np.random.uniform(low=0, high=2**32 - 1, size=height), dtype=pl.UInt32
            ),
            pl.Series(
                np.random.uniform(low=0, high=2**8 - 1, size=height), dtype=pl.UInt64
            ),
        ]
    )
    return core_3_1(6, layout, data)


def to_data_parts(r: pft.MixedRange) -> tuple[float | int, DType]:
    if r[0] == "F32":
        return (float(r[1]), pl.Float32)
    elif r[0] == "F64":
        return (float(r[1]), pl.Float64)
    elif r[0] == "U08":
        return (r[1], pl.UInt8)
    elif r[0] == "U16":
        return (r[1], pl.UInt16)
    elif r[0] == "U32":
        return (r[1], pl.UInt32)
    elif r[0] == "U64":
        return (r[1], pl.UInt64)
    else:
        assert False, f"invalid datatype {r[1]}"


def core_3_2_a8(height: int, big_endian: bool) -> pf.CoreDataset3_2:
    floats: list[pft.MixedRange] = [("F32", 1e10)] * 380
    ints: list[pft.MixedRange] = [("U32", 2**32 - 1)] * 20
    rs = floats + ints
    layout = pf.MixedDataSchema(rs)
    data_parts = [to_data_parts(r) for r in rs]
    data = pl.DataFrame(
        pl.Series(np.random.uniform(low=0, high=u, size=height), dtype=t)
        for (u, t) in data_parts
    )
    ms = [meas_3_2(i) for i in range(0, len(rs))]
    core = pf.CoreDataset3_2(ms, layout, data, cyt="WALL-E")
    return core


def core_3_2_random_mixed(height: int, big_endian: bool) -> pf.CoreDataset3_2:
    n_cols = 15

    f32: list[pft.MixedRange] = [("F32", 1e10)] * n_cols
    f64: list[pft.MixedRange] = [("F64", 1e10)] * n_cols
    int8: list[pft.MixedRange] = [("U08", 2**8 - 1)] * n_cols
    int16: list[pft.MixedRange] = [("U16", 2**16 - 1)] * n_cols
    int32: list[pft.MixedRange] = [("U32", 2**32 - 1)] * n_cols
    int64: list[pft.MixedRange] = [("U64", 2**64 - 1)] * n_cols

    rs = f32 + f64 + int8 + int16 + int32 + int64

    # torture the branch predictor
    shuffle(rs)
    layout = pf.MixedDataSchema(rs)

    data_parts = [to_data_parts(r) for r in rs]
    data = pl.DataFrame(
        pl.Series(np.random.uniform(low=0, high=u, size=height), dtype=t)
        for (u, t) in data_parts
    )
    ms = [meas_3_2(i) for i in range(0, len(rs))]
    core = pf.CoreDataset3_2(ms, layout, data, cyt="GLaDOS")
    return core


def make_bench_files(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    bench_files = []

    def print_files(name: str, core: pft.AnyCoreDataset) -> None:
        print(f"Writing files for '{name}'")
        core.write_dataset(root / Path(f"{name}.fcs"))
        core.data.write_csv(
            root / Path(f"{name}.tsv"),
            separator="\t",
        )
        bench_files.append(core_to_benchfile(name, core))

    # Make three different sizes of this to demonstrate how time changes with
    # width and height. We expect that for a given datatype, normalized DATA
    # throughput should not depend on width or height. TEXT throughput should
    # not depend on height but should depend on width. Standardization overhead
    # should depend on FCS version and width.
    print_files("i32_10000x25", core_3_1_int(10000, 25, 4, False))
    print_files("i32_10000x75", core_3_1_int(10000, 75, 4, False))
    print_files("i32_100000x25", core_3_1_int(100000, 25, 4, False))

    # Make a mixed byteord file just for fun, it should be way slower. This
    # also helps test a 3.0 file vs other 3.1 files
    print_files("mx_i32_10000x25", core_3_0_pdp11(10000, 25))

    # make a big endian file just for fun (it should be the same as le)
    print_files("be_i32_10000x25", core_3_1_int(10000, 25, 4, True))

    # make some other int sizes
    print_files("i16_10000x25", core_3_1_int(10000, 25, 2, False))
    print_files("i24_10000x25", core_3_1_int(10000, 25, 3, False))
    print_files("i64_10000x25", core_3_1_int(10000, 25, 8, False))

    # make float layouts
    print_files("f32_10000x25", core_3_1_float(10000, 25, False))
    print_files("f64_10000x25", core_3_1_float(10000, 25, True))

    # add cyflow cube's infamous mixed width layout
    print_files("cube_10000x6", core_3_1_cube(10000, False))

    # add BD S8/A8's mixed 32bit layout
    print_files("s8_1000x400", core_3_2_a8(1000, False))

    # layout with random mixed-width/type data, nobody uses this but it is a
    # good test since it should be the hardest to process
    print_files("mixrand_1000x90", core_3_2_random_mixed(1000, False))

    with open(root / BENCH_FILES_NAME, "w") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(BenchFile._fields)
        for b in bench_files:
            row = [
                b.name,
                b.version,
                str(b.height),
                str(b.width),
                str(b.byteord),
                str(b.datatypes),
                str(b.bit_widths),
                str(b.n_keywords),
                str(b.text_nbytes),
                str(b.data_nbytes),
            ]
            w.writerow(row)


def fmt_value(mean: str, ci: str, out: str, digits: int = 1) -> pl.Expr:
    return pl.format(
        "{} (±{}%)",
        pl.col(mean).round(digits),
        # 95% confidence interval as percentage of mean
        (pl.col(ci) / pl.col(mean) * 100 * 1.96).round(1),
    ).alias(out)


def compute_read_df(
    bench_files: pl.DataFrame,
    read_text_df: pl.DataFrame,
    read_data_df: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        read_text_df.join(bench_files, on=BENCH_NAME)
        .with_columns(
            # normalize TEXT parse time to keyword number and TEXT length in kB
            (pl.col(MEAN_READ_TEXT_NS) / pl.col(N_KEYWORDS)).alias(
                MEAN_READ_TEXT_NS_PER_KW
            ),
            (pl.col(SERR_READ_TEXT_NS) / pl.col(N_KEYWORDS)).alias(
                SERR_READ_TEXT_NS_PER_KW
            ),
            (pl.col(MEAN_READ_TEXT_NS) / pl.col(TEXT_NBYTES) * 1000).alias(
                MEAN_READ_TEXT_NS_PER_KB
            ),
            (pl.col(SERR_READ_TEXT_NS) / pl.col(TEXT_NBYTES) * 1000).alias(
                SERR_READ_TEXT_NS_PER_KB
            ),
        )
        .join(read_data_df, on=BENCH_NAME)
        .with_columns(
            # compute time taken to read DATA by taking difference of data run
            # and flat run (note DATA was read in flat mode to reduce noise)
            (pl.col(MEAN_READ_DATA_NS) - pl.col(MEAN_READ_TEXT_NS)).alias(
                MEAN_READ_DATA_DIFF_NS
            ),
            (pl.col(SERR_READ_DATA_NS).pow(2) + pl.col(SERR_READ_TEXT_NS).pow(2))
            .sqrt()
            .alias(SERR_READ_DATA_DIFF_NS),
        )
        .with_columns(
            # normalize DATA read time to number of kB read and number of
            # values read
            (pl.col(MEAN_READ_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1000).alias(
                MEAN_READ_DATA_DIFF_NS_PER_KB
            ),
            (pl.col(SERR_READ_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1000).alias(
                SERR_READ_DATA_DIFF_NS_PER_KB
            ),
            (pl.col(MEAN_READ_DATA_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                MEAN_READ_DATA_DIFF_NS_PER_VAL
            ),
            (pl.col(SERR_READ_DATA_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                SERR_READ_DATA_DIFF_NS_PER_VAL
            ),
        )
    )

    return df


def compute_write_df(
    read_df: pl.DataFrame,
    write_text_df: pl.DataFrame,
    write_data_df: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        read_df.join(write_text_df, on=BENCH_NAME)
        .with_columns(
            # normalize TEXT write time to keyword number and TEXT length in kB
            (pl.col(MEAN_WRITE_TEXT_NS) / pl.col(N_KEYWORDS)).alias(
                MEAN_WRITE_TEXT_NS_PER_KW
            ),
            (pl.col(SERR_WRITE_TEXT_NS) / pl.col(N_KEYWORDS)).alias(
                SERR_WRITE_TEXT_NS_PER_KW
            ),
            (pl.col(MEAN_WRITE_TEXT_NS) / pl.col(TEXT_NBYTES) * 1000).alias(
                MEAN_WRITE_TEXT_NS_PER_KB
            ),
            (pl.col(SERR_WRITE_TEXT_NS) / pl.col(TEXT_NBYTES) * 1000).alias(
                SERR_WRITE_TEXT_NS_PER_KB
            ),
        )
        .join(write_data_df, on=BENCH_NAME)
        .with_columns(
            # compute time taken to write DATA by taking difference of DATA run
            # and TEXT run
            (pl.col(MEAN_WRITE_DATA_NS) - pl.col(MEAN_WRITE_TEXT_NS)).alias(
                MEAN_WRITE_DATA_DIFF_NS
            ),
            (pl.col(SERR_WRITE_DATA_NS).pow(2) + pl.col(SERR_WRITE_TEXT_NS).pow(2))
            .sqrt()
            .alias(SERR_WRITE_DATA_DIFF_NS),
        )
        .with_columns(
            # normalize DATA read time to number of kB written and number of
            # values written
            (pl.col(MEAN_WRITE_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1000).alias(
                MEAN_WRITE_DATA_DIFF_NS_PER_KB
            ),
            (pl.col(SERR_WRITE_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1000).alias(
                SERR_WRITE_DATA_DIFF_NS_PER_KB
            ),
            (pl.col(MEAN_WRITE_DATA_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                MEAN_WRITE_DATA_DIFF_NS_PER_VAL
            ),
            (pl.col(SERR_WRITE_DATA_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                SERR_WRITE_DATA_DIFF_NS_PER_VAL
            ),
        )
    )

    return df


def run_flowio_bench(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = pl.read_csv(input_root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col(BENCH_NAME).is_in(names_filter))

    runs = [
        FlowIOBenchRun(name=n, key=k)
        for n in bench_files.filter(
            ~pl.col("version").eq("FCS3.2")
            & pl.col("byteord").is_in(["1,2,3,4", "4,3,2,1"])
            & ~pl.col("bit_widths").is_in(["24", "64"])
        )[BENCH_NAME]
        for k in FlowIOBenchKey
        for _ in range(0, FLOWIO_TRIAL_NUMBER[k])
    ]

    # Don't check DATA vs TSV truth data like we do for fireflow

    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.run(input_root, scratch_root) for r in runs]

    read_flat_results = [r for r in results if r.key == FlowIOBenchKey.READ_TEXT]
    read_data_results = [r for r in results if r.key == FlowIOBenchKey.READ_DATA]
    write_text_results = [r for r in results if r.key == FlowIOBenchKey.WRITE_TEXT]
    write_data_results = [r for r in results if r.key == FlowIOBenchKey.WRITE_DATA]

    def to_df(rs: list[FlowIOBenchResult], name: str) -> pl.DataFrame:
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {BENCH_NAME: pl.String, full_name: pl.Float32},
        )
        return result_df.group_by(BENCH_NAME).agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / pl.col(full_name).count().sqrt()).name.prefix(
                "serr_"
            ),
        )

    read_text_df = to_df(read_flat_results, "r_text")
    read_data_df = to_df(read_data_results, "r_data")
    write_text_df = to_df(write_text_results, "w_text")
    write_data_df = to_df(write_data_results, "w_data")

    df_read = compute_read_df(
        bench_files,
        read_text_df,
        read_data_df,
    )

    return compute_write_df(
        df_read,
        write_text_df,
        write_data_df,
    )


def run_fcsparser_bench(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = pl.read_csv(input_root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col(BENCH_NAME).is_in(names_filter))

    runs = [
        FCSParserBenchRun(name=n, key=k)
        for n in bench_files.filter(
            ~pl.col("version").eq("FCS3.2")
            & pl.col("byteord").is_in(["1,2,3,4", "4,3,2,1"])
            & ~pl.col("bit_widths").is_in(["64"])
        )[BENCH_NAME]
        for k in FCSParserBenchKey
        for _ in range(0, FCSPARSER_TRIAL_NUMBER[k])
    ]

    # Don't check DATA vs TSV truth data like we do for fireflow

    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.run(input_root, scratch_root) for r in runs]

    read_flat_results = [r for r in results if r.key == FCSParserBenchKey.READ_TEXT]
    read_data_results = [r for r in results if r.key == FCSParserBenchKey.READ_DATA]

    def to_df(rs: list[FCSParserBenchResult], name: str) -> pl.DataFrame:
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {BENCH_NAME: pl.String, full_name: pl.Float32},
        )
        return result_df.group_by(BENCH_NAME).agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / pl.col(full_name).count().sqrt()).name.prefix(
                "serr_"
            ),
        )

    read_text_df = to_df(read_flat_results, "r_text")
    read_data_df = to_df(read_data_results, "r_data")

    return compute_read_df(
        bench_files,
        read_text_df,
        read_data_df,
    )


def run_ff_bench(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = pl.read_csv(input_root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col(BENCH_NAME).is_in(names_filter))

    runs = [
        FFBenchRun(name=n, key=k)
        for n in bench_files[BENCH_NAME]
        for k in FFBenchKey
        for _ in range(0, FF_TRIAL_NUMBER[k])
    ]

    # loop through each name only once to check DATA integrity
    for r in set(r for r in runs if r.key == FFBenchKey.READ_DATA):
        r.check_data(input_root, scratch_root)

    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.run(input_root, scratch_root) for r in runs]

    read_flat_results = [r for r in results if r.key == FFBenchKey.READ_FLAT]
    read_std_results = [r for r in results if r.key == FFBenchKey.READ_STD]
    read_data_results = [r for r in results if r.key == FFBenchKey.READ_DATA]
    read_data_rng_results = [r for r in results if r.key == FFBenchKey.READ_DATA_RNG]
    read_data_crc_results = [r for r in results if r.key == FFBenchKey.READ_DATA_CRC]
    write_text_results = [r for r in results if r.key == FFBenchKey.WRITE_TEXT]
    write_data_results = [r for r in results if r.key == FFBenchKey.WRITE_DATA]

    def to_df(rs: list[FFBenchResult], name: str) -> pl.DataFrame:
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {BENCH_NAME: pl.String, full_name: pl.Float32},
        )
        return result_df.group_by(BENCH_NAME).agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / pl.col(full_name).count().sqrt()).name.prefix(
                "serr_"
            ),
        )

    read_text_df = to_df(read_flat_results, "r_text")
    read_std_df = to_df(read_std_results, "r_std")
    read_data_df = to_df(read_data_results, "r_data")
    read_data_rng_df = to_df(read_data_rng_results, "r_data_rng")
    read_data_crc_df = to_df(read_data_crc_results, "r_data_crc")
    write_text_df = to_df(write_text_results, "w_text")
    write_data_df = to_df(write_data_results, "w_data")

    df_read = compute_read_df(
        bench_files,
        read_text_df,
        read_data_df,
    )

    df_read_write = compute_write_df(
        df_read,
        write_text_df,
        write_data_df,
    )

    df_analyzed = (
        df_read_write.join(read_std_df, on=BENCH_NAME)
        .with_columns(
            # compute the overhead of standardizing TEXT by taking difference of
            # total std run and flat run
            (
                (pl.col(MEAN_READ_STD_NS) - pl.col(MEAN_READ_TEXT_NS))
                / pl.col(N_KEYWORDS)
            ).alias(MEAN_READ_STD_DIFF_NS_PER_KW),
            (
                (
                    pl.col(SERR_READ_STD_NS).pow(2) + pl.col(SERR_READ_TEXT_NS).pow(2)
                ).sqrt()
                / pl.col(N_KEYWORDS)
            ).alias(SERR_READ_STD_DIFF_NS_PER_KW),
            # also compute the ratio of standard to flat (no variance since this
            # is really complex
            (pl.col(MEAN_READ_STD_NS) / pl.col(MEAN_READ_TEXT_NS) * 100 - 100).alias(
                "r_std_ratio"
            ),
        )
        .join(read_data_rng_df, on=BENCH_NAME)
        .with_columns(
            # compute time taken to check ranges by taking difference of reading
            # DATA with and without range change applied. Note that there should
            # be no actual range errors given how to dataframes were built.
            (pl.col(MEAN_READ_DATA_RNG_NS) - pl.col(MEAN_READ_DATA_NS)).alias(
                MEAN_READ_DATA_RNG_DIFF_NS
            ),
            (pl.col(SERR_READ_DATA_RNG_NS).pow(2) + pl.col(SERR_READ_DATA_NS).pow(2))
            .sqrt()
            .alias(SERR_READ_DATA_RNG_DIFF_NS),
            # also compute the ratio of DATA+range check to reading DATA alone
            # (no variance since this is really complex
            (
                pl.col(MEAN_READ_DATA_RNG_NS) / pl.col(MEAN_READ_DATA_DIFF_NS) * 100
                - 100
            ).alias("r_data_rng_ratio"),
        )
        .join(read_data_crc_df, on=BENCH_NAME)
        .with_columns(
            # do analogous calculation to range check for CRC computation
            (pl.col(MEAN_READ_DATA_CRC_NS) - pl.col(MEAN_READ_DATA_NS)).alias(
                MEAN_READ_DATA_CRC_DIFF_NS
            ),
            (pl.col(SERR_READ_DATA_CRC_NS).pow(2) + pl.col(SERR_READ_DATA_NS).pow(2))
            .sqrt()
            .alias(SERR_READ_DATA_CRC_DIFF_NS),
            # also compute the ratio of DATA+CRC check to reading DATA alone
            # (no variance since this is really complex
            (
                pl.col(MEAN_READ_DATA_CRC_NS) / pl.col(MEAN_READ_DATA_DIFF_NS) * 100
                - 100
            ).alias("r_data_crc_ratio"),
        )
        .with_columns(
            # ratio of write to read (no variance because this more complicated than its worth)
            (pl.col(MEAN_READ_TEXT_NS) / pl.col(MEAN_WRITE_TEXT_NS) * 100).alias(
                "text_rw_ratio"
            ),
            (pl.col("mean_r_data_ns") / pl.col(MEAN_WRITE_DATA_NS) * 100).alias(
                "data_rw_ratio"
            ),
            # normalize the CRC and range check differences similar to
            # standardization overhead
            (pl.col(MEAN_READ_DATA_RNG_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                MEAN_READ_DATA_RNG_DIFF_NS_PER_VAL
            ),
            (pl.col(SERR_READ_DATA_RNG_DIFF_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias(
                SERR_READ_DATA_RNG_DIFF_NS_PER_VAL
            ),
            (
                pl.col(MEAN_READ_DATA_CRC_DIFF_NS)
                / (pl.col(TEXT_NBYTES) + pl.col(DATA_NBYTES))
                * 1000
            ).alias(MEAN_READ_DATA_CRC_DIFF_NS_PER_KB),
            (
                pl.col(SERR_READ_DATA_CRC_DIFF_NS)
                / (pl.col(TEXT_NBYTES) + pl.col(DATA_NBYTES))
                * 1000
            ).alias(SERR_READ_DATA_CRC_DIFF_NS_PER_KB),
        )
    )

    return df_analyzed


def print_ff_df(df: pl.DataFrame, output_root: Path | None) -> None:
    metadata_cols = [
        "version",
        pl.col(WIDTH).alias("$PAR"),
        pl.col(HEIGHT).alias("$TOT"),
        pl.col(BYTEORD).alias("$BYTEORD"),
        pl.col(DATATYPES).alias("$DATATYPE"),
        pl.col(BIT_WIDTHS).alias("$PnB"),
    ]

    sort_cols = [BYTEORD, VERSION, BIT_WIDTHS, DATATYPES, HEIGHT]

    READ_TEXT_PER_KW = "TEXT read (ns/kw)"
    READ_TEXT_PER_KB = "TEXT read (ns/kB)"
    READ_STD_PER_KW = "Std Overhead (ns/kw)"
    READ_STD_RATIO = "Std Overhead (%)"
    READ_RNG_PER_VAL = "$PnR Overhead (ns/val)"
    READ_RNG_RATIO = "$PnR Overhead (%)"
    READ_CRC_PER_KB = "CRC Overhead (ns/kB)"
    READ_CRC_RATIO = "CRC Overhead (%)"
    READ_DATA_PER_KB = "DATA read (ns/kB)"
    READ_DATA_PER_VAL = "DATA read (ns/val)"

    WRITE_TEXT_PER_KW = "TEXT write (ns/kw)"
    WRITE_TEXT_PER_KB = "TEXT write (ns/kB)"
    WRITE_DATA_PER_VAL = "DATA write (ns/val)"
    WRITE_DATA_PER_KB = "DATA write (ns/kB)"

    df_final = df.sort(by=sort_cols).select(
        [
            BENCH_NAME,
            *metadata_cols,
            # read flat
            fmt_value(
                MEAN_READ_TEXT_NS_PER_KW,
                SERR_READ_TEXT_NS_PER_KW,
                READ_TEXT_PER_KW,
            ),
            fmt_value(
                MEAN_READ_TEXT_NS_PER_KB,
                SERR_READ_TEXT_NS_PER_KB,
                READ_TEXT_PER_KB,
            ),
            # read std
            fmt_value(
                MEAN_READ_STD_DIFF_NS_PER_KW,
                SERR_READ_STD_DIFF_NS_PER_KW,
                READ_STD_PER_KW,
            ),
            pl.col("r_std_ratio").round(1).alias(READ_STD_RATIO),
            # read data
            fmt_value(
                MEAN_READ_DATA_DIFF_NS_PER_VAL,
                SERR_READ_DATA_DIFF_NS_PER_VAL,
                READ_DATA_PER_VAL,
                3,
            ),
            fmt_value(
                MEAN_READ_DATA_DIFF_NS_PER_KB,
                SERR_READ_DATA_DIFF_NS_PER_KB,
                READ_DATA_PER_KB,
            ),
            # read range
            fmt_value(
                MEAN_READ_DATA_RNG_DIFF_NS_PER_VAL,
                SERR_READ_DATA_RNG_DIFF_NS_PER_VAL,
                READ_RNG_PER_VAL,
                3,
            ),
            pl.col("r_data_rng_ratio").round(1).alias(READ_RNG_RATIO),
            # read crc
            fmt_value(
                MEAN_READ_DATA_CRC_DIFF_NS_PER_KB,
                SERR_READ_DATA_CRC_DIFF_NS_PER_KB,
                READ_CRC_PER_KB,
            ),
            pl.col("r_data_crc_ratio").round(1).alias(READ_CRC_RATIO),
            # write text
            fmt_value(
                MEAN_WRITE_TEXT_NS_PER_KW,
                SERR_WRITE_TEXT_NS_PER_KW,
                WRITE_TEXT_PER_KW,
            ),
            fmt_value(
                MEAN_WRITE_TEXT_NS_PER_KB,
                SERR_WRITE_TEXT_NS_PER_KB,
                WRITE_TEXT_PER_KB,
            ),
            # write data
            fmt_value(
                MEAN_WRITE_DATA_DIFF_NS_PER_VAL,
                SERR_WRITE_DATA_DIFF_NS_PER_VAL,
                WRITE_DATA_PER_VAL,
                3,
            ),
            fmt_value(
                MEAN_WRITE_DATA_DIFF_NS_PER_KB,
                SERR_WRITE_DATA_DIFF_NS_PER_KB,
                WRITE_DATA_PER_KB,
            ),
            # read vs write
            pl.col("text_rw_ratio").round(1).alias("TEXT R:W Ratio (%)"),
            pl.col("data_rw_ratio").round(1).alias("DATA R:W Ratio (%)"),
        ]
    )

    if output_root is None:
        df_final.write_csv(sys.stdout, separator="\t")
    else:
        output_root.mkdir(parents=True, exist_ok=True)
        with open(output_root / "analysis.tsv", "w") as f:
            df_final.write_csv(f, separator="\t")


def main(args: list[str]) -> None:
    cmd = args[1]
    bench_path = Path(args[2])

    if cmd == "make":
        make_bench_files(bench_path)
    elif cmd == "run_all":
        output_root = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        names_filter = args[5:]
        df_ff = run_ff_bench(bench_path, scratch_root, names_filter)
        df_flowio = run_flowio_bench(bench_path, scratch_root, names_filter)
        df_fcsparser = run_fcsparser_bench(bench_path, scratch_root, names_filter)
        read_columns = [
            BENCH_NAME,
            BYTEORD,
            VERSION,
            BIT_WIDTHS,
            DATATYPES,
            WIDTH,
            HEIGHT,
            N_KEYWORDS,
            TEXT_NBYTES,
            DATA_NBYTES,
            MEAN_READ_TEXT_NS,
            MEAN_READ_TEXT_NS_PER_KW,
            MEAN_READ_TEXT_NS_PER_KB,
            SERR_READ_TEXT_NS,
            SERR_READ_TEXT_NS_PER_KW,
            SERR_READ_TEXT_NS_PER_KB,
            MEAN_READ_DATA_NS,
            MEAN_READ_DATA_DIFF_NS,
            MEAN_READ_DATA_DIFF_NS_PER_KB,
            MEAN_READ_DATA_DIFF_NS_PER_VAL,
            SERR_READ_DATA_NS,
            SERR_READ_DATA_DIFF_NS,
            SERR_READ_DATA_DIFF_NS_PER_KB,
            SERR_READ_DATA_DIFF_NS_PER_VAL,
        ]
        write_columns = [
            MEAN_WRITE_TEXT_NS,
            MEAN_WRITE_TEXT_NS_PER_KW,
            MEAN_WRITE_TEXT_NS_PER_KB,
            SERR_WRITE_TEXT_NS,
            SERR_WRITE_TEXT_NS_PER_KW,
            SERR_WRITE_TEXT_NS_PER_KB,
            MEAN_WRITE_DATA_NS,
            MEAN_WRITE_DATA_DIFF_NS,
            MEAN_WRITE_DATA_DIFF_NS_PER_KB,
            MEAN_WRITE_DATA_DIFF_NS_PER_VAL,
            SERR_WRITE_DATA_NS,
            SERR_WRITE_DATA_DIFF_NS,
            SERR_WRITE_DATA_DIFF_NS_PER_KB,
            SERR_WRITE_DATA_DIFF_NS_PER_VAL,
        ]
        all_columns = read_columns + write_columns
        df_all = pl.concat(
            [
                df_ff.select(all_columns).with_columns(tool=pl.lit("fireflow")),
                df_flowio.select(all_columns).with_columns(tool=pl.lit("flowio")),
                df_fcsparser.select(read_columns)
                .with_columns(pl.lit(None).alias(n) for n in write_columns)
                .with_columns(tool=pl.lit("fcsparser")),
            ],
            how="vertical",
        )
        if output_root is None:
            df_all.write_csv(sys.stdout, separator="\t")
        else:
            output_root.mkdir(parents=True, exist_ok=True)
            with open(output_root / "bench_all.tsv", "w") as f:
                df_all.write_csv(f, separator="\t")

    elif cmd == "run_ff":
        output_root = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        df = run_ff_bench(bench_path, scratch_root, args[5:])
        print_ff_df(df, output_root)
    else:
        print(f"invalid command: {cmd}")
        exit(1)


main(sys.argv)
