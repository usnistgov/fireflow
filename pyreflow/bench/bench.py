import csv
import sys
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


class BenchKey(Enum):
    READ_FLAT = "read_flat"
    READ_STD = "read_std"
    READ_DATA = "read_data"
    WRITE_TEXT = "write_text"
    WRITE_DATA = "write_data"


BENCH_FILES_NAME = "bench_files.tsv"


TRIAL_NUMBER = {
    BenchKey.READ_FLAT: 100,
    BenchKey.READ_STD: 100,
    BenchKey.READ_DATA: 10,
    BenchKey.WRITE_TEXT: 100,
    BenchKey.WRITE_DATA: 10,
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


class BenchResult(NamedTuple):
    name: str
    key: BenchKey
    value: float


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


class BenchRun(NamedTuple):
    name: str
    key: BenchKey

    @property
    def fcs_name(self) -> Path:
        return Path(f"{self.name}.fcs")

    @property
    def tsv_name(self) -> Path:
        return Path(f"{self.name}.tsv")

    def read_flat(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_flat_text(root / self.fcs_name)
        return perf_counter_ns() - start

    def read_std(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_std_text(root / self.fcs_name, time_meas_pattern=None)
        return perf_counter_ns() - start

    def read_flat_data(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_flat_dataset(root / self.fcs_name)
        end = perf_counter_ns()
        return end - start

    def write_text(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_text(
            input_root / self.fcs_name, time_meas_pattern=None
        )
        start = perf_counter_ns()
        core.write_text(scratch_root / self.fcs_name)
        end = perf_counter_ns()
        return end - start

    def write_data(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name, time_meas_pattern=None
        )
        start = perf_counter_ns()
        core.write_dataset(scratch_root / self.fcs_name)
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> BenchResult:
        if self.key == BenchKey.READ_FLAT:
            value = self.read_flat(input_root)
        elif self.key == BenchKey.READ_STD:
            value = self.read_std(input_root)
        elif self.key == BenchKey.READ_DATA:
            value = self.read_flat_data(input_root)
        elif self.key == BenchKey.WRITE_TEXT:
            value = self.write_text(input_root, scratch_root)
        elif self.key == BenchKey.WRITE_DATA:
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
            input_root / self.fcs_name, time_meas_pattern=None
        )
        tsv = pl.read_csv(
            input_root / self.tsv_name,
            separator="\t",
            schema=core.data.schema,
        )
        assert core.data.equals(tsv)

        # test that writing FCS file produces same data as the input FCS file
        core.write_dataset(scratch_root / self.fcs_name)
        nu_core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name, time_meas_pattern=None
        )

        assert core.data.equals(nu_core.data)


def core_to_benchfile(name: str, core: pft.AnyCoreDataset) -> BenchFile:
    def sum_dict(xs: dict[str, str]) -> int:
        return sum(len(k) + len(v) for k, v in xs.items())

    version: pft.FCSVersion
    # TODO this should be a method on the core class
    if isinstance(core, pf.CoreDataset2_0):
        version = "FCS2.0"
    elif isinstance(core, pf.CoreDataset3_0):
        version = "FCS3.0"
    elif isinstance(core, pf.CoreDataset3_1):
        version = "FCS3.1"
    elif isinstance(core, pf.CoreDataset3_2):
        version = "FCS3.2"
    else:
        assert_never(core)

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
            ",".join(str(x + 1) for x in lt.byteord)
            if isinstance(lt.byteord, list)
            else endian_to_order(lt.byteord)
        )

    std_keywords = core.standard_keywords("both", "both")

    n_keywords = (
        len(std_keywords)
        + len(core.nonstandard_keywords)
        + sum(len(m.nonstandard_keywords) for m in core.measurements)
    )

    n_delimiters = n_keywords * 2 + 1
    text_nbytes = (
        n_delimiters
        + sum_dict(std_keywords)
        + sum_dict(core.nonstandard_keywords)
        + +sum(sum_dict(m.nonstandard_keywords) for m in core.measurements)
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


def meas_3_0(i: int) -> tuple[str, pf.Optical3_0 | pf.Temporal3_0]:
    return (
        f"C{i + 1}",
        pf.Optical3_0(
            1.0,
            longname=f"Column{i + 1}",
            wavelength=randrange(500, 700),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
            nonstandard_keywords=nonstd_keywords(i),
        ),
    )


def meas_3_1(i: int) -> tuple[str, pf.Optical3_1 | pf.Temporal3_1]:
    return (
        f"C{i + 1}",
        pf.Optical3_1(
            1.0,
            longname=f"Column{i + 1}",
            wavelengths=[randrange(500, 700)],
            display=(False, randrange(0, 10), randrange(11, 20)),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
            nonstandard_keywords=nonstd_keywords(i),
        ),
    )


def meas_3_2(i: int) -> tuple[str, pf.Optical3_2 | pf.Temporal3_2]:
    return (
        f"C{i + 1}",
        pf.Optical3_2(
            1.0,
            longname=f"Column{i + 1}",
            wavelengths=[randrange(500, 700)],
            display=(False, randrange(0, 10), randrange(11, 20)),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
            measurement_type="phy",
            tag=f"Tag{i + 1}",
            nonstandard_keywords=nonstd_keywords(i),
        ),
    )


def core_3_0_pdp11(
    height: int,
    width: int,
) -> pf.CoreDataset3_0:
    ms: list[tuple[str | None, pf.Optical3_0 | pf.Temporal3_0]] = [
        meas_3_0(i) for i in range(0, width)
    ]
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
    ms: list[tuple[str, pf.Optical3_1 | pf.Temporal3_1]] = [
        meas_3_1(i) for i in range(0, width)
    ]
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
    rs = [Decimal(upper) for _ in range(0, width)]
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
        ("I16", 2**16 - 1),
        ("I16", 2**16 - 1),
        ("I16", 2**16 - 1),
        ("I16", 2**16 - 1),
        ("I32", 2**32 - 1),
        ("I08", 2**8 - 1),
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
    if r[0] == "F":
        return (float(r[1]), pl.Float32)
    elif r[0] == "D":
        return (float(r[1]), pl.Float64)
    elif r[0] == "I08":
        return (r[1], pl.UInt8)
    elif r[0] == "I16":
        return (r[1], pl.UInt16)
    elif r[0] == "I32":
        return (r[1], pl.UInt32)
    elif r[0] == "I64":
        return (r[1], pl.UInt64)
    else:
        assert False, f"invalid datatype {r[1]}"


def core_3_2_a8(height: int, big_endian: bool) -> pf.CoreDataset3_2:
    floats: list[pft.MixedRange] = [("F", Decimal(1e10))] * 380
    ints: list[pft.MixedRange] = [("I32", 2**32 - 1)] * 20
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

    f32: list[pft.MixedRange] = [("F", Decimal(1e10))] * n_cols
    f64: list[pft.MixedRange] = [("D", Decimal(1e10))] * n_cols
    int8: list[pft.MixedRange] = [("I08", 2**8 - 1)] * n_cols
    int16: list[pft.MixedRange] = [("I16", 2**16 - 1)] * n_cols
    int32: list[pft.MixedRange] = [("I32", 2**32 - 1)] * n_cols
    int64: list[pft.MixedRange] = [("I64", 2**64 - 1)] * n_cols

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


def run_bench(
    input_root: Path,
    output_root: Path | None,
    scratch_root: Path,
    names_filter: list[str],
) -> None:
    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = pl.read_csv(input_root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col("name").is_in(names_filter))

    runs = [
        BenchRun(name=n, key=k)
        for n in bench_files["name"]
        for k in BenchKey
        for _ in range(0, TRIAL_NUMBER[k])
    ]

    # loop through each name only once
    for r in set(r for r in runs if r.key == BenchKey.READ_DATA):
        r.check_data(input_root, scratch_root)

    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.run(input_root, scratch_root) for r in runs]

    read_flat_results = [r for r in results if r.key == BenchKey.READ_FLAT]
    read_std_results = [r for r in results if r.key == BenchKey.READ_STD]
    read_data_results = [r for r in results if r.key == BenchKey.READ_DATA]
    write_text_results = [r for r in results if r.key == BenchKey.WRITE_TEXT]
    write_data_results = [r for r in results if r.key == BenchKey.WRITE_DATA]

    def to_df(rs: list[BenchResult], name: str) -> pl.DataFrame:
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {"name": pl.String, full_name: pl.Float32},
        )
        return result_df.group_by("name").agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / pl.col(full_name).count().sqrt()).name.prefix(
                "serr_"
            ),
        )

    read_flat_df = to_df(read_flat_results, "r_flat")
    read_std_df = to_df(read_std_results, "r_std")
    read_data_df = to_df(read_data_results, "r_data")
    write_text_df = to_df(write_text_results, "w_text")
    write_data_df = to_df(write_data_results, "w_data")

    df_analyzed = (
        read_flat_df.join(bench_files, on="name")
        .with_columns(
            # normalize flat TEXT parse time to keyword number and TEXT length in kB
            (pl.col("mean_r_flat_ns") / pl.col("n_keywords")).alias(
                "mean_r_flat_ns_per_kw"
            ),
            (pl.col("serr_r_flat_ns") / pl.col("n_keywords")).alias(
                "serr_r_flat_ns_per_kw"
            ),
            (pl.col("mean_r_flat_ns") / pl.col("text_nbytes") * 1000).alias(
                "mean_r_flat_ns_per_kB"
            ),
            (pl.col("serr_r_flat_ns") / pl.col("text_nbytes") * 1000).alias(
                "serr_r_flat_ns_per_kB"
            ),
        )
        .join(read_std_df, on="name")
        .with_columns(
            # compute the overhead of standardizing TEXT by taking difference of
            # total std run and flat run
            (
                (pl.col("mean_r_std_ns") - pl.col("mean_r_flat_ns"))
                / pl.col("n_keywords")
            ).alias("mean_r_std_diff_ns_per_kw"),
            (
                (
                    pl.col("serr_r_std_ns").pow(2) + pl.col("serr_r_flat_ns").pow(2)
                ).sqrt()
                / pl.col("n_keywords")
            ).alias("serr_r_std_diff_ns_per_kw"),
            # also compute the ratio of standard to flat (no variance since this
            # is really complex
            (pl.col("mean_r_std_ns") / pl.col("mean_r_flat_ns") * 100 - 100).alias(
                "r_std_ratio"
            ),
        )
        .join(read_data_df, on="name")
        .with_columns(
            # compute time taken to read DATA by taking difference of data run
            # and flat run (note DATA was read in flat mode to reduce noise)
            (pl.col("mean_r_data_ns") - pl.col("mean_r_flat_ns")).alias(
                "mean_r_data_diff_ns"
            ),
            (pl.col("serr_r_data_ns").pow(2) + pl.col("serr_r_flat_ns").pow(2))
            .sqrt()
            .alias("serr_r_data_diff_ns"),
        )
        .with_columns(
            # normalize DATA read time to number of kB read and number of
            # values read
            (pl.col("mean_r_data_diff_ns") / pl.col("data_nbytes") * 1000).alias(
                "mean_r_data_diff_ns_per_kB"
            ),
            (pl.col("serr_r_data_diff_ns") / pl.col("data_nbytes") * 1000).alias(
                "serr_r_data_diff_ns_per_kB"
            ),
            (
                pl.col("mean_r_data_diff_ns")
                / pl.col("width")
                / pl.col("height")
                * 1000
            ).alias("mean_r_data_diff_ns_per_value"),
            (
                pl.col("serr_r_data_diff_ns")
                / pl.col("width")
                / pl.col("height")
                * 1000
            ).alias("serr_r_data_diff_ns_per_value"),
        )
        .join(write_text_df, on="name")
        .with_columns(
            # normalize TEXT write time to keyword number and TEXT length in kB
            (pl.col("mean_w_text_ns") / pl.col("n_keywords")).alias(
                "mean_w_text_ns_per_kw"
            ),
            (pl.col("serr_w_text_ns") / pl.col("n_keywords")).alias(
                "serr_w_text_ns_per_kw"
            ),
            (pl.col("mean_w_text_ns") / pl.col("text_nbytes") * 1000).alias(
                "mean_w_text_ns_per_kB"
            ),
            (pl.col("serr_w_text_ns") / pl.col("text_nbytes") * 1000).alias(
                "serr_w_text_ns_per_kB"
            ),
        )
        .join(write_data_df, on="name")
        .with_columns(
            # compute time taken to write DATA by taking difference of DATA run
            # and TEXT run
            (pl.col("mean_w_data_ns") - pl.col("mean_w_text_ns")).alias(
                "mean_w_data_diff_ns"
            ),
            (pl.col("serr_w_data_ns").pow(2) + pl.col("serr_w_text_ns").pow(2))
            .sqrt()
            .alias("serr_w_data_diff_ns"),
        )
        .with_columns(
            # normalize DATA read time to number of kB written and number of
            # values written
            (pl.col("mean_w_data_diff_ns") / pl.col("data_nbytes") * 1000).alias(
                "mean_w_data_diff_ns_per_kB"
            ),
            (pl.col("serr_w_data_diff_ns") / pl.col("data_nbytes") * 1000).alias(
                "serr_w_data_diff_ns_per_kB"
            ),
            (
                pl.col("mean_w_data_diff_ns")
                / pl.col("width")
                / pl.col("height")
                * 1000
            ).alias("mean_w_data_diff_ns_per_value"),
            (
                pl.col("serr_w_data_diff_ns")
                / pl.col("width")
                / pl.col("height")
                * 1000
            ).alias("serr_w_data_diff_ns_per_value"),
        )
        .with_columns(
            # ratio of write to read (no variance because this more complicated than its worth)
            (pl.col("mean_r_flat_ns") / pl.col("mean_w_text_ns") * 100).alias(
                "text_rw_ratio"
            ),
            (pl.col("mean_r_data_ns") / pl.col("mean_w_data_ns") * 100).alias(
                "data_rw_ratio"
            ),
        )
    )

    def fmt_value(mean: str, ci: str, out: str) -> pl.Expr:
        return pl.format(
            "{} (±{}%)",
            pl.col(mean).round(1),
            # 95% confidence interval as percentage of mean
            (pl.col(ci) / pl.col(mean) * 100 * 1.96).round(1),
        ).alias(out)

    metadata_cols = [
        "version",
        pl.col("width").alias("$PAR"),
        pl.col("height").alias("$TOT"),
        pl.col("byteord").alias("$BYTEORD"),
        pl.col("datatypes").alias("$DATATYPE"),
        pl.col("bit_widths").alias("$PnB"),
    ]

    sort_cols = ["byteord", "version", "bit_widths", "datatypes", "height"]

    READ_TEXT_PER_KW = "TEXT read (ns/kw)"
    READ_TEXT_PER_KB = "TEXT read (ns/kB)"
    READ_STD_PER_KW = "Std Overhead (ns/kw)"
    READ_STD_RATIO = "Std Overhead (%)"
    READ_DATA_PER_KB = "DATA read (ns/kB)"
    READ_DATA_PER_VAL = "DATA read (ns/val)"

    WRITE_TEXT_PER_KW = "TEXT write (ns/kw)"
    WRITE_TEXT_PER_KB = "TEXT write (ns/kB)"
    WRITE_DATA_PER_VAL = "DATA write (ns/val)"
    WRITE_DATA_PER_KB = "DATA write (ns/kB)"

    df_analyzed_full = df_analyzed.sort(by=sort_cols).select(
        [
            "name",
            *metadata_cols,
            # read flat
            fmt_value(
                "mean_r_flat_ns_per_kw", "serr_r_flat_ns_per_kw", READ_TEXT_PER_KW
            ),
            fmt_value(
                "mean_r_flat_ns_per_kB", "serr_r_flat_ns_per_kB", READ_TEXT_PER_KB
            ),
            # read std
            fmt_value(
                "mean_r_std_diff_ns_per_kw",
                "serr_r_std_diff_ns_per_kw",
                READ_STD_PER_KW,
            ),
            pl.col("r_std_ratio").round(1).alias(READ_STD_RATIO),
            # read data
            fmt_value(
                "mean_r_data_diff_ns_per_value",
                "serr_r_data_diff_ns_per_value",
                READ_DATA_PER_VAL,
            ),
            fmt_value(
                "mean_r_data_diff_ns_per_kB",
                "serr_r_data_diff_ns_per_kB",
                READ_DATA_PER_KB,
            ),
            # write text
            fmt_value(
                "mean_w_text_ns_per_kw", "serr_w_text_ns_per_kw", WRITE_TEXT_PER_KW
            ),
            fmt_value(
                "mean_w_text_ns_per_kB", "serr_w_text_ns_per_kB", WRITE_TEXT_PER_KB
            ),
            # write data
            fmt_value(
                "mean_w_data_diff_ns_per_value",
                "serr_w_data_diff_ns_per_value",
                WRITE_DATA_PER_VAL,
            ),
            fmt_value(
                "mean_w_data_diff_ns_per_kB",
                "serr_w_data_diff_ns_per_kB",
                WRITE_DATA_PER_KB,
            ),
            # read vs write
            pl.col("text_rw_ratio").round(1).alias("TEXT R:W Ratio (%)"),
            pl.col("data_rw_ratio").round(1).alias("DATA R:W Ratio (%)"),
        ]
    )

    if output_root is None:
        df_analyzed_full.write_csv(sys.stdout, separator="\t")
    else:
        output_root.mkdir(parents=True, exist_ok=True)
        with open(output_root / "analysis.tsv", "w") as f:
            df_analyzed_full.write_csv(f, separator="\t")


def main(args: list[str]) -> None:
    cmd = args[1]
    bench_path = Path(args[2])

    if cmd == "make":
        make_bench_files(bench_path)
    elif cmd == "run":
        output_root = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        run_bench(bench_path, output_root, scratch_root, args[5:])
    else:
        print(f"invalid command: {cmd}")
        exit(1)


main(sys.argv)
