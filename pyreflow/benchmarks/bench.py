import csv
import math
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

BENCH_FILES_NAME = "bench_files.tsv"


class BenchKey(Enum):
    FLAT = "flat"
    STD = "std"
    DATA = "data"


class BenchResult(NamedTuple):
    name: str
    key: BenchKey
    value: float


class BenchFile(NamedTuple):
    name: str
    version: pft.FCSVersion
    height: int
    width: int
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

    def read_flat(self, root: Path, n: int) -> float:
        start = perf_counter_ns()
        for _ in range(0, n):
            pf.api.fcs_read_flat_text(root / self.fcs_name)
        return (perf_counter_ns() - start) / n

    def read_std(self, root: Path, n: int) -> float:
        start = perf_counter_ns()
        for _ in range(0, n):
            pf.api.fcs_read_std_text(root / self.fcs_name, time_meas_pattern=None)
        return (perf_counter_ns() - start) / n

    def read_flat_data(self, root: Path) -> float:
        start = perf_counter_ns()
        pf.api.fcs_read_flat_dataset(root / self.fcs_name)
        end = perf_counter_ns()
        return end - start

    def run(self, root: Path) -> BenchResult:
        print(f"running {self.key.value} test for '{self.name}'")
        if self.key == BenchKey.FLAT:
            value = self.read_flat(root, 1000)
        elif self.key == BenchKey.STD:
            value = self.read_std(root, 1000)
        elif self.key == BenchKey.DATA:
            value = self.read_flat_data(root)
        else:
            assert_never(self.key)
        return BenchResult(name=self.name, key=self.key, value=value)

    def check_data(self, root: Path) -> None:
        """Ensure DATA didn't get screwed up during optimization."""
        out = pf.api.fcs_read_flat_dataset(root / self.fcs_name)
        tsv = pl.read_csv(root / self.tsv_name, separator="\t")
        assert out.dataset.data == tsv


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

    lt = core.layout

    if isinstance(lt, pf.MixedLayout) or isinstance(lt, pf.EndianUintLayout):
        data_nbytes = sum(lt.byte_widths) * height
    elif (
        isinstance(lt, pf.EndianF32Layout)
        or isinstance(lt, pf.OrderedF32Layout)
        or isinstance(lt, pf.OrderedUint32Layout)
    ):
        data_nbytes = 4 * n_values
    elif (
        isinstance(lt, pf.EndianF64Layout)
        or isinstance(lt, pf.OrderedF64Layout)
        or isinstance(lt, pf.OrderedUint64Layout)
    ):
        data_nbytes = 8 * n_values
    elif isinstance(lt, pf.OrderedUint08Layout):
        data_nbytes = n_values
    elif isinstance(lt, pf.OrderedUint16Layout):
        data_nbytes = 2 * n_values
    elif isinstance(lt, pf.OrderedUint24Layout):
        data_nbytes = 3 * n_values
    else:
        assert False, "invalid layout"

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


# TODO there is no way to save a file without truncating bits first, which
# may or may not be what we want. Some files "use" these upper buts for things
def core_3_1(
    width: int,
    layout: pf.EndianUintLayout | pf.EndianF32Layout | pf.EndianF64Layout,
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
    upper = 2 ** (8 * byte_width) if byte_width < 8 else 2**40
    rs = [upper for _ in range(0, width)]
    layout = pf.EndianUintLayout(rs, "big" if big_endian else "little")
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
    layout = pf.EndianF64Layout(rs) if is64 else pf.EndianF32Layout(rs)
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
    layout = pf.EndianUintLayout(
        [2**16] * 4 + [2**32] + [2**8], "big" if big_endian else "little"
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**16, size=height), dtype=pl.UInt16
            )
            for _ in range(0, 4)
        ]
        + [
            pl.Series(
                np.random.uniform(low=0, high=2**32, size=height), dtype=pl.UInt32
            ),
            pl.Series(
                np.random.uniform(low=0, high=2**8, size=height), dtype=pl.UInt64
            ),
        ]
    )
    return core_3_1(6, layout, data)


def core_3_2_a8(height: int, big_endian: bool) -> pf.CoreDataset3_2:
    Range = tuple[Literal["I", "A"], int] | tuple[Literal["F", "D"], Decimal]
    floats: list[Range] = [("F", Decimal(3.4e38))] * 380
    ints: list[Range] = [("I", 2**32)] * 20
    layout = pf.MixedLayout(floats + ints)
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**32, size=height), dtype=pl.UInt32
            )
            for _ in range(0, 20)
        ]
        + [
            pl.Series(
                np.random.uniform(low=0, high=3.4e38, size=height), dtype=pl.Float32
            )
            for _ in range(0, 380)
        ]
    )
    ms = [meas_3_2(i) for i in range(0, 400)]
    core = pf.CoreDataset3_2(ms, layout, data, cyt="WALL-E")
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
    # not depend on height but may have a different overall throughput per
    # keyword since not all keywords correspond to measurements.
    print_files("le_int32_10000x25", core_3_1_int(10000, 25, 4, False))
    print_files("le_int32_10000x75", core_3_1_int(10000, 75, 4, False))
    print_files("le_int32_100000x25", core_3_1_int(100000, 25, 4, False))

    # make a big endian file just for fun (it should be the same as le)
    print_files("be_int32_10000x25", core_3_1_int(10000, 25, 4, True))

    # make some other int sizes
    print_files("le_int16_10000x25", core_3_1_int(10000, 25, 2, False))
    print_files("le_int24_10000x25", core_3_1_int(10000, 25, 3, False))
    print_files("le_int64_10000x25", core_3_1_int(10000, 25, 8, False))

    # make float layouts
    print_files("le_f32_10000x25", core_3_1_float(10000, 25, False))
    print_files("le_f64_10000x25", core_3_1_float(10000, 25, True))

    # add cyflow cube's infamous mixed int layout
    print_files("le_cube_10000x6", core_3_1_cube(10000, False))

    # add BD S8/A8's mixed 32bit layout
    print_files("le_s8_2000x400", core_3_1_cube(2000, False))

    # add fully mixed layout just for fun (not that anyone uses this yet)

    with open(root / BENCH_FILES_NAME, "w") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(BenchFile._fields)
        for b in bench_files:
            row = [
                b.name,
                b.version,
                str(b.height),
                str(b.width),
                str(b.n_keywords),
                str(b.text_nbytes),
                str(b.data_nbytes),
            ]
            w.writerow(row)


def run_bench(root: Path, names_filter: list[str]) -> None:
    TRIAL_NUMBER = 3

    bench_files = pl.read_csv(root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col("name").is_in(names_filter))

    runs = [
        BenchRun(name=n, key=k)
        for n in bench_files["name"]
        for _ in range(0, TRIAL_NUMBER)
        for k in BenchKey
    ]
    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.run(root) for r in runs]

    flat_results = [r for r in results if r.key == BenchKey.FLAT]
    std_results = [r for r in results if r.key == BenchKey.STD]
    data_results = [r for r in results if r.key == BenchKey.DATA]

    def to_df(rs: list[BenchResult], name: str) -> pl.DataFrame:
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {"name": pl.String, full_name: pl.Float32},
        )
        return result_df.group_by("name").agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / math.sqrt(TRIAL_NUMBER)).name.prefix("serr_"),
        )

    flat_df = to_df(flat_results, "flat")
    std_df = to_df(std_results, "std")
    data_df = to_df(data_results, "data")

    df_analyzed = (
        flat_df.join(bench_files, on="name")
        .with_columns(
            # normalize flat TEXT parse time to keyword number and TEXT length in kB
            (pl.col("mean_flat_ns") / pl.col("n_keywords")).alias(
                "mean_flat_ns_per_kw"
            ),
            (pl.col("serr_flat_ns") / pl.col("n_keywords") * 1.96).alias(
                "ci_flat_ns_per_kw"
            ),
            (pl.col("mean_flat_ns") / pl.col("text_nbytes") * 1000).alias(
                "mean_flat_ns_per_kB"
            ),
            (pl.col("serr_flat_ns") / pl.col("text_nbytes") * 1000 * 1.96).alias(
                "ci_flat_ns_per_kB"
            ),
        )
        .join(std_df, on="name")
        .with_columns(
            # compute the overhead of standardizing TEXT by taking difference of
            # total std run and flat run
            (pl.col("mean_std_ns") - pl.col("mean_flat_ns")).alias("mean_std_diff_ns"),
            (
                (pl.col("serr_std_ns").pow(2) + pl.col("serr_flat_ns").pow(2)).sqrt()
            ).alias("serr_std_diff_ns"),
        )
        .with_columns(
            # normalize standardization overhead to number of keywords
            (pl.col("mean_std_diff_ns") / pl.col("n_keywords")).alias(
                "mean_std_diff_ns_per_kw"
            ),
            (pl.col("serr_std_diff_ns") / pl.col("n_keywords") * 1.96).alias(
                "ci_std_diff_ns_per_kw"
            ),
        )
        .join(data_df, on="name")
        .with_columns(
            # compute time taken to read DATA by taking difference of data run
            # and flat run (note DATA was read in flat mode to reduce noise)
            (pl.col("mean_data_ns") - pl.col("mean_flat_ns")).alias(
                "mean_data_diff_ns"
            ),
            (pl.col("serr_data_ns").pow(2) + pl.col("serr_flat_ns").pow(2))
            .sqrt()
            .alias("serr_data_diff_ns"),
        )
        .with_columns(
            (pl.col("serr_data_diff_ns") * 1.96).alias("ci_data_diff_ns"),
            # normalize DATA read time to number of kB read and number of
            # values read
            (pl.col("mean_data_diff_ns") / pl.col("data_nbytes") * 1000).alias(
                "mean_data_diff_ns_per_kB"
            ),
            (pl.col("serr_data_diff_ns") / pl.col("data_nbytes") * 1000 * 1.96).alias(
                "ci_data_diff_ns_per_kB"
            ),
            (pl.col("mean_data_diff_ns") / pl.col("width") / pl.col("height")).alias(
                "mean_data_diff_ns_per_value"
            ),
            (
                pl.col("serr_data_diff_ns") / pl.col("width") / pl.col("height") * 1.96
            ).alias("ci_data_diff_ns_per_value"),
        )
    )

    def fmt_value(mean: str, ci: str, out: str) -> pl.Expr:
        return pl.format(
            "{} (+/-{})",
            pl.col(mean).round(1),
            pl.col(ci).round(1),
        ).alias(out)

    df_analyzed_flat = df_analyzed.select(
        [
            "name",
            fmt_value(
                "mean_flat_ns_per_kw",
                "ci_flat_ns_per_kw",
                "TEXT throughput (ns/keyword)",
            ),
            fmt_value(
                "mean_flat_ns_per_kB",
                "ci_flat_ns_per_kB",
                "TEXT throughput (ns/kB)",
            ),
        ]
    )

    df_analyzed_std = df_analyzed.select(
        [
            "name",
            fmt_value(
                "mean_std_diff_ns_per_kw",
                "ci_std_diff_ns_per_kw",
                "Standardization Overhead (ns/keyword)",
            ),
        ]
    )

    df_analyzed_data = df_analyzed.select(
        [
            "name",
            fmt_value(
                "mean_data_diff_ns_per_kB",
                "ci_data_diff_ns_per_kB",
                "DATA throughput (ns/kB)",
            ),
            fmt_value(
                "mean_data_diff_ns_per_value",
                "ci_data_diff_ns_per_value",
                "DATA throughput (ns/value)",
            ),
        ]
    )

    print(df_analyzed_flat)
    print(df_analyzed_std)
    print(df_analyzed_data)


def main(args: list[str]) -> None:
    cmd = args[1]
    bench_path = Path(args[2])

    if cmd == "make":
        make_bench_files(bench_path)
    elif cmd == "run":
        # TODO add results output dir
        run_bench(bench_path, args[3:])
    else:
        print(f"invalid command: {cmd}")
        exit(1)


main(sys.argv)
