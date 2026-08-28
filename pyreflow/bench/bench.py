#! /usr/bin/env python3

import csv
import os
import select
import gc
import re
import sys
import tempfile as tf
import textwrap as tw
import platform as plm
import flowio as fi  # type: ignore
import fcsparser as fp  # type: ignore
import subprocess as sp
from dataclasses import dataclass
from datetime import datetime, UTC
from collections.abc import Callable
from typing import NamedTuple, assert_never, Literal
from pathlib import Path
from decimal import Decimal
from random import randrange, shuffle
from time import perf_counter_ns
from enum import Enum
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from plotnine import (
    ggplot,
    aes,
    geom_col,
    geom_hline,
    geom_errorbar,
    coord_flip,
    labs,
    scale_fill_discrete,
    facet_wrap,
    theme,
)

import polars as pl
import polars.selectors as cs
from polars.testing import assert_frame_equal
import numpy as np

import pyreflow as pf
import pyreflow.pydantic as pfp
import pyreflow.typing as pft

FIREFLOW = "fireflow"
FIREFLOW_FIX = "fireflow_fix"
FCSPARSER = "fcsparser"
FLOWIO = "flowio"
FLOWCORE = "flowCore"

LIBRARIES = [FIREFLOW, FIREFLOW_FIX, FCSPARSER, FLOWIO, FLOWCORE]

BENCH_FILES_NAME = "bench_files.tsv"

# column names

BENCH_NAME = "name"
BYTEORD = "byteord"
VERSION = "version"
DATATYPES = "datatypes"
WIDTH = "n_cols"
HEIGHT = "n_rows"
N_KEYWORDS = "n_keywords"
TEXT_NBYTES = "text_nbytes"
DATA_NBYTES = "data_nbytes"
LIBRARY = "library"

READ_TEXT_RUNS = "read_text_runs"
WRITE_TEXT_RUNS = "write_text_runs"
READ_DATA_RUNS = "read_data_runs"
WRITE_DATA_RUNS = "write_data_runs"
READ_STD_RUNS = "read_std_runs"
READ_DATA_RNG_RUNS = "read_data_rng_runs"
READ_DATA_CRC_RUNS = "read_data_crc_runs"

MEAN_READ_TEXT_NS = "mean_r_text_ns"
MEAN_READ_TEXT_NS_PER_KW = "mean_r_text_ns_per_kw"
MEAN_READ_TEXT_NS_PER_KIB = "mean_r_text_ns_per_kiB"
SERR_READ_TEXT_NS = "serr_r_text_ns"
SERR_READ_TEXT_NS_PER_KW = "serr_r_text_ns_per_kw"
SERR_READ_TEXT_NS_PER_KIB = "serr_r_text_ns_per_kiB"

MEAN_READ_DATA_NS = "mean_r_data_ns"
MEAN_READ_DATA_DIFF_NS = "mean_r_data_diff_ns"
MEAN_READ_DATA_DIFF_NS_PER_KIB = "mean_r_data_diff_ns_per_kb"
MEAN_READ_DATA_DIFF_NS_PER_VAL = "mean_r_data_diff_ns_per_value"

SERR_READ_DATA_NS = "serr_r_data_ns"
SERR_READ_DATA_DIFF_NS = "serr_r_data_diff_ns"
SERR_READ_DATA_DIFF_NS_PER_KIB = "serr_r_data_diff_ns_per_kb"
SERR_READ_DATA_DIFF_NS_PER_VAL = "serr_r_data_diff_ns_per_value"

MEAN_WRITE_TEXT_NS = "mean_w_text_ns"
MEAN_WRITE_TEXT_NS_PER_KW = "mean_w_text_ns_per_kw"
MEAN_WRITE_TEXT_NS_PER_KIB = "mean_w_text_ns_per_kiB"
SERR_WRITE_TEXT_NS = "serr_w_text_ns"
SERR_WRITE_TEXT_NS_PER_KW = "serr_w_text_ns_per_kw"
SERR_WRITE_TEXT_NS_PER_KIB = "serr_w_text_ns_per_kiB"

MEAN_WRITE_DATA_NS = "mean_w_data_ns"
MEAN_WRITE_DATA_DIFF_NS = "mean_w_data_diff_ns"
MEAN_WRITE_DATA_DIFF_NS_PER_KIB = "mean_w_data_diff_ns_per_kiB"
MEAN_WRITE_DATA_DIFF_NS_PER_VAL = "mean_w_data_diff_ns_per_value"

SERR_WRITE_DATA_NS = "serr_w_data_ns"
SERR_WRITE_DATA_DIFF_NS = "serr_w_data_diff_ns"
SERR_WRITE_DATA_DIFF_NS_PER_KIB = "serr_w_data_diff_ns_per_kb"
SERR_WRITE_DATA_DIFF_NS_PER_VAL = "serr_w_data_diff_ns_per_value"

# MEAN_READ_STD_NS = "mean_r_std_ns"
# SERR_READ_STD_NS = "serr_r_std_ns"

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
MEAN_READ_DATA_CRC_DIFF_NS_PER_KIB = "mean_r_data_crc_diff_ns_per_kiB"
SERR_READ_DATA_CRC_NS = "serr_r_data_crc_ns"
SERR_READ_DATA_CRC_DIFF_NS = "serr_r_data_crc_diff_ns"
SERR_READ_DATA_CRC_DIFF_NS_PER_KIB = "serr_r_data_crc_diff_ns_per_kiB"

READ_HEADER_NS = "read_header_ns"
READ_TEXT_NS = "read_text_ns"
READ_STD_NS = "read_std_ns"
READ_ALL_STD_NS = "read_all_std_ns"
READ_SCHEMA_NS = "read_schema_ns"
READ_DATA_NS = "read_data_ns"
CHECK_RANGE_NS = "check_range_ns"
READ_CRC_NS = "read_crc_ns"
RS_TOTAL_NS = "rs_total_ns"
READ_TEXT_TOTAL_NS = "read_text_total_ns"
READ_STD_TOTAL_NS = "read_std_total_ns"
TOTAL_NS = "total_ns"
PY_OVERHEAD_NS = "py_overhead_ns"

here = Path(sys.argv[0]).parent


class FFBenchKey(Enum):
    """Testing modes for fireflow.

    Unlike other libraries which only support reading/writing TEXT/DATA,
    fireflow additionally supports keyword standardization, CRC checks, and $PnR
    checks/truncation. Each of these are extra steps which may be optionally
    applied that don't exist elsewhere. Therefore they are measured separately.

    """

    READ_FLAT = "read_flat"
    READ_DATA = "read_data"
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


class FlowCoreBenchKey(Enum):
    """Testing modes for flowCore."""

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


type AnyBenchKey = (
    FlowCoreBenchKey | tuple[FFBenchKey, bool] | FCSParserBenchKey | FlowIOBenchKey
)


FF_TRIAL_NUMBER = {
    FFBenchKey.READ_FLAT: 100,
    FFBenchKey.READ_DATA: 30,
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

FLOWCORE_TRIAL_NUMBER = {
    FlowCoreBenchKey.READ_TEXT: 3,
    FlowCoreBenchKey.READ_DATA: 3,
    FlowCoreBenchKey.WRITE_TEXT: 3,
    FlowCoreBenchKey.WRITE_DATA: 3,
}


type UintDType = type[pl.UInt8 | pl.UInt16 | pl.UInt32 | pl.UInt64]

type DType = UintDType | type[pl.Float32 | pl.Float64]

type Range = tuple[Literal["I", "A"], int] | tuple[Literal["F", "D"], Decimal]


class FFInternalResult(NamedTuple):
    name: str
    scalpal: bool
    total_ns: int
    read_header_ns: int
    read_text_ns: int
    read_std_ns: int
    read_schema_ns: int
    read_data_ns: int
    check_range_ns: int
    read_crc_ns: int


@dataclass(frozen=True)
class BenchResult[X]:
    name: str
    key: X
    value: float


type FCSParserBenchResult = BenchResult[FCSParserBenchKey]
type FlowIOBenchResult = BenchResult[FlowIOBenchKey]
type FlowCoreBenchResult = BenchResult[FlowCoreBenchKey]
type FFBenchResult = BenchResult[tuple[FFBenchKey, bool]]


type AnyBenchResult = (
    FCSParserBenchResult | FlowIOBenchResult | FlowCoreBenchResult | FFBenchResult
)


class BenchFile(NamedTuple):
    name: str
    version: pft.FCSVersion
    n_rows: int
    n_cols: int
    byteord: str
    datatypes: str
    n_keywords: int
    text_nbytes: int
    data_nbytes: int
    description: str


@dataclass(frozen=True)
class BenchRun[X, Y]:
    name: str
    key: X

    def fcs_name(self, suffix: str | None = None) -> Path:
        if suffix is not None:
            return Path(f"{self.name}_{suffix}.fcs")
        return Path(f"{self.name}.fcs")

    def run(self, input_root: Path, scratch_root: Path) -> Y:
        raise NotImplementedError


@dataclass(frozen=True)
class FlowIOBenchRun(BenchRun[FlowIOBenchKey, FlowIOBenchResult]):
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
        gc.collect()
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
        return BenchResult(self.name, self.key, value)


@dataclass(frozen=True)
class FCSParserBenchRun(BenchRun[FCSParserBenchKey, FCSParserBenchResult]):
    """A benchmark run for fcsparser."""

    def read_text(self, root: Path) -> float:
        start = perf_counter_ns()
        fp.parse(root / self.fcs_name(), meta_data_only=True)
        return perf_counter_ns() - start

    def read_data(self, root: Path) -> float:
        start = perf_counter_ns()
        # set reformat to false to do less work and get a cleaner measurement of
        # how fast the DATA parser really is
        _, _ = fp.parse(root / self.fcs_name(), reformat_meta=False)
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> FCSParserBenchResult:
        gc.collect()
        if self.key == FCSParserBenchKey.READ_TEXT:
            value = self.read_text(input_root)
        elif self.key == FCSParserBenchKey.READ_DATA:
            value = self.read_data(input_root)
        else:
            assert_never(self.key)
        return BenchResult(self.name, self.key, value)


@dataclass(frozen=True)
class FlowCoreBenchRun(BenchRun[FlowCoreBenchKey, FlowCoreBenchResult]):
    """A benchmark run for flowCore."""

    py_to_r: Path
    r_to_py: Path

    def call_flowcore(self, cmd: str) -> float:
        data = call_flowcore_inner(self.py_to_r, self.r_to_py, cmd)
        return float(data.strip())

    def read_text(self, root: Path) -> float:
        fcs_path = (root / self.fcs_name()).relative_to(here)
        return self.call_flowcore(f"read text {fcs_path}")

    def read_data(self, root: Path) -> float:
        fcs_path = (root / self.fcs_name()).relative_to(here)
        return self.call_flowcore(f"read data {fcs_path}")

    def write_text(self, input_root: Path, scratch_root: Path) -> float:
        in_path = (input_root / self.fcs_name()).relative_to(here)
        out_path = scratch_root / self.fcs_name("flowcore_write_text")
        return self.call_flowcore(f"write text {in_path} {out_path}")

    def write_data(self, input_root: Path, scratch_root: Path) -> float:
        in_path = (input_root / self.fcs_name()).relative_to(here)
        out_path = scratch_root / self.fcs_name("flowcore_write_text")
        return self.call_flowcore(f"write data {in_path} {out_path}")

    def run(self, input_root: Path, scratch_root: Path) -> FlowCoreBenchResult:
        gc.collect()
        if self.key == FlowCoreBenchKey.READ_TEXT:
            value = self.read_text(input_root)
        elif self.key == FlowCoreBenchKey.READ_DATA:
            value = self.read_data(input_root)
        elif self.key == FlowCoreBenchKey.WRITE_TEXT:
            value = self.write_text(input_root, scratch_root)
        elif self.key == FlowCoreBenchKey.WRITE_DATA:
            value = self.write_data(input_root, scratch_root)
        else:
            assert_never(self.key)
        return BenchResult(self.name, self.key, value)


@dataclass(frozen=True)
class FFBenchRun(BenchRun[FFBenchKey, FFBenchResult]):
    """A benchmark run for fireflow."""

    scalpal: bool

    @property
    def tsv_name(self) -> Path:
        return Path(f"{self.name}.tsv")

    def read_flat(self, root: Path) -> float:
        conf = pfp.PyreflowReadFlatTEXTConfig()
        if self.scalpal:
            conf = conf.new_scalpal()
        start = perf_counter_ns()
        conf.read_flat_text(root / self.fcs_name())
        return perf_counter_ns() - start

    def read_flat_data(
        self,
        root: Path,
        check_range: bool,
        compute_crc: bool,
    ) -> float:
        conf = pfp.PyreflowReadFlatDatasetConfig()
        if self.scalpal:
            conf = conf.new_scalpal()
        conf.over_range_action = "warn" if check_range else "none"
        conf.compute_crc = "always" if compute_crc else "never"

        start = perf_counter_ns()
        conf.read_flat_dataset(root / self.fcs_name())
        end = perf_counter_ns()
        return end - start

    def write_text(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_text(
            input_root / self.fcs_name(), time_meas_pattern=None
        )
        start = perf_counter_ns()
        mode = "scalpal" if self.scalpal else "clean"
        core.write_text(scratch_root / self.fcs_name(f"ff_write_text_{mode}"))
        end = perf_counter_ns()
        return end - start

    def write_data(self, input_root: Path, scratch_root: Path) -> float:
        core, _ = pf.api.fcs_read_std_dataset(
            input_root / self.fcs_name(), time_meas_pattern=None
        )
        start = perf_counter_ns()
        mode = "scalpal" if self.scalpal else "clean"
        core.write_dataset(scratch_root / self.fcs_name(f"ff_write_data_{mode}"))
        end = perf_counter_ns()
        return end - start

    def run(self, input_root: Path, scratch_root: Path) -> FFBenchResult:
        if self.key == FFBenchKey.READ_FLAT:
            value = self.read_flat(input_root)
        elif self.key == FFBenchKey.READ_DATA:
            value = self.read_flat_data(input_root, False, False)
        elif self.key == FFBenchKey.WRITE_TEXT:
            value = self.write_text(input_root, scratch_root)
        elif self.key == FFBenchKey.WRITE_DATA:
            value = self.write_data(input_root, scratch_root)
        else:
            assert_never(self.key)
        return BenchResult(self.name, (self.key, self.scalpal), value)


class FFInternalBenchRun(NamedTuple):
    """A benchmark run for fireflow (internal testing)."""

    name: str
    scalpal: bool

    def fcs_name(self, suffix: str | None = None) -> Path:
        if suffix is not None:
            return Path(f"{self.name}_{suffix}.fcs")
        return Path(f"{self.name}.fcs")

    def read_std_data(self, root: Path) -> FFInternalResult:
        conf = pfp.PyreflowReadStdDatasetConfig()
        if self.scalpal:
            conf = conf.new_scalpal()
        conf.over_range_action = "warn"
        conf.compute_crc = "always"
        conf.time_meas_pattern = None

        target = root / self.fcs_name()

        gc.collect()
        start = perf_counter_ns()
        _, diag = conf.read_std_dataset(target)
        end = perf_counter_ns()

        total = end - start

        fd = diag.flat_diagnostics
        ds = diag.dataset
        sd = ds.std_diagnostics
        dds = ds.dataset_diagnostics

        ret = FFInternalResult(
            self.name,
            self.scalpal,
            total,
            fd.header_supp.header.read_header_ns,
            fd.read_text_ns,
            sd.read_std_ns,
            sd.schema_diagnostics.read_schema_ns,
            dds.read_data_ns,
            dds.check_range_ns,
            dds.read_crc_ns,
        )
        return ret


type AnyBenchRun = FCSParserBenchRun | FlowIOBenchRun | FlowCoreBenchRun | FFBenchRun


def call_flowcore_inner(py_to_r: Path, r_to_py: Path, cmd: str) -> bytes:
    with open(py_to_r, "w") as f:
        f.write(cmd)
    # Wait for R to finish running flowcore for 5 seconds; if we hear
    # nothing assume something terrible happened and scream (loudly).
    fd = os.open(r_to_py, os.O_RDONLY | os.O_NONBLOCK)
    r, _, _ = select.select([fd], [], [], 5.0)
    if len(r) == 0:
        raise TimeoutError("Writer never showed up. Rude.")
    data = os.read(fd, 4096)
    os.close(fd)
    return data


def get_runs(k: AnyBenchKey) -> int:
    if isinstance(k, tuple) and isinstance(k[0], FFBenchKey):
        return FF_TRIAL_NUMBER[k[0]]
    elif isinstance(k, FlowCoreBenchKey):
        return FLOWCORE_TRIAL_NUMBER[k]
    elif isinstance(k, FCSParserBenchKey):
        return FCSPARSER_TRIAL_NUMBER[k]
    elif isinstance(k, FlowIOBenchKey):
        return FLOWIO_TRIAL_NUMBER[k]
    else:
        assert_never(k)


def core_to_benchfile(name: str, core: pft.AnyCoreDataset, desc: str) -> BenchFile:
    def sum_dict(xs: dict[str, str]) -> int:
        return sum(len(k) + len(v) for k, v in xs.items())

    version = core.version
    n_rows = core.data.height
    n_cols = core.data.width

    n_values = n_rows * n_cols

    lt = core.data_schema

    if isinstance(lt, pf.MixedDataSchema | pf.VariableUintDataSchema):
        data_nbytes = sum(lt.byte_widths) * n_rows
    elif isinstance(lt, pft.MatrixDataSchema):
        data_nbytes = lt.byte_width * n_values
    else:
        assert False, "invalid layout"

    datatypes: str

    if isinstance(lt, pf.MixedDataSchema):
        datatypes = ",".join(sorted({t for (t, _) in lt.typed_ranges}))
    elif isinstance(lt, pft.MatrixDataSchema) and isinstance(lt, pft.NumericDataSchema):
        prefix = "F" if lt.is_float else "U"
        val_width = lt.byte_width * 8
        datatypes = f"{prefix}{val_width}"
    elif isinstance(lt, pf.VariableUintDataSchema):
        datatypes = ",".join(sorted({f"U{w * 8:02}" for w in lt.byte_widths}))
    else:
        assert False, "invalid layout"

    byteord: str

    def endian_to_order(e: pft.Endian) -> str:
        return "1,2,3,4" if e == "little" else "4,3,2,1"

    if isinstance(lt, pft.BigLittleDataSchema):
        byteord = endian_to_order(lt.endian)
    elif isinstance(lt, pft.OrderedDataSchema):
        byteord = (
            ",".join(map(str, lt.byteord))
            if isinstance(lt.byteord, list)
            else endian_to_order(lt.byteord)
        )
    else:
        assert False, "invalid layout"

    std_keywords = core.standard_keywords("both", "both")

    n_keywords = len(std_keywords) + len(core.nonstandard_keywords)

    n_delimiters = n_keywords * 2 + 1
    text_nbytes = (
        n_delimiters + sum_dict(std_keywords) + sum_dict(core.nonstandard_keywords)
    )

    return BenchFile(
        name,
        version,
        n_rows=n_rows,
        n_cols=n_cols,
        byteord=byteord,
        datatypes=datatypes,
        text_nbytes=text_nbytes,
        data_nbytes=data_nbytes,
        n_keywords=n_keywords,
        description=desc,
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


def meas_2_0(i: int) -> pft.Measurement2_0:
    return (
        f"C{i + 1}",
        pf.Optical2_0(
            longname=f"Column{i + 1}",
            wavelength=randrange(500, 700),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
        ),
        (1.0, 1.0),
    )


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
            display=(False, randrange(10), randrange(11, 20)),
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
            display=(False, randrange(10), randrange(11, 20)),
            power=randrange(1, 1000),
            detector_voltage=randrange(1, 1000),
            measurement_type="phy",
            tag=f"Tag{i + 1}",
        ),
        1.0,
    )


def core_2_0(
    width: int,
    layout: pf.OrderedUintDataSchema
    | pf.OrderedF32DataSchema
    | pf.OrderedF64DataSchema,
    data: pl.DataFrame,
) -> pf.CoreDataset2_0:
    ms: pft.Measurements2_0 = [meas_2_0(i) for i in range(width)]
    core = pf.CoreDataset2_0(ms, layout, data)
    return core


def core_3_0(
    width: int,
    layout: pf.OrderedUintDataSchema
    | pf.OrderedF32DataSchema
    | pf.OrderedF64DataSchema,
    data: pl.DataFrame,
) -> pf.CoreDataset3_0:
    ms: pft.Measurements3_0 = [meas_3_0(i) for i in range(width)]
    core = pf.CoreDataset3_0(ms, layout, data)
    return core


def core_3_1(
    width: int,
    layout: pf.SingleUintDataSchema
    | pf.VariableUintDataSchema
    | pf.BigLittleF32DataSchema
    | pf.BigLittleF64DataSchema,
    data: pl.DataFrame,
) -> pf.CoreDataset3_1:
    ms: pft.Measurements3_1 = [meas_3_1(i) for i in range(width)]
    core = pf.CoreDataset3_1(ms, layout, data)
    return core


def core_3_0_pdp11(
    height: int,
    width: int,
) -> pf.CoreDataset3_0:
    rs = [2**32 - 1 for _ in range(width)]
    # wonky byteord...
    layout = pf.OrderedUintDataSchema(rs, byteord=[3, 4, 1, 2])
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**32 - 1, size=height),
                dtype=pl.UInt32,
            )
            for _ in range(width)
        ]
    )
    return core_3_0(width, layout, data)


def width_to_uint_type(byte_width: int) -> UintDType:
    if byte_width == 1:
        return pl.UInt8
    elif byte_width == 2:
        return pl.UInt16
    elif byte_width < 5:
        return pl.UInt32
    else:
        return pl.UInt64


def core_2_0_int(
    height: int, width: int, byte_width: pft.ByteWidth, big_endian: bool
) -> pf.CoreDataset2_0:
    upper = 2 ** (8 * byte_width) - 1
    rs = [upper for _ in range(width)]
    layout = pf.OrderedUintDataSchema(
        rs,
        byte_width=byte_width,
        byteord="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=width_to_uint_type(byte_width),
            )
            for _ in range(width)
        ]
    )
    return core_2_0(width, layout, data)


def core_3_0_int(
    height: int, width: int, byte_width: pft.ByteWidth, big_endian: bool
) -> pf.CoreDataset3_0:
    upper = 2 ** (8 * byte_width) - 1
    rs = [upper for _ in range(width)]
    layout = pf.OrderedUintDataSchema(
        rs,
        byte_width=byte_width,
        byteord="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=width_to_uint_type(byte_width),
            )
            for _ in range(width)
        ]
    )
    return core_3_0(width, layout, data)


def core_3_2_int(
    height: int, width: int, byte_width: pft.ByteWidth, big_endian: bool
) -> pf.CoreDataset3_0:
    upper = 2 ** (8 * byte_width) - 1
    rs = [upper for _ in range(width)]
    layout = pf.OrderedUintDataSchema(
        rs,
        byte_width=byte_width,
        byteord="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=width_to_uint_type(byte_width),
            )
            for _ in range(width)
        ]
    )
    return core_3_0(width, layout, data)


def core_3_1_int(
    height: int, width: int, byte_width: pft.ByteWidth, big_endian: bool
) -> pf.CoreDataset3_1:
    upper = 2 ** (8 * byte_width) - 1
    rs = [upper for _ in range(width)]
    layout = pf.SingleUintDataSchema(
        rs,
        byte_width=byte_width,
        endian="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=width_to_uint_type(byte_width),
            )
            for _ in range(width)
        ]
    )
    return core_3_1(width, layout, data)


def core_3_1_float(height: int, width: int, is64: bool) -> pf.CoreDataset3_1:
    upper = 1e10
    rs = [upper for _ in range(width)]
    layout = pf.BigLittleF64DataSchema(rs) if is64 else pf.BigLittleF32DataSchema(rs)
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=upper, size=height),
                dtype=pl.Float64 if is64 else pl.Float32,
            )
            for _ in range(width)
        ]
    )
    return core_3_1(width, layout, data)


def core_3_1_cube(height: int, big_endian: bool) -> pf.CoreDataset3_1:
    # per https://github.com/RGLab/flowCore/issues/46, Nx16+32+8
    N_OPTICAL = 12
    optical: list[pft.VariableBitmask] = [(2, 2**16 - 1)] * N_OPTICAL
    rs: list[pft.VariableBitmask] = [*optical, (4, 2**32 - 1), (1, 2**8 - 1)]
    layout = pf.VariableUintDataSchema(
        rs,
        endian="big" if big_endian else "little",
    )
    data = pl.DataFrame(
        [
            pl.Series(
                np.random.uniform(low=0, high=2**16 - 1, size=height), dtype=pl.UInt16
            )
            for _ in range(N_OPTICAL)
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
    return core_3_1(N_OPTICAL + 2, layout, data)


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
    ms = [meas_3_2(i) for i in range(len(rs))]
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
    ms = [meas_3_2(i) for i in range(len(rs))]
    core = pf.CoreDataset3_2(ms, layout, data, cyt="GLaDOS")
    return core


def make_bench_files(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    bench_files = []

    def print_files(name: str, core: pft.AnyCoreDataset, desc: str) -> None:
        print(f"Writing files for '{name}'")
        core.write_dataset(root / Path(f"{name}.fcs"))
        core.data.write_csv(
            root / Path(f"{name}.tsv"),
            separator="\t",
        )
        bench_files.append(core_to_benchfile(name, core, desc))

    # Make three different sizes of this to demonstrate how time changes with
    # width and height. We expect that for a given datatype, normalized DATA
    # throughput should not depend on width or height. TEXT throughput should
    # not depend on height but should depend on width. Standardization overhead
    # should depend on FCS version and width.
    i32_name = "i32_31_10000x25"
    print_files(
        i32_name,
        core_3_1_int(10000, 25, 4, False),
        (
            "32-bit unsigned integer data in little-endian in FCS3.1. "
            "This matches a typical file for instruments that don't use floating "
            "point data."
        ),
    )

    # make different sizes of the same file
    print_files(
        "i32_31_10000x75",
        core_3_1_int(10000, 75, 4, False),
        f"Same as '{i32_name}' but wider.",
    )
    print_files(
        "i32_31_100000x25",
        core_3_1_int(100000, 25, 4, False),
        f"Same as '{i32_name}' but with more events.",
    )
    print_files(
        "i32_31_100000x75",
        core_3_1_int(100000, 75, 4, False),
        f"Same as '{i32_name}' but wider and with more events.",
    )

    # make different FCS versions of the same file
    print_files(
        "i32_20_10000x25",
        core_2_0_int(10000, 25, 4, False),
        f"Same as '{i32_name}' but in FCS 2.0.",
    )
    print_files(
        "i32_30_10000x25",
        core_3_0_int(10000, 25, 4, False),
        f"Same as '{i32_name}' but in FCS 3.0.",
    )
    print_files(
        "i32_32_10000x25",
        core_3_2_int(10000, 25, 4, False),
        f"Same as '{i32_name}' but in FCS 3.2.",
    )

    # Make a mixed byteord file just for fun, it should be way slower. This
    # also helps test a 3.0 file vs other 3.1 files
    print_files(
        "i32_30_mx_10000x25",
        core_3_0_pdp11(10000, 25),
        (
            f"Same as '{i32_name}' but with PDP-11 byte order and in FCS3.0, which "
            "is the latest standard that allows this schema to exist."
        ),
    )

    # make a big endian file just for fun (it should be the same as le)
    print_files(
        "i32_30_be_10000x25",
        core_3_0_int(10000, 25, 4, True),
        f"Same as '{i32_name}' but with big-endian byte order.",
    )

    # make some other int sizes
    print_files(
        "i16_31_10000x25",
        core_3_1_int(10000, 25, 2, False),
        "16-bit unsigned integer data. Some older instruments still use this bit width.",
    )
    print_files(
        "i24_31_10000x25",
        core_3_1_int(10000, 25, 3, False),
        (
            f"Like '{i32_name}' but 24-bit. This is much rarer than 16-bit files, "
            "but some older instruments still use this bit width. "
            "This is also a good width to test since it isn't a power of 2."
        ),
    )
    print_files(
        "i64_31_10000x25",
        core_3_1_int(10000, 25, 8, False),
        (
            f"Like '{i32_name}' but 64-bit. Practically no machine uses this width, "
            "at least not explicitly. However, some machines have Time measurements "
            "which are actually 64-bit numbers split across 2 columns. There is also "
            "nothing stopping anyone from writing such a file manually."
        ),
    )

    # make float layouts
    print_files(
        "f32_31_10000x25",
        core_3_1_float(10000, 25, False),
        f"Like '{i32_name}' but with 32-bit floats. This is extremely common.",
    )
    print_files(
        "f64_31_10000x25",
        core_3_1_float(10000, 25, True),
        (
            f"Like '{i32_name}' but with 64-bit floats. Practically no machine uses "
            "this datatype, but nothing is stopping someone from writing a file manually."
        ),
    )

    # add cyflow cube's infamous mixed width layout
    print_files(
        "cube_10000x14",
        core_3_1_cube(10000, False),
        (
            "The Partec CyFlow Cube 6 layout. This is one of a few machines that "
            "uses mixed integer widths (Stratedigm broadly being the other vendor who "
            "does this). In this specific case, the layout has 12 optical channels at "
            "16-bit, one time channel at 32-bit, and a doublet mask at 8-bit. The exact "
            "machine does not matter much; this is simply a representative case to test "
            "variable bit width parsing."
        ),
    )

    # add BD S8/A8's mixed 32bit layout
    print_files(
        "s8_10000x400",
        core_3_2_a8(10000, False),
        (
            "The BD FACSDiscover S8 (or A8) layout. At time of writing, this is the only "
            "known machine that explicitly produces FCS 3.2 files. This standard is required "
            "because it includes a mix of float and integer data (all at 32-bit). `fireflow` "
            "also has optimizations for mixed data like this that is all the same width (it "
            "'cheats' by reading it all as one data type and then casting). The exact "
            "machine does not matter; this is a representative case meant to test this "
            "layout."
        ),
    )

    # layout with random mixed-width/type data, nobody uses this but it is a
    # good test since it should be the hardest to process
    print_files(
        "mixrand_10000x90",
        core_3_2_random_mixed(10000, False),
        (
            "An FCS 3.2 file with totally mixed numeric data types (not including ASCII). "
            "No machine is known to use this format, but it is useful for testing purposes "
            "since it represents the most complex data layout a parser will need to process."
        ),
    )

    with open(root / BENCH_FILES_NAME, "w") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(BenchFile._fields)
        for b in bench_files:
            row = [
                b.name,
                b.version,
                str(b.n_rows),
                str(b.n_cols),
                str(b.byteord),
                str(b.datatypes),
                str(b.n_keywords),
                str(b.text_nbytes),
                str(b.data_nbytes),
                str(b.description),
            ]
            w.writerow(row)


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
            (pl.col(MEAN_READ_TEXT_NS) / pl.col(TEXT_NBYTES) * 1024).alias(
                MEAN_READ_TEXT_NS_PER_KIB
            ),
            (pl.col(SERR_READ_TEXT_NS) / pl.col(TEXT_NBYTES) * 1024).alias(
                SERR_READ_TEXT_NS_PER_KIB
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
            (pl.col(MEAN_READ_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1024).alias(
                MEAN_READ_DATA_DIFF_NS_PER_KIB
            ),
            (pl.col(SERR_READ_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1024).alias(
                SERR_READ_DATA_DIFF_NS_PER_KIB
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
            (pl.col(MEAN_WRITE_TEXT_NS) / pl.col(TEXT_NBYTES) * 1024).alias(
                MEAN_WRITE_TEXT_NS_PER_KIB
            ),
            (pl.col(SERR_WRITE_TEXT_NS) / pl.col(TEXT_NBYTES) * 1024).alias(
                SERR_WRITE_TEXT_NS_PER_KIB
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
            (pl.col(MEAN_WRITE_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1024).alias(
                MEAN_WRITE_DATA_DIFF_NS_PER_KIB
            ),
            (pl.col(SERR_WRITE_DATA_DIFF_NS) / pl.col(DATA_NBYTES) * 1024).alias(
                SERR_WRITE_DATA_DIFF_NS_PER_KIB
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


def check_ff_data(input_fcs: Path, input_tsv: Path, scratch_root: Path) -> None:
    """Ensure DATA didn't get screwed up during optimization.

    DATA will be compared against a TSV file which was generated
    in parallel to the FCS file directly from the polars dataframe.

    The read test will be successful if reading the FCS file produces the
    same dataframe as that in the TSV file. Note that the schema for the FCS
    file needs to be used when reading the TSV file.

    The write test will succeed if an FCS file that is written and read
    again produces the same dataframe.

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
    core, _ = pf.api.fcs_read_std_dataset(input_fcs, time_meas_pattern=None)
    tsv = pl.read_csv(input_tsv, separator="\t", schema=core.data.schema)
    assert core.data.equals(tsv)

    # test that writing FCS file produces same data as the input FCS file
    new_fcs = scratch_root / f"ff_write_check_{input_fcs.name}"
    core.write_dataset(new_fcs)
    nu_core, _ = pf.api.fcs_read_std_dataset(new_fcs, time_meas_pattern=None)

    assert core.data.equals(nu_core.data)


def read_bench_files(input_root: Path, names_filter: list[str]) -> pl.DataFrame:
    bench_files = pl.read_csv(input_root / BENCH_FILES_NAME, separator="\t")
    if len(names_filter) > 0:
        bench_files = bench_files.filter(pl.col(BENCH_NAME).is_in(names_filter))
    return bench_files


def fcsparser_files(bench_files: pl.DataFrame) -> list[str]:
    return [
        n
        for n in bench_files.filter(
            ~pl.col("version").eq("FCS3.2")
            & pl.col("byteord").is_in(["1,2,3,4", "4,3,2,1"])
            & ~pl.col("datatypes").eq("U64")
        )[BENCH_NAME]
    ]


def flowio_files(bench_files: pl.DataFrame) -> list[str]:
    return [
        n
        for n in bench_files.filter(
            ~pl.col("version").eq("FCS3.2")
            & pl.col("byteord").is_in(["1,2,3,4", "4,3,2,1"])
            & ~pl.col("datatypes").is_in(["U24", "U64"])
        )[BENCH_NAME]
    ]


def flowcore_files(bench_files: pl.DataFrame) -> list[str]:
    return [
        n
        for n in bench_files.filter(
            ~pl.col("version").eq("FCS3.2")
            & ~pl.col("datatypes").is_in(["U08,U16,U32", "U64"])
        )[BENCH_NAME]
    ]


def flowio_runs(bench_files: pl.DataFrame) -> list[FlowIOBenchRun]:
    return [
        FlowIOBenchRun(n, k)
        for n in flowio_files(bench_files)
        for k in FlowIOBenchKey
        for _ in range(FLOWIO_TRIAL_NUMBER[k])
    ]


def fcsparser_runs(bench_files: pl.DataFrame) -> list[FCSParserBenchRun]:
    return [
        FCSParserBenchRun(n, k)
        for n in fcsparser_files(bench_files)
        for k in FCSParserBenchKey
        for _ in range(FCSPARSER_TRIAL_NUMBER[k])
    ]


def flowcore_runs(
    bench_files: pl.DataFrame, py_to_r: Path, r_to_py: Path
) -> list[FlowCoreBenchRun]:
    return [
        FlowCoreBenchRun(n, k, py_to_r, r_to_py)
        for n in flowcore_files(bench_files)
        for k in FlowCoreBenchKey
        for _ in range(FLOWCORE_TRIAL_NUMBER[k])
    ]


def ff_runs(
    bench_files: pl.DataFrame,
    input_root: Path,
    scratch_root: Path,
    scalpal: bool,
) -> list[FFBenchRun]:
    runs = [
        FFBenchRun(n, k, scalpal)
        for n in bench_files[BENCH_NAME]
        for k in FFBenchKey
        for _ in range(FF_TRIAL_NUMBER[k])
    ]

    return runs


def read_ground_truth_dataframes(
    bench_files: pl.DataFrame,
    input_root: Path,
) -> dict[str, pl.DataFrame]:
    def go(n: str) -> pl.DataFrame:
        p = input_root / f"{n}.tsv"
        with open(p, "r") as f:
            ncol = len(next(f).strip().split("\t"))
        df = pl.read_csv(p, separator="\t", schema_overrides=[pl.Float64] * ncol)
        df.columns = [f"X{i}" for i in range(ncol)]
        return df

    files = [n for n in bench_files[BENCH_NAME]]
    return {k: go(k) for k in files}


def read_ff_dataframes(
    bench_files: pl.DataFrame,
    input_root: Path,
) -> dict[str, pl.DataFrame]:
    def go(n: str) -> pl.DataFrame:
        p = input_root / f"{n}.fcs"
        return pf.api.fcs_read_flat_dataset(p).dataset.data.cast(pl.Float64)

    files = [n for n in bench_files[BENCH_NAME]]
    return {k: go(k) for k in files}


def read_fcsparser_dataframes(
    bench_files: pl.DataFrame,
    input_root: Path,
) -> dict[str, pl.DataFrame]:
    def go(n: str) -> pl.DataFrame:
        p = input_root / f"{n}.fcs"
        df = pl.from_pandas(fp.parse(p)[1]).cast(pl.Float64)
        ncol = len(df.columns)
        df.columns = [f"X{i}" for i in range(ncol)]
        return df

    # only read files that at least one python lib can understand
    files = fcsparser_files(bench_files)
    return {k: go(k) for k in files}


def read_flowio_dataframes(
    bench_files: pl.DataFrame,
    input_root: Path,
) -> dict[str, pl.DataFrame]:
    def go(n: str) -> pl.DataFrame:
        p = input_root / f"{n}.fcs"
        df = pl.DataFrame(fi.FlowData(p).as_array(preprocess=False)).cast(pl.Float64)
        ncol = len(df.columns)
        df.columns = [f"X{i}" for i in range(ncol)]
        return df

    # only read files that at least one python lib can understand
    files = flowio_files(bench_files)
    return {k: go(k) for k in files}


def read_flowcore_dataframes(
    bench_files: pl.DataFrame,
    input_root: Path,
    output_root: Path,
    py_to_r: Path,
    r_to_py: Path,
) -> dict[str, pl.DataFrame]:
    def go(n: str) -> pl.DataFrame:
        inpath = (input_root / f"{n}.fcs").relative_to(here)
        outpath = output_root / f"{n}.tsv"
        cmd = f"dump {inpath} {outpath}"
        _ = call_flowcore_inner(py_to_r, r_to_py, cmd)
        with open(outpath, "r") as f:
            ncol_orig = len(next(f).strip().split("\t"))
        df = pl.read_csv(
            outpath,
            separator="\t",
            schema_overrides=[pl.Float64] * ncol_orig,
            has_header=False,
        ).select(~cs.by_index(0))
        ncol = len(df.columns)
        df.columns = [f"X{i}" for i in range(ncol)]
        return df

    # only read files that at least one python lib can understand
    files = flowcore_files(bench_files)
    return {k: go(k) for k in files}


def with_flowcore_loop[X](f: Callable[[Path, Path, Path], X]) -> X:
    with tf.TemporaryDirectory() as td:
        py_to_r = Path(td) / "py_to_r"
        r_to_py = Path(td) / "r_to_py"
        os.mkfifo(py_to_r)
        os.mkfifo(r_to_py)

        here = Path(sys.argv[0]).parent

        # start R loop in subprocess
        r_cmd = [
            "Rscript",
            "--no-save",
            "--no-restore",
            "R/run_flowcore_loop.R",
            str(py_to_r),
            str(r_to_py),
        ]
        with sp.Popen(r_cmd, cwd=here) as r_proc:
            print("starting R deamon")

            try:
                return f(Path(td), py_to_r, r_to_py)
            finally:
                # This should fire on any exception, including KeyboardInterrupt
                print("stopping R deamon (politely)")
                r_proc.terminate()
                try:
                    r_proc.wait(timeout=5)
                except sp.TimeoutExpired:
                    print("killing R deamon (impolitely)")
                    r_proc.kill()


def run_checks(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    bench_files = read_bench_files(input_root, names_filter)

    # # First, check that ff read and write are inverses of each other

    # for n in bench_files[BENCH_NAME]:
    #     input_fcs = input_root / f"{n}.fcs"
    #     input_tsv = input_root / f"{n}.tsv"
    #     check_ff_data(input_fcs, input_tsv, scratch_root)

    # # Next, check that each library produces the same data when parsing DATA

    def go(tmpdir: Path, py_to_r: Path, r_to_py: Path) -> pl.DataFrame:
        gt_dfs = read_ground_truth_dataframes(bench_files, input_root)
        ff_dfs = read_ff_dataframes(bench_files, input_root)
        fp_dfs = read_fcsparser_dataframes(bench_files, input_root)
        fi_dfs = read_flowio_dataframes(bench_files, input_root)
        fc_dfs = read_flowcore_dataframes(
            bench_files, input_root, tmpdir, py_to_r, r_to_py
        )

        def check(a: pl.DataFrame, b: pl.DataFrame) -> bool:
            try:
                assert_frame_equal(a, b)
                return True
            except AssertionError:
                return False

        ff_checks = [
            check(v, ff_dfs[k]) if k in ff_dfs else None for k, v in gt_dfs.items()
        ]
        fp_checks = [
            check(v, fp_dfs[k]) if k in fp_dfs else None for k, v in gt_dfs.items()
        ]
        fi_checks = [
            check(v, fi_dfs[k]) if k in fi_dfs else None for k, v in gt_dfs.items()
        ]
        fc_checks = [
            check(v, fc_dfs[k]) if k in fc_dfs else None for k, v in gt_dfs.items()
        ]
        return pl.DataFrame(
            [list(ff_dfs.keys()), ff_checks, fp_checks, fi_checks, fc_checks],
            schema={
                BENCH_NAME: str,
                FIREFLOW: bool,
                FCSPARSER: bool,
                FLOWIO: bool,
                FLOWCORE: bool,
            },
        )

    return with_flowcore_loop(go)


def run_all_bench(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    read_columns = [
        BENCH_NAME,
        BYTEORD,
        VERSION,
        DATATYPES,
        WIDTH,
        HEIGHT,
        N_KEYWORDS,
        TEXT_NBYTES,
        DATA_NBYTES,
        MEAN_READ_TEXT_NS,
        MEAN_READ_TEXT_NS_PER_KW,
        MEAN_READ_TEXT_NS_PER_KIB,
        SERR_READ_TEXT_NS,
        SERR_READ_TEXT_NS_PER_KW,
        SERR_READ_TEXT_NS_PER_KIB,
        MEAN_READ_DATA_NS,
        MEAN_READ_DATA_DIFF_NS,
        MEAN_READ_DATA_DIFF_NS_PER_KIB,
        MEAN_READ_DATA_DIFF_NS_PER_VAL,
        SERR_READ_DATA_NS,
        SERR_READ_DATA_DIFF_NS,
        SERR_READ_DATA_DIFF_NS_PER_KIB,
        SERR_READ_DATA_DIFF_NS_PER_VAL,
        READ_TEXT_RUNS,
        READ_DATA_RUNS,
    ]
    write_columns = [
        MEAN_WRITE_TEXT_NS,
        MEAN_WRITE_TEXT_NS_PER_KW,
        MEAN_WRITE_TEXT_NS_PER_KIB,
        SERR_WRITE_TEXT_NS,
        SERR_WRITE_TEXT_NS_PER_KW,
        SERR_WRITE_TEXT_NS_PER_KIB,
        MEAN_WRITE_DATA_NS,
        MEAN_WRITE_DATA_DIFF_NS,
        MEAN_WRITE_DATA_DIFF_NS_PER_KIB,
        MEAN_WRITE_DATA_DIFF_NS_PER_VAL,
        SERR_WRITE_DATA_NS,
        SERR_WRITE_DATA_DIFF_NS,
        SERR_WRITE_DATA_DIFF_NS_PER_KIB,
        SERR_WRITE_DATA_DIFF_NS_PER_VAL,
        WRITE_TEXT_RUNS,
        WRITE_DATA_RUNS,
    ]
    all_columns = read_columns + write_columns

    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = read_bench_files(input_root, names_filter)

    def go(_tmpdir: Path, py_to_r: Path, r_to_py: Path) -> list[AnyBenchResult]:
        runs: list[AnyBenchRun] = [
            *fcsparser_runs(bench_files),
            *flowio_runs(bench_files),
            *flowcore_runs(bench_files, py_to_r, r_to_py),
            *ff_runs(bench_files, input_root, scratch_root, True),
            *ff_runs(bench_files, input_root, scratch_root, False),
        ]

        # Warm up all code paths once; also load all files into page cache
        _ = [r.run(input_root, scratch_root) for r in set(runs)]

        # randomly shuffle runs to eliminate temporal bias
        shuffle(runs)
        return [r.run(input_root, scratch_root) for r in runs]

    results = with_flowcore_loop(go)

    def to_df(key: AnyBenchKey, name: str, runs_name: str) -> pl.DataFrame:
        runs = get_runs(key)
        rs = [r for r in results if r.key == key]
        full_name = f"{name}_ns"
        result_df = pl.DataFrame(
            [[r.name for r in rs], [r.value for r in rs]],
            {BENCH_NAME: pl.String, full_name: pl.Float32},
        )
        df = result_df.group_by(BENCH_NAME).agg(
            pl.col(full_name).mean().name.prefix("mean_"),
            (pl.col(full_name).std() / pl.col(full_name).count().sqrt()).name.prefix(
                "serr_"
            ),
        )
        return df.with_columns(pl.lit(runs).alias(runs_name))

    def compute_read_write_df(
        df_read_text: pl.DataFrame,
        df_read_data: pl.DataFrame,
        df_write_text: pl.DataFrame,
        df_write_data: pl.DataFrame,
    ) -> pl.DataFrame:
        df_read = compute_read_df(
            bench_files,
            df_read_text,
            df_read_data,
        )

        return compute_write_df(
            df_read,
            df_write_text,
            df_write_data,
        )

    df_fcsparser = compute_read_df(
        bench_files,
        to_df(FCSParserBenchKey.READ_TEXT, "r_text", READ_TEXT_RUNS),
        to_df(FCSParserBenchKey.READ_DATA, "r_data", READ_DATA_RUNS),
    )

    df_flowio = compute_read_write_df(
        to_df(FlowIOBenchKey.READ_TEXT, "r_text", READ_TEXT_RUNS),
        to_df(FlowIOBenchKey.READ_DATA, "r_data", READ_DATA_RUNS),
        to_df(FlowIOBenchKey.WRITE_TEXT, "w_text", WRITE_TEXT_RUNS),
        to_df(FlowIOBenchKey.WRITE_DATA, "w_data", WRITE_DATA_RUNS),
    )

    df_flowcore = compute_read_write_df(
        to_df(FlowCoreBenchKey.READ_TEXT, "r_text", READ_TEXT_RUNS),
        to_df(FlowCoreBenchKey.READ_DATA, "r_data", READ_DATA_RUNS),
        to_df(FlowCoreBenchKey.WRITE_TEXT, "w_text", WRITE_TEXT_RUNS),
        to_df(FlowCoreBenchKey.WRITE_DATA, "w_data", WRITE_DATA_RUNS),
    )

    df_ff_clean = compute_read_write_df(
        to_df((FFBenchKey.READ_FLAT, False), "r_text", READ_TEXT_RUNS),
        to_df((FFBenchKey.READ_DATA, False), "r_data", READ_DATA_RUNS),
        to_df((FFBenchKey.WRITE_TEXT, False), "w_text", WRITE_TEXT_RUNS),
        to_df((FFBenchKey.WRITE_DATA, False), "w_data", WRITE_DATA_RUNS),
    )

    df_ff_fix = compute_read_write_df(
        to_df((FFBenchKey.READ_FLAT, True), "r_text", READ_TEXT_RUNS),
        to_df((FFBenchKey.READ_DATA, True), "r_data", READ_DATA_RUNS),
        to_df((FFBenchKey.WRITE_TEXT, True), "w_text", WRITE_TEXT_RUNS),
        to_df((FFBenchKey.WRITE_DATA, True), "w_data", WRITE_DATA_RUNS),
    )

    df_all = pl.concat(
        [
            df_ff_clean.select(all_columns).with_columns(library=pl.lit(FIREFLOW)),
            df_ff_fix.select(all_columns).with_columns(library=pl.lit(FIREFLOW_FIX)),
            df_flowio.select(all_columns).with_columns(library=pl.lit(FLOWIO)),
            df_fcsparser.select(read_columns)
            .with_columns(pl.lit(None).alias(n) for n in write_columns)
            .with_columns(library=pl.lit(FCSPARSER)),
            df_flowcore.select(all_columns).with_columns(library=pl.lit(FLOWCORE)),
        ],
        how="vertical",
    )

    return df_all


def run_ff_bench(
    input_root: Path,
    scratch_root: Path,
    names_filter: list[str],
) -> pl.DataFrame:
    scratch_root.mkdir(parents=True, exist_ok=True)

    bench_files = read_bench_files(input_root, names_filter)
    runs = [
        FFInternalBenchRun(n, s)
        for n in bench_files[BENCH_NAME]
        for _ in range(20)
        for s in [True, False]
    ]

    # warm up code paths and load files into page cache
    _ = [r.read_std_data(input_root) for r in set(runs)]

    # randomly shuffle runs to eliminate temporal bias
    shuffle(runs)
    results = [r.read_std_data(input_root) for r in runs]

    df = (
        pl.DataFrame(results)
        .with_columns(
            (
                pl.col(READ_HEADER_NS)
                + pl.col(READ_TEXT_NS)
                + pl.col(READ_STD_NS)
                + pl.col(READ_SCHEMA_NS)
                + pl.col(READ_DATA_NS)
                + pl.col(CHECK_RANGE_NS)
                + pl.col(READ_CRC_NS)
            ).alias(RS_TOTAL_NS),
            (pl.col(READ_STD_NS) + pl.col(READ_SCHEMA_NS)).alias(READ_ALL_STD_NS),
        )
        .with_columns((pl.col(TOTAL_NS) - pl.col(RS_TOTAL_NS)).alias(PY_OVERHEAD_NS))
        .with_columns(
            (
                pl.col(READ_HEADER_NS)
                + pl.col(READ_TEXT_NS)
                + pl.col(READ_STD_NS)
                + pl.col(PY_OVERHEAD_NS)
            ).alias(READ_TEXT_TOTAL_NS),
        )
        .with_columns(
            (pl.col(READ_TEXT_TOTAL_NS) + pl.col(READ_ALL_STD_NS)).alias(
                READ_STD_TOTAL_NS
            ),
        )
        .with_columns(
            pl.when(pl.col("scalpal"))
            .then(pl.lit(FIREFLOW_FIX))
            .otherwise(pl.lit(FIREFLOW))
            .alias(LIBRARY)
        )
        .drop("scalpal")
        .join(bench_files, on=BENCH_NAME)
    )

    return df


def print_ff_df(df: pl.DataFrame, output_path: Path | None) -> None:
    if output_path is None:
        df.write_csv(sys.stdout, separator="\t")
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            df.write_csv(f, separator="\t")


def fill_cartesian[X](df: pl.DataFrame, col: str, fill: float | None) -> pl.DataFrame:
    wide = df.select([BENCH_NAME, col, LIBRARY]).pivot(LIBRARY, index=BENCH_NAME)
    return (wide.fill_null(fill) if fill is not None else wide).unpivot(
        None,
        index=BENCH_NAME,
        variable_name=LIBRARY,
        value_name=col,
    )


def parser_enum() -> pl.Enum:
    return pl.Enum(list(reversed(LIBRARIES)))


def agg_mean_ci(df: pl.DataFrame, col: str) -> pl.DataFrame:
    return (
        df.group_by([BENCH_NAME, LIBRARY])
        .agg(
            pl.col(col).mean().alias("mean"),
            (pl.col(col).std() / pl.col(col).count().sqrt() * 1.96).alias("serr"),
        )
        .with_columns(
            (pl.col("mean") - pl.col("serr")).alias("lower"),
            (pl.col("mean") + pl.col("serr")).alias("upper"),
        )
    )


def plot_read_text(df: pl.DataFrame, out_path: Path) -> None:
    df_mean = fill_cartesian(df, MEAN_READ_TEXT_NS_PER_KW, 0)
    df_serr = fill_cartesian(df, SERR_READ_TEXT_NS_PER_KW, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (pl.col(MEAN_READ_TEXT_NS_PER_KW) - pl.col(SERR_READ_TEXT_NS_PER_KW)).alias(
                "lower"
            ),
            (pl.col(MEAN_READ_TEXT_NS_PER_KW) + pl.col(SERR_READ_TEXT_NS_PER_KW)).alias(
                "upper"
            ),
        )
        .with_columns(pl.col(LIBRARY).cast(parser_enum()))
    )

    read_text_plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_READ_TEXT_NS_PER_KW, x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="TEXT read time (ns/keyword pair)", x="FCS File", fill="Library")
        + coord_flip()
        + scale_fill_discrete(limits=LIBRARIES)
    )
    read_text_plt.save(out_path)


def plot_read_data(
    df: pl.DataFrame, out_path: Path, out_path_no_flowcore: Path
) -> None:
    df_mean = fill_cartesian(df, MEAN_READ_DATA_DIFF_NS_PER_VAL, 0)
    df_serr = fill_cartesian(df, SERR_READ_DATA_DIFF_NS_PER_VAL, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (
                pl.col(MEAN_READ_DATA_DIFF_NS_PER_VAL)
                - pl.col(SERR_READ_DATA_DIFF_NS_PER_VAL)
            ).alias("lower"),
            (
                pl.col(MEAN_READ_DATA_DIFF_NS_PER_VAL)
                + pl.col(SERR_READ_DATA_DIFF_NS_PER_VAL)
            ).alias("upper"),
        )
        .with_columns(pl.col(LIBRARY).cast(parser_enum()))
    )

    read_text_plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_READ_DATA_DIFF_NS_PER_VAL, x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="DATA read time (ns/value)", x="FCS File", fill="Library")
        + coord_flip(ylim=(None, 40))
        + scale_fill_discrete(limits=LIBRARIES)
    )
    read_text_plt.save(out_path)

    read_text_plt = (
        ggplot(
            df_combined.filter(~pl.col(LIBRARY).eq(FLOWCORE)),
            aes(y=MEAN_READ_DATA_DIFF_NS_PER_VAL, x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="DATA read time (ns/value)", x="FCS File", fill="Library")
        + coord_flip(ylim=(None, 10))
        + scale_fill_discrete(limits=[t for t in LIBRARIES if t != FLOWCORE])
    )
    read_text_plt.save(out_path_no_flowcore)


def plot_write_text(df: pl.DataFrame, out_path: Path) -> None:
    df_mean = fill_cartesian(df, MEAN_WRITE_TEXT_NS_PER_KW, 0)
    df_serr = fill_cartesian(df, SERR_WRITE_TEXT_NS_PER_KW, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (
                pl.col(MEAN_WRITE_TEXT_NS_PER_KW) - pl.col(SERR_WRITE_TEXT_NS_PER_KW)
            ).alias("lower"),
            (
                pl.col(MEAN_WRITE_TEXT_NS_PER_KW) + pl.col(SERR_WRITE_TEXT_NS_PER_KW)
            ).alias("upper"),
        )
        .with_columns(pl.col(LIBRARY).cast(parser_enum()))
    ).filter(~pl.col(LIBRARY).is_in([FCSPARSER, FIREFLOW_FIX]))

    read_text_plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_WRITE_TEXT_NS_PER_KW, x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="TEXT write time (ns/keyword pair)", x="FCS File", fill="Library")
        + coord_flip()
        + scale_fill_discrete(
            limits=[t for t in LIBRARIES if t not in [FCSPARSER, FIREFLOW_FIX]]
        )
    )
    read_text_plt.save(out_path)


def plot_write_data(df: pl.DataFrame, out_path: Path) -> None:
    df_mean = fill_cartesian(df, MEAN_WRITE_DATA_DIFF_NS_PER_VAL, 0)
    df_serr = fill_cartesian(df, SERR_WRITE_DATA_DIFF_NS_PER_VAL, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (
                pl.col(MEAN_WRITE_DATA_DIFF_NS_PER_VAL)
                - pl.col(SERR_WRITE_DATA_DIFF_NS_PER_VAL)
            ).alias("lower"),
            (
                pl.col(MEAN_WRITE_DATA_DIFF_NS_PER_VAL)
                + pl.col(SERR_WRITE_DATA_DIFF_NS_PER_VAL)
            ).alias("upper"),
        )
        .with_columns(pl.col(LIBRARY).cast(parser_enum()))
    ).filter(~pl.col(LIBRARY).is_in([FCSPARSER, FIREFLOW_FIX]))

    read_text_plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_WRITE_DATA_DIFF_NS_PER_VAL, x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="DATA write time (ns/value)", x="FCS File", fill="Library")
        + coord_flip()
        + scale_fill_discrete(
            limits=[t for t in LIBRARIES if t not in [FCSPARSER, FIREFLOW_FIX]]
        )
    )
    read_text_plt.save(out_path)


def plot_fireflow_composition(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = (
        df.select(
            [
                BENCH_NAME,
                LIBRARY,
                READ_TEXT_TOTAL_NS,
                READ_STD_TOTAL_NS,
                READ_DATA_NS,
                CHECK_RANGE_NS,
                READ_CRC_NS,
            ]
        )
        .rename(
            {
                READ_TEXT_TOTAL_NS: "TEXT",
                READ_STD_TOTAL_NS: "Std",
                READ_DATA_NS: "DATA",
                CHECK_RANGE_NS: "Rng",
                READ_CRC_NS: "CRC",
            }
        )
        .unpivot(index=[BENCH_NAME, LIBRARY])
        .group_by([BENCH_NAME, LIBRARY, "variable"])
        .agg((pl.col("value").mean() / 1000000).alias("mean"))
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill="variable"),  # type: ignore
        )
        + facet_wrap(facets=[LIBRARY], ncol=2)
        + geom_col()
        + labs(y="Read time (ms)", x="FCS File")
        + coord_flip()
        + scale_fill_discrete(limits=["TEXT", "Std", "DATA", "Rng", "CRC"])
        + theme(legend_position="bottom")  # type: ignore
    )
    plt.save(out_path)  # type: ignore


def plot_fireflow_std_overhead(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns((pl.col(READ_ALL_STD_NS) / pl.col(N_KEYWORDS)).alias("rate")),
        "rate",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="TEXT Std. Overhead (ns/keyword pair)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_fireflow_std_overhead_ratio(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns(
            (
                pl.col(READ_ALL_STD_NS)
                / (pl.col(READ_ALL_STD_NS) + pl.col(READ_TEXT_TOTAL_NS))
                * 100
            ).alias("frac")
        ),
        "frac",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="TEXT Std. Overhead (% of total TEXT parse time)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_fireflow_crc_overhead(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns(
            (
                pl.col(READ_CRC_NS) / (pl.col(TEXT_NBYTES) + pl.col(DATA_NBYTES)) * 1024
            ).alias("rate")
        ),
        "rate",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="CRC Overhead (ns/KiB)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_fireflow_crc_overhead_ratio(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns((pl.col(READ_CRC_NS) / pl.col(TOTAL_NS) * 100).alias("frac")),
        "frac",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="CRC Overhead (% of total read time)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_fireflow_rng_overhead(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns(
            (pl.col(CHECK_RANGE_NS) / pl.col(WIDTH) / pl.col(HEIGHT)).alias("rate")
        ),
        "rate",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="Range Overhead (ns/value)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_fireflow_rng_overhead_ratio(df: pl.DataFrame, out_path: Path) -> None:
    df_agg = agg_mean_ci(
        df.with_columns(
            (pl.col(CHECK_RANGE_NS) / pl.col(TOTAL_NS) * 100).alias("frac")
        ),
        "frac",
    )

    plt = (
        ggplot(
            df_agg,
            aes(y="mean", x=BENCH_NAME, fill=LIBRARY),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + labs(y="Range Check Overhead (% of total read time)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_read_data_kib(
    df: pl.DataFrame,
    out_path: Path,
    ssd_speed: float,
    nvme_speed: float,
    ram_speed: float,
) -> None:
    df_mean = fill_cartesian(df, MEAN_READ_DATA_DIFF_NS_PER_KIB, 0)
    df_serr = fill_cartesian(df, SERR_READ_DATA_DIFF_NS_PER_KIB, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (
                pl.col(MEAN_READ_DATA_DIFF_NS_PER_KIB)
                - pl.col(SERR_READ_DATA_DIFF_NS_PER_KIB)
            ).alias("lower"),
            (
                pl.col(MEAN_READ_DATA_DIFF_NS_PER_KIB)
                + pl.col(SERR_READ_DATA_DIFF_NS_PER_KIB)
            ).alias("upper"),
        )
        .filter(pl.col(LIBRARY).eq(FIREFLOW))
    )

    plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_READ_DATA_DIFF_NS_PER_KIB, x=BENCH_NAME),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + geom_hline(
            aes(yintercept=ssd_speed),  # type: ignore
            color="blue",
        )
        + geom_hline(
            aes(yintercept=nvme_speed),  # type: ignore
            color="#00ff00",
        )
        + geom_hline(
            aes(yintercept=ram_speed),  # type: ignore
            color="red",
        )
        + labs(y="DATA read time (ns/KiB)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def plot_write_data_kib(
    df: pl.DataFrame,
    out_path: Path,
    ssd_speed: float,
    nvme_speed: float,
    ram_speed: float,
) -> None:
    df_mean = fill_cartesian(df, MEAN_WRITE_DATA_DIFF_NS_PER_KIB, 0)
    df_serr = fill_cartesian(df, SERR_WRITE_DATA_DIFF_NS_PER_KIB, None)
    df_combined = (
        df_mean.join(df_serr, on=[BENCH_NAME, LIBRARY])
        .with_columns(
            (
                pl.col(MEAN_WRITE_DATA_DIFF_NS_PER_KIB)
                - pl.col(SERR_WRITE_DATA_DIFF_NS_PER_KIB)
            ).alias("lower"),
            (
                pl.col(MEAN_WRITE_DATA_DIFF_NS_PER_KIB)
                + pl.col(SERR_WRITE_DATA_DIFF_NS_PER_KIB)
            ).alias("upper"),
        )
        .filter(pl.col(LIBRARY).eq(FIREFLOW))
    )

    plt = (
        ggplot(
            df_combined,
            aes(y=MEAN_WRITE_DATA_DIFF_NS_PER_KIB, x=BENCH_NAME),  # type: ignore
        )
        + geom_col(position="dodge")
        + geom_errorbar(
            aes(ymin="lower", ymax="upper"),  # type: ignore
            position="dodge",
            width=0.9,
        )
        + geom_hline(
            aes(yintercept=ssd_speed),  # type: ignore
            color="blue",
        )
        + geom_hline(
            aes(yintercept=nvme_speed),  # type: ignore
            color="#00ff00",
        )
        + geom_hline(
            aes(yintercept=ram_speed),  # type: ignore
            color="red",
        )
        + labs(y="DATA write time (ns/KiB)", x="FCS File")
        + coord_flip()
    )
    plt.save(out_path)


def dataframe_to_md(df: pl.DataFrame) -> str:
    cols = df.columns
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    rows = ["| " + " | ".join(str(v) for v in row) + " |" for row in df.rows()]
    return "\n".join([header, sep, *rows])


def cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def total_memory() -> float:
    with open("/proc/meminfo") as f:
        meminfo = {i.split()[0].rstrip(":"): int(i.split()[1]) for i in f}
        return int(meminfo["MemTotal"] / 1024 / 1024)


def get_flowcore_version(exec_dir: Path) -> str:
    ret = sp.run(
        [
            "R",
            "--no-save",
            "--no-restore",
            "-s",
            "-e",
            'getNamespaceVersion("flowCore")[[1]]',
        ],
        capture_output=True,
        text=True,
        cwd=exec_dir,
        check=False,
    )
    if ret.returncode == 0:
        if m := re.match('^\\[1\\] "(.+)"$', ret.stdout):
            return m[1]
        else:
            assert False, f"could not get version from stdout: {ret.stdout}"
    else:
        assert False, ret.stderr


def get_r_version(exec_dir: Path) -> str:
    ret = sp.run(
        ["R", "--no-save", "--no-restore", "--version"],
        capture_output=True,
        text=True,
        cwd=exec_dir,
        check=False,
    )
    if ret.returncode == 0:
        if (
            m := re.match(
                "^R version ([^ ]+) \\((.+)\\) .+\n.+\nPlatform: ([^ \n]+)", ret.stdout
            )
        ) is not None:
            return f"{m[1]} ({m[2]}, {m[3]})"
        else:
            assert False, f"could not get version from stdout: {ret.stdout}"
    else:
        assert False, ret.stderr


def get_flowcore_bytecompiled(exec_dir: Path) -> bool:
    ret = sp.run(
        ["R", "--no-save", "--no-restore", "-e", "flowCore::read.FCS"],
        capture_output=True,
        text=True,
        cwd=exec_dir,
        check=False,
    )
    if ret.returncode == 0:
        return re.search("<bytecode: [^ ]+>", ret.stdout) is not None
    else:
        assert False, ret.stderr


def get_flowcore_compilers(exec_dir: Path) -> list[str]:
    ret_r = sp.run(
        [
            "R",
            "--no-save",
            "--no-restore",
            "-s",
            "-e",
            'cat(system.file("libs", package = "flowCore"), "\\n")',
        ],
        capture_output=True,
        text=True,
        cwd=exec_dir,
        check=False,
    )
    if ret_r.returncode == 0:
        libpath = Path(ret_r.stdout.strip()) / "flowCore.so"
        ret_elf = sp.run(
            ["readelf", "-p", ".comment", str(libpath)],
            capture_output=True,
            text=True,
            cwd=exec_dir,
            check=False,
        )
        if ret_elf.returncode == 0:
            elf_lines = ret_elf.stdout.strip().split("\n")
            return [
                re.sub("  \\[.+\\]  ", "", s) for s in elf_lines if s.startswith("  [")
            ]
        else:
            assert False, f"could not get compilers for {libpath}"
    else:
        assert False, ret_r.stderr


def render_all(
    bench_exec_dir: Path,
    files_path: Path,
    check_path: Path,
    bench_path: Path,
    bench_ff_path: Path,
    template_path: Path,
    static_dir: Path,
    readme_path: Path,
) -> None:
    static_dir.mkdir(parents=True, exist_ok=True)
    df_files = pl.read_csv(files_path, separator="\t")
    df_check = pl.read_csv(check_path, separator="\t")
    df_results = pl.read_csv(bench_path, separator="\t")
    df_ff_results = pl.read_csv(bench_ff_path, separator="\t")

    df_runs = df_results.select(
        [
            LIBRARY,
            READ_TEXT_RUNS,
            READ_DATA_RUNS,
            WRITE_TEXT_RUNS,
            WRITE_DATA_RUNS,
        ]
    ).unique()

    readme_dir = readme_path.parent

    read_text_path = static_dir / "read_text.svg"
    read_data_path = static_dir / "read_data.svg"
    read_data_noflowcore_path = static_dir / "read_data_no_flowcore.svg"
    write_text_path = static_dir / "write_text.svg"
    write_data_path = static_dir / "write_data.svg"

    plot_read_text(df_results, read_text_path)
    plot_read_data(df_results, read_data_path, read_data_noflowcore_path)
    plot_write_text(df_results, write_text_path)
    plot_write_data(df_results, write_data_path)

    read_std_composition_path = static_dir / "read_std_composition.svg"

    read_std_overhead_path = static_dir / "read_std_overhead.svg"
    read_crc_overhead_path = static_dir / "read_crc_overhead.svg"
    read_rng_overhead_path = static_dir / "read_rng_overhead.svg"

    read_std_overhead_ratio_path = static_dir / "read_std_overhead_ratio.svg"
    read_crc_overhead_ratio_path = static_dir / "read_crc_overhead_ratio.svg"
    read_rng_overhead_ratio_path = static_dir / "read_rng_overhead_ratio.svg"

    plot_fireflow_composition(df_ff_results, read_std_composition_path)
    plot_fireflow_std_overhead(df_ff_results, read_std_overhead_path)
    plot_fireflow_crc_overhead(df_ff_results, read_crc_overhead_path)
    plot_fireflow_rng_overhead(df_ff_results, read_rng_overhead_path)

    plot_fireflow_std_overhead_ratio(df_ff_results, read_std_overhead_ratio_path)
    plot_fireflow_crc_overhead_ratio(df_ff_results, read_crc_overhead_ratio_path)
    plot_fireflow_rng_overhead_ratio(df_ff_results, read_rng_overhead_ratio_path)

    read_data_kib_path = static_dir / "read_data_kib.svg"
    write_data_kib_path = static_dir / "write_data_kib.svg"

    # convert from GiB/s to ns/KiB
    SPEED_CONV_FACTOR = 1 / 1024 / 1024 * 1e9
    RAM_SPEED = 20  # GiB/s
    NVME_READ_SPEED = 3.5  # GiB/s
    NVME_WRITE_SPEED = 2  # GiB/s
    SSD_SPEED = 0.5  # GiB/s

    plot_read_data_kib(
        df_results,
        read_data_kib_path,
        (1 / SSD_SPEED) * SPEED_CONV_FACTOR,
        (1 / NVME_READ_SPEED) * SPEED_CONV_FACTOR,
        (1 / RAM_SPEED) * SPEED_CONV_FACTOR,
    )
    plot_write_data_kib(
        df_results,
        write_data_kib_path,
        (1 / SSD_SPEED) * SPEED_CONV_FACTOR,
        (1 / NVME_WRITE_SPEED) * SPEED_CONV_FACTOR,
        (1 / RAM_SPEED) * SPEED_CONV_FACTOR,
    )

    env = Environment(
        loader=FileSystemLoader(template_path.parent),
        undefined=StrictUndefined,
    )
    template = env.get_template(template_path.name)
    readme_path.parent.mkdir(exist_ok=True, parents=True)

    file_descriptions = [
        "\n".join(
            tw.wrap(f"* *{r[BENCH_NAME]}*: {r['description']}", subsequent_indent="  ")
        )
        for r in df_files.iter_rows(named=True)
    ]

    np_cfg = np.show_config("dicts")  # type: ignore
    assert np_cfg is not None
    np_xs = np_cfg["SIMD Extensions"]
    np_xs_used = np_xs["baseline"] + np_xs["found"]
    np_compilers = [
        f"{v['name']}-{v['version']} ({k})" for k, v in np_cfg["Compilers"].items()
    ]

    with open(readme_path, "w") as f:
        f.write(
            template.render(
                {
                    "run_datetime": datetime.now(UTC).strftime("%b %d %Y %H:%M"),
                    "read_text_plot_path": read_text_path.relative_to(readme_dir),
                    "read_data_plot_path": read_data_path.relative_to(readme_dir),
                    "read_data_noflowcore_plot_path": read_data_noflowcore_path.relative_to(
                        readme_dir
                    ),
                    "write_text_plot_path": write_text_path.relative_to(readme_dir),
                    "read_std_composition_path": read_std_composition_path.relative_to(
                        readme_dir
                    ),
                    "read_std_overhead_path": read_std_overhead_path.relative_to(
                        readme_dir
                    ),
                    "read_crc_overhead_path": read_crc_overhead_path.relative_to(
                        readme_dir
                    ),
                    "read_rng_overhead_path": read_rng_overhead_path.relative_to(
                        readme_dir
                    ),
                    "read_std_overhead_ratio_path": read_std_overhead_ratio_path.relative_to(
                        readme_dir
                    ),
                    "read_crc_overhead_ratio_path": read_crc_overhead_ratio_path.relative_to(
                        readme_dir
                    ),
                    "read_rng_overhead_ratio_path": read_rng_overhead_ratio_path.relative_to(
                        readme_dir
                    ),
                    "test_file_table": dataframe_to_md(df_files.drop(["description"])),
                    "check_table": dataframe_to_md(df_check),
                    "test_file_descriptions": file_descriptions,
                    "flowio_version": fi.__version__,
                    "fcsparser_version": fp.__version__,
                    "flowcore_version": get_flowcore_version(bench_exec_dir),
                    "trial_number_table": dataframe_to_md(df_runs),
                    "python_version": sys.version,
                    "r_version": get_r_version(bench_exec_dir),
                    "ff_build_info": pf.BuildInfo(),
                    "numpy_version": np.__version__,
                    "numpy_compilers": np_compilers,
                    "numpy_extensions": np_xs_used,
                    "flowcore_byte_compiled": get_flowcore_bytecompiled(bench_exec_dir),
                    "flowcore_compilers": get_flowcore_compilers(bench_exec_dir),
                    "cpu_model": cpu_model(),
                    "total_memory": total_memory(),
                    "kernel_uname": plm.uname().release,
                    "write_data_plot_path": write_data_path.relative_to(readme_dir),
                    "ssd_speed": SSD_SPEED,
                    "nvme_read_speed": NVME_READ_SPEED,
                    "nvme_write_speed": NVME_WRITE_SPEED,
                    "ram_speed": RAM_SPEED,
                    "read_data_kib_plot_path": read_data_kib_path.relative_to(
                        readme_dir
                    ),
                    "write_data_kib_plot_path": write_data_kib_path.relative_to(
                        readme_dir
                    ),
                }
            )
        )


def main(args: list[str]) -> None:
    this = Path(args[0])
    cmd = args[1]
    bench_path = Path(args[2])

    # make FCS files to test (and index them)
    if cmd == "make":
        make_bench_files(bench_path)

    # run checks to ensure benchmarks are equivalent between libraries
    elif cmd == "check":
        output_path = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        names_filter = args[5:]
        df_all = run_checks(bench_path, scratch_root, names_filter)
        if output_path is None:
            df_all.write_csv(sys.stdout, separator="\t")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                df_all.write_csv(f, separator="\t")

    # run all benchmarks against FCS files
    elif cmd == "run_all":
        output_path = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        names_filter = args[5:]
        df_all = run_all_bench(bench_path, scratch_root, names_filter)
        if output_path is None:
            df_all.write_csv(sys.stdout, separator="\t")
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                df_all.write_csv(f, separator="\t")

    # run just the fireflow benchmarks on the FCS files
    elif cmd == "run_ff":
        output_path = None if args[3] == "-" else Path(args[3])
        scratch_root = Path(args[4])
        df = run_ff_bench(bench_path, scratch_root, args[5:])
        print_ff_df(df, output_path)

    # render plots and benchmark summary
    elif cmd == "render":
        files_path = Path(args[2])
        check_path = Path(args[3])
        bench_path = Path(args[4])
        bench_ff_path = Path(args[5])
        template_path = Path(args[6])
        static_dir = Path(args[7])
        readme_path = Path(args[8])
        render_all(
            this.parent,
            files_path,
            check_path,
            bench_path,
            bench_ff_path,
            template_path,
            static_dir,
            readme_path,
        )

    # woopsie
    else:
        print(f"invalid command: {cmd}")
        sys.exit(1)


main(sys.argv)
