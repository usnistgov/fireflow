#
# Testing/Docs Pipeline
#
# Technically the docs and tests can be run separately but they reuse many of
# the same components. Namely, they all use the same python venv to run the
# python tests and also share the same rust target.

VENV=.venv

uv_at = uv --directory pyreflow
# don't sync every time we call run, allows for better build control
uv_run = $(uv_at) run --no-sync
build_dev = $(uv_run) maturin develop --uv
build_rel = $(build_dev) --release
check_py = $(uv_run) python -c "import sys; print(sys.executable, sys.version)"

.PHONY: .uv
.uv:
	@uv -V || echo 'uv must be installed'

pyreflow/.venv: .uv
	$(uv_at) sync --locked --group all --all-extras --no-install-project --inexact

.PHONY: rs-lint
rs-lint:
	cargo clippy --all-targets --locked -- -D warnings -D clippy::dbg_macro

.PHONY: rs-fmt
rs-fmt:
	cargo fmt --all -- --check

.PHONY: rs-test
rs-test:
	cargo test -p fireflow-core

.PHONY: rs-docs
rs-docs:
	RUSTDOCFLAGS="-D warnings" cargo doc -p fireflow-core --no-deps

# TODO make these depend on debug build
.PHONY: py-lint
py-lint: pyreflow/.venv
	$(uv_run) ruff format --check
	$(uv_run) python -m mypy.stubtest pyreflow._pyreflow
	$(uv_run) mypy --no-incremental --cache-dir=/dev/null python
	$(uv_run) mypy --no-incremental --cache-dir=/dev/null tests

.PHONY: py-test
py-test: pyreflow/.venv
	$(uv_run) pytest

.PHONY: py-check-env
py-check-env: pyreflow/.venv
	$(check_py)
	PYO3_PRINT_CONFIG=1 $(build_dev) || true
	PYO3_PRINT_CONFIG=1 $(build_rel) || true

.PHONY: build-dev
build-dev: pyreflow/.venv
	$(build_dev)

.PHONY: build-prod
build-prod: pyreflow/.venv
	$(build_rel)

.PHONY: all-dev
all-dev: rs-fmt rs-docs rs-test rs-lint build-dev py-lint py-test

docs_out_current=docs/build/

.PHONY: docs
docs: build-dev
	$(uv_run) docs/build.sh docs/source/ $(docs_out_current)

.PHONY: clean
clean:  
	rm -rf `find pyreflow -name __pycache__`
	rm -rf pyreflow/.venv
	rm -rf pyreflow/.mypy_cache
	rm -rf pyreflow/.pytest_cache
	rm -rf pyreflow/.ruff_cache
	cargo clean

docs_out_all=pyreflow/docs/build_all/
docs_tmp=pyreflow/docs/build_tmp/

version_file    = pyreflow/docs/refs.txt
versions       := $(file < $(version_file))
version_outputs = $(patsubst %,$(docs_out_all)/%,$(versions)) 
rs_target := $(shell realpath target)

$(version_outputs): $(docs_out_all)/%:
	pyreflow/docs/build_version.sh $(docs_out_all) $(docs_tmp) $(rs_target) $*

.PHONY: all-docs
all-docs: $(version_outputs)

.PHONY: clean-all-docs
clean-all-docs:
	rm -rf pyreflow/$(docs_out_all)
	rm -rf pyreflow/$(docs_tmp)

#
# Benchmarking pipeline
#
# This is (almost) separate from the above pipeline because it relies on a conda
# env rather than a python venv. This is necessary because flowCore is R-based
# (venv is automatically out) and the python libraries we wish to test in
# parallel (flowio et al) to fireflow have dependency trees that should not
# pollute the testing pipeline (namely fcsparser requires numpy 1.x).
#
# NOTE: this is not totally separate from the above pipeline because the conda
# env uses PYTHONPATH to point to the rust build for fireflow. This is not
# explicitly run (for now) so it must be built manually before calling any of
# these targets

bench_root = pyreflow/bench
bench_script = $(bench_root)/bench.py
bench_inputs = $(bench_root)/inputs
bench_outputs = $(bench_root)/outputs
bench_files = $(bench_inputs)/bench_files.tsv
bench_all_ff = $(bench_outputs)/bench_all_ff.tsv
bench_checks = $(bench_outputs)/checks.tsv
bench_all = $(bench_outputs)/bench_all.tsv
bench_readme = $(bench_root)/README.md
bench_static = $(bench_root)/static
bench_readme_template = $(bench_root)/templates/README.j2
bench_env = $(bench_root)/conda_env

bench_scratch = /tmp/fireflow_bench/scratch
bench_env_spec = env.yml

pyreflow_abs_path=$$(realpath pyreflow/python)

# This assumes $CONDA_EXE is in the environment. This is necessary because conda
# has a zillion ways it could be installed depending on where this runs and using
# a dedicated variable allows us not to clobber $PATH. The alternative is to run
# the entire make pipeline in a conda env.
conda_setup = eval "$$($$CONDA_EXE shell.bash hook)"
conda_create = conda env create -f $(bench_env_spec)
conda_activate = conda activate $(bench_env)
conda_link = conda env config vars set PYTHONPATH=$(pyreflow_abs_path)
conda_run = $(conda_setup) && $(conda_activate)

.PHONY: create-bench-env
create-bench-env:
	$(conda_setup) && \
	$(conda_create) && \
	$(conda_link) -n pyreflow-bench

$(bench_env):
	$(conda_setup) && \
	$(conda_create) -p $(bench_env) && \
	$(conda_link) -p $(bench_env)

$(bench_files): $(bench_script) \
	$(bench_env)
	$(conda_run) && $(bench_script) make $(bench_inputs)

$(bench_checks): $(bench_files) \
	$(bench_env)
	$(conda_run) && $(bench_script) check \
		$(bench_inputs) \
		$(bench_checks) \
		$(bench_scratch)

$(bench_all_ff): $(bench_files) \
	$(bench_script) \
	$(bench_env)
	$(conda_run) && $(bench_script) run_ff \
		$(bench_inputs) \
		$(bench_all_ff) \
		$(bench_scratch)

$(bench_all): $(bench_files) \
	$(bench_script) \
	$(bench_env)
	$(conda_run) && $(bench_script) run_all \
		$(bench_inputs) \
		$(bench_all) \
		$(bench_scratch)

$(bench_readme): $(bench_files) \
	$(bench_checks) \
	$(bench_all_ff) \
	$(bench_all) \
	$(bench_script) \
	$(bench_readme_template) \
	$(bench_env)
	$(conda_run) && $(bench_script) render \
		$(bench_files) \
		$(bench_checks) \
		$(bench_all) \
		$(bench_all_ff) \
		$(bench_readme_template) \
		$(bench_static) \
		$(bench_readme)

.PHONY: bench
bench: $(bench_readme)

.PHONY: clean-bench
clean-bench:  
	rm -rf $(bench_inputs)
	rm -rf $(bench_outputs)
	rm -rf $(bench_scratch)
	rm -rf $(bench_readme)
	rm -rf $(bench_static)
	rm -rf $(bench_env)
