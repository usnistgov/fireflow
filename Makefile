VENV=.venv

uv_at = uv --directory pyreflow
build_dev = $(uv_at) run maturin develop --uv
build_rel = $(build_dev) --release
check_py = $(uv_at) run python -c "import sys; print(sys.executable, sys.version)"

.PHONY: .uv
.uv:
	@uv -V || echo 'uv must be installed'

pyreflow/.venv: .uv
	$(uv_at) sync --locked --group all --all-extras

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

.PHONY: py-lint
py-lint: pyreflow/.venv
	$(uv_at) run ruff format --check
	$(uv_at) run python -m mypy.stubtest pyreflow._pyreflow
	$(uv_at) run mypy --no-incremental --cache-dir=/dev/null python
	$(uv_at) run mypy --no-incremental --cache-dir=/dev/null tests

.PHONY: py-test
py-test: pyreflow/.venv
	$(uv_at) run pytest

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

.PHONY: docs
docs: build-dev
	$(uv_at) run sphinx-build -M html docs/source/ docs/build/ --fresh-env -W --nitpicky

.PHONY: clean
clean:  
	rm -rf `find pyreflow -name __pycache__`
	rm -rf pyreflow/.venv
	rm -rf pyreflow/.mypy_cache
	rm -rf pyreflow/.pytest_cache
	rm -rf pyreflow/.ruff_cache
	cargo clean

# NOTE: all benchmark paths are relative to the uv runtime directory (which is
# pyreflow)

bench_script = bench/bench.py
bench_inputs = bench/inputs
bench_files = $(bench_inputs)/bench_files.tsv
bench_all_ff = bench/outputs/bench_all_ff.tsv
bench_checks = bench/outputs/checks.tsv
bench_all = bench/outputs/bench_all.tsv
bench_readme = bench/README.md
bench_scratch = /tmp/fireflow_bench/scratch
bench_static = bench/static
bench_readme_template = bench/templates/README.j2

pyreflow/$(bench_files): pyreflow/.venv \
	pyreflow/$(bench_script)
	$(uv_at) run $(bench_script) make $(bench_inputs)

pyreflow/$(bench_checks): pyreflow/$(bench_files)
	$(uv_at) run $(bench_script) check \
		$(bench_inputs) \
		$(bench_checks)

pyreflow/$(bench_all_ff): pyreflow/$(bench_files) \
	pyreflow/$(bench_script)
	$(uv_at) run $(bench_script) run_ff \
		$(bench_inputs) \
		$(bench_all_ff) \
		$(bench_scratch)

pyreflow/$(bench_all): pyreflow/$(bench_files) \
	pyreflow/$(bench_script)
	$(uv_at) run $(bench_script) run_all \
		$(bench_inputs) \
		$(bench_all) \
		$(bench_scratch)

pyreflow/$(bench_readme): pyreflow/$(bench_files) \
	pyreflow/$(bench_checks) \
	pyreflow/$(bench_all_ff) \
	pyreflow/$(bench_all) \
	pyreflow/$(bench_script) \
	pyreflow/$(bench_readme_template)
	$(uv_at) run $(bench_script) render \
		$(bench_files) \
		$(bench_checks) \
		$(bench_all) \
		$(bench_all_ff) \
		$(bench_readme_template) \
		$(bench_static) \
		$(bench_readme)

.PHONY: bench
bench: pyreflow/bench/README.md

.PHONY: clean-bench
clean-bench:  
	rm -rf pyreflow/bench/inputs
	rm -rf pyreflow/bench/outputs
	rm -rf pyreflow/bench/scratch
