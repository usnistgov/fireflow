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

.PHONY: build-dev
build-dev: pyreflow/.venv
	$(check_py)
	PYO3_PRINT_CONFIG=1 $(build_dev) || true
	$(build_dev)

.PHONY: build-prod
build-prod: pyreflow/.venv
	$(check_py)
	PYO3_PRINT_CONFIG=1 $(build_rel) || true
	$(build_rel)

.PHONY: all-dev
all-dev: rs-fmt rs-docs rs-test rs-lint build-dev py-lint py-test

.PHONY: docs
docs: build-dev
	$(uv_at) run sphinx-build -M html docs/source/ docs/build/ --fresh-env -W

pyreflow/bench/inputs/bench_files.tsv: pyreflow/.venv
	$(uv_at) run ./bench/bench.py make ./bench/inputs

pyreflow/bench/outputs/data.tsv: pyreflow/bench/inputs/bench_files.tsv build-prod
	$(uv_at) run ./bench/bench.py run ./bench/inputs ./bench/outputs ./bench/scratch

.PHONY: clean
clean:  
	rm -rf `find pyreflow -name __pycache__`
	rm -rf pyreflow/.venv
	rm -rf pyreflow/.mypy_cache
	rm -rf pyreflow/.pytest_cache
	rm -rf pyreflow/.ruff_cache
	rm -rf pyreflow/bench/inputs
	rm -rf pyreflow/bench/outputs
	rm -rf pyreflow/bench/scratch
	cargo clean
