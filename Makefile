VENV=.venv

uv_at = uv --directory pyreflow

.PHONY: .uv
.uv:
	@uv -V || echo 'uv must be installed'

pyreflow/.venv: .uv
	$(uv_at) sync --frozen --group all

# TODO add cargo fmt --all -- --check
.PHONY: rs-lint
rs-lint:
	cargo clippy --all-targets --locked -- -D warnings -D clippy::dbg_macro

.PHONY: rs-test
rs-test:
	cargo test -p fireflow-core

.PHONY: py-lint
py-lint: pyreflow/.venv
	$(uv_at) run ruff format --check
	$(uv_at) run python -m mypy.stubtest pyreflow._pyreflow --whitelist .mypy-stubtest-allowlist

.PHONY: py-test
py-test: pyreflow/.venv
	$(uv_at) run pytest

.PHONY: build-dev
build-dev: pyreflow/.venv
	$(uv_at) run maturin develop --uv

.PHONY: build-prod
build-prod: pyreflow/.venv
	$(uv_at) run maturin develop --uv --release

.PHONY: all-dev
all-dev: rs-lint rs-test py-lint py-test build-dev

.PHONY: clean
clean:  
	rm -rf `find pyreflow -name __pycache__`
	rm -rf pyreflow/.venv
	rm -rf pyreflow/.mypy_cache
	rm -rf pyreflow/.pytest_cache
	rm -rf pyreflow/.ruff_cache
	cargo clean
