VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

# set up python env
.venv:
	python3 -m venv $(VENV)
	$(MAKE) requirements

# install python requirements to venv
.PHONY: requirements
requirements: .venv  
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --upgrade --compile-bytecode --no-build \
	   -r pyreflow/requirements-dev.txt

# build pyreflow for development
.PHONY: build
build: .venv  
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/maturin develop -m pyreflow/Cargo.toml $(ARGS)

# invoke everyone's fav virtual assistant
.PHONY: clippy
clippy:
	cargo clippy --all-targets --locked -- -D warnings -D clippy::dbg_macro

# clean up caches/build stuff/venv
.PHONY: clean
clean:  
	@rm -rf .venv/
	@cargo clean
