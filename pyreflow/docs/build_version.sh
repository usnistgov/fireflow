#! /usr/bin/env bash

# target output directory where new docs will live (relative to cwd)
out=$1

# directory where stuff will be built (absolute)
builddir=$2

# the cargo build dir
rs_target=$3

# the target ref to build
ref=$4

here=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
src=$here/source

# make build output
mkdir -p $out/en

if [ $? -eq 0 ] && [ ! -d $out/en/$ref ]; then
    echo "Starting with $ref"
    target=$builddir/wt-$ref
    uvdir=$target/pyreflow
    git worktree add --force $target $ref
    # Sync the venv and build pyreflow, split across two commands to prevent uv
    # from building twice (once during syncing, which we don't need). Also use
    # only one target directory to cache most of rust's output between versions.
    uv --directory=$uvdir sync --group docs --all-extras --no-install-project
    (exit) && CARGO_TARGET_DIR=$rs_target uv --directory=$uvdir run --no-sync maturin develop
    # run sphinx
    (exit) && uv --directory=$uvdir run --no-sync $here/build.sh $src _site_tmp
    # copy the html to the output dir
    (exit) && mv $uvdir/_site_tmp/html $out/en/$ref
else
    echo "$ref already exists, skipping"
fi
