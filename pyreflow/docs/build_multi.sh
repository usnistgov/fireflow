#! /usr/bin/env bash

# target output directory where new docs will live (relative to cwd)
out=$1

# directory where stuff will be built (absolute)
builddir=$2

# list of refs for which to make docs, these are assumed to be in order from
# latest (top) to earliest (bottom)
refs=$(cat $3)

here=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
src=$here/source

# make build output
mkdir -p $out/en

first=1
rustdir=$builddir/target

for ref in $refs;
do
    if [ $? -eq 0 ] && [ ! -d $out/en/$ref ]; then
        echo "Starting with $ref"
        target=$builddir/wt-$ref
        uvdir=$target/pyreflow
        git worktree add --force $target $ref
        # Sync the venv and build pyreflow, split across two commands to prevent
        # uv from building twice (once during syncing, which we don't need).
        # Also use only one target directory to cache most of rust's output
        # between versions.
        uv --directory=$uvdir sync --group docs --all-extras --no-install-project
        (exit) && CARGO_TARGET_DIR=$rustdir uv --directory=$uvdir run --no-sync maturin develop
        # run sphinx
        (exit) && uv --directory=$uvdir run --no-sync $here/build.sh $src _site_tmp
        # copy the html to the output dir
        (exit) && mv $uvdir/_site_tmp/html $out/en/$ref
        # make link to latest version
        (exit) && [ $first == 1 ] && ln -s $ref $out/en/latest
    else
        echo "$ref already exists, skipping"
    fi
    rm -rf $target
    echo "Done with $ref"
    first=0
done
