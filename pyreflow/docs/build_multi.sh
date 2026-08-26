#! /usr/bin/env bash

# target output directory where new docs will live (relative to cwd)
out=$1

# directory where stuff will be built (absolute)
builddir=$2

here=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
src=$here/source

# make build output
mkdir -p $out/en

# for ref in f671a33b 2cee4d61;
for ref in f671a33b;
do
    echo "Starting with $ref"
    target=$builddir/wt-$ref
    uvdir=$target/pyreflow
    git worktree add --force $target $ref
    # sync the venv and build pyreflow, split across two commands to prevent
    # uv from building twice (once during syncing, which we don't need)
    uv --directory=$uvdir sync --group docs --all-extras --no-install-project
    (exit) && uv --directory=$uvdir run --no-sync maturin develop
    # run sphinx
    (exit) && uv --directory=$uvdir run --no-sync $here/build.sh $src _site_tmp 
    # copy the html to the output dir
    (exit) && mv $uvdir/_site_tmp/html $out/en/$ref
    # copy the version list to _static, this is the same for ref
    (exit) && cp $src/switcher.json $out/en/$ref/_static
    rm -rf $target
    echo "Done with $ref"
done
