#! /usr/bin/env Rscript

# make a new renv environment, install flowCore, and freeze
#
# This only needs to be run once during development when setting up the env
# for local testing. Once created, one only needs to run renv::restore() to
# capture the lock file.
#
# NOTE: flowCore's dependencies require ccache to be installed which is not an
# R package. This script also obviously assumes you have R installed with renv

renv::init(bare = TRUE, restart = FALSE)
renv::install(c("bioc::flowCore"))
renv::snapshot(type = "all")
