#! /usr/bin/env Rscript

suppressPackageStartupMessages(library(flowCore))

args <- commandArgs(trailingOnly = TRUE)
mode <- args[1]
path <- args[2]

if (mode == "text") {
    t0 <- Sys.time()
    fr <- read.FCSheader(path)
} else if (mode != "data") {
  stop("unknown mode given")
} else {
    t0 <- Sys.time()
    fr <- read.FCS(path, transformation = FALSE, truncate_max_range = FALSE)
}

elapsed <- as.numeric(Sys.time() - t0, units = "secs")
cat(sprintf("%.9f\n", elapsed))

