#! /usr/bin/env Rscript

suppressPackageStartupMessages(library(flowCore))

args <- commandArgs(trailingOnly = TRUE)
mode <- args[1]
inpath <- args[2]
outpath <- args[3]

fr <- read.FCS(inpath, transformation = FALSE, truncate_max_range = FALSE)

if (mode == "text") {
  # TODO find a better way to do this; this will "measure" the write speed for
  # TEXT by only writing one cell (out of 1000s) in the DATA segment. Crude,
  # but probably not terrible.
  m <- matrix(fr@exprs[1, 1])
  colnames(m) <- colnames(fr@exprs)[[1]]
  exprs(fr) <- m
} else if (mode != "data") {
  stop("unknown mode given")
}

t0 <- Sys.time()
out <- write.FCS(fr, outpath, endian = "little")
elapsed <- as.numeric(Sys.time() - t0, units = "secs")

cat(sprintf("%.9f\n", elapsed))
