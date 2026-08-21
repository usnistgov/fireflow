#! /usr/bin/env Rscript

# Run a deamon in a loop which listens for commands that will test flowcore
# in various ways.
#
# The reason this is in a loop is to keep all flowcore testing within one
# process. Not only is this faster to test since we don't need to create/kill R
# subprocesses repeatedly from python, it is a fairer benchmark since the memory
# of the R process and all function calls within it will be cached (same as
# python).
#
# Overall architecture of the loop:
#
# * Assume there are two pipes, fed to the script as arguments. The first pipe
#   is python->R and the second is R->python
# * Loop will endlessly wait for input on the first pipe
# * Upon getting input, will dispatch to a function which calls flowcore. The
#   function and its behavior are dictated by the input sent to the pipe from
#   from Python.
# * After function runs, return output on the other pipe. Python is assumed to
#   be waiting for it.
# * Rinse and repeat

suppressPackageStartupMessages(library(flowCore))
library(microbenchmark)

# read an FCS file and dump its DATA segment as a tsv file
dump_dataframe <- function(inpath, outpath) {
  fr <- read.FCS(inpath, transformation = FALSE, truncate_max_range = FALSE)
  # return dummy value to give something for python to read while it blocks
  write.table(fr@exprs, outpath, sep = "\t", row.names = TRUE, col.names = FALSE)
  "done"
}

# run FCS read tests
test_read <- function(is_text, path) {
  if (is_text) {
    gc()
    t0 <- get_nanotime()
    fr <- read.FCSheader(path)
  } else {
    gc()
    t0 <- get_nanotime()
    fr <- read.FCS(path, transformation = FALSE, truncate_max_range = FALSE)
  }
  get_nanotime() - t0
}

# run FCS write tests
test_write <- function(is_text, inpath, outpath) {
  fr <- read.FCS(inpath, transformation = FALSE, truncate_max_range = FALSE)
  to_write <- if (is_text) {
    # TODO find a better way to do this; this will "measure" the write speed for
    # TEXT by only writing one cell (out of 1000s) in the DATA segment. Crude,
    # but probably not terrible.
    m <- matrix(fr@exprs[1, 1])
    colnames(m) <- colnames(fr@exprs)[[1]]
    exprs(fr) <- m
  } else {
    fr
  }
  gc()
  t0 <- get_nanotime()
  out <- write.FCS(fr, outpath, endian = "little")
  get_nanotime() - t0
}

dispatch <- function(line_args) {
  cmd <- line_args[[1]]
  if (cmd == "dump") {
    inpath <- line_args[[2]]
    outpath <- line_args[[3]]
    dump_dataframe(inpath, outpath)
  } else {
    mode <- line_args[[2]]
    is_text <- if (mode == "text") {
      TRUE
    } else if(mode == "data") {
      FALSE
    } else {
      stop("unknown mode")
    }
    if (cmd == "read") {
      test_read(is_text, line_args[[3]])
    } else if(cmd == "write") {
      test_write(is_text, line_args[[3]], line_args[[4]])
    } else {
      stop("unknown command")
    }
  }
}

args <- commandArgs(trailingOnly = TRUE)
read_pipe <- args[[1]]
write_pipe <- args[[2]]

repeat {
  con <- file(read_pipe, open = "r", raw = TRUE)
  line <- readLines(con, n = 1, warn = FALSE)
  close(con)
  if (line == "exit") {
    break
  } else {
    line_args <- strsplit(line, " ")[[1]]
    out <- dispatch(line_args)
    con <- file(write_pipe, open = "w", raw = TRUE)
    writeLines(as.character(out), con)
    close(con)
  }
}
