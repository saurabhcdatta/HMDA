# =============================================================================
# 03_Combine_HMDA_Parquet.R
# -----------------------------------------------------------------------------
# Purpose : Materialize the partitioned HMDA Parquet store into a single
#           combined Parquet file. Usually NOT needed -- arrow::open_dataset()
#           treats the folder as one logical table. Use this only when a
#           downstream tool requires a single file or you want to hand off
#           the full panel as one artifact.
#
# Author  : Saurabh C. Datta
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
})

CONFIG <- list(
  parquet_dir       = "S:/Projects/HMDA/Time_Series/Data/parquet",
  output_file       = "S:/Projects/HMDA/Time_Series/Data/hmda_ts_2019_2025.parquet",
  compression       = "zstd",
  compression_level = 3L,
  # If TRUE, the combined file is sorted by src_year. Set FALSE for fastest write.
  sort_by_year      = TRUE
)

main <- function() {
  if (!dir.exists(CONFIG$parquet_dir)) {
    stop("Parquet directory not found: ", CONFIG$parquet_dir)
  }

  message("Opening dataset: ", CONFIG$parquet_dir)
  ds <- arrow::open_dataset(CONFIG$parquet_dir, unify_schemas = TRUE)

  message("Schema columns: ", length(ds$schema$names))
  message("Writing combined file to: ", CONFIG$output_file)

  t0 <- Sys.time()

  # Atomic write: write to .tmp, then rename
  tmp <- paste0(CONFIG$output_file, ".tmp")

  q <- ds
  if (CONFIG$sort_by_year) q <- dplyr::arrange(q, src_year)

  # write_dataset with a single file target gives us streaming write --
  # the whole panel never has to fit in RAM at once.
  arrow::write_dataset(
    q,
    path              = dirname(tmp),
    format            = "parquet",
    basename_template = paste0(basename(tmp), "-{i}"),
    compression       = CONFIG$compression,
    max_rows_per_file = .Machine$integer.max   # force a single output file
  )

  # write_dataset names files with the template; rename to the final name
  written <- list.files(dirname(tmp),
                        pattern = paste0("^", basename(tmp), "-0"),
                        full.names = TRUE)
  if (length(written) != 1) {
    stop("Expected exactly one output file, got: ", length(written))
  }
  file_move(written, CONFIG$output_file)

  elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 2)
  size_gb <- round(file.info(CONFIG$output_file)$size / 1024^3, 2)
  message(sprintf("Done in %s min. Output size: %s GB", elapsed, size_gb))
}

if (sys.nframe() == 0L) main()
