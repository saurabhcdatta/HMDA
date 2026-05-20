# =============================================================================
# 03_Combine_HMDA_Parquet.R  (v3 - streaming, low memory)
# -----------------------------------------------------------------------------
# Purpose : Materialize the partitioned HMDA Parquet store into a single
#           combined dataset for handoff.
#
# IMPORTANT: For your own analytical work you do NOT need this script.
# arrow::open_dataset(parquet_dir) already treats the folder as one logical
# table. Use this only when you must hand the panel to a tool or person
# who wants a self-contained artifact.
#
# Design choices to avoid OOM at HMDA-LAR scale:
#   - No global sort (arrange across files would buffer ~all data in RAM).
#   - Output is a FOLDER of Parquet files (partitioned by src_year), not a
#     single mega-file. Still readable as one dataset by Arrow / DuckDB /
#     pandas / Spark / etc.
#   - Streaming write via write_dataset() means peak RAM stays modest.
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
  output_dir        = "S:/Projects/HMDA/Time_Series/Data/hmda_ts_2019_2025",
  compression       = "zstd",
  compression_level = 3L,
  # Partition the output by src_year. Recipients can then read just one year
  # without scanning the whole panel.
  partition_by_year = TRUE,
  # Optional numeric casting -- streams through Arrow's C++ engine.
  cast_numeric      = FALSE,
  numeric_cols      = c(
    "loan_amount", "income", "interest_rate", "rate_spread",
    "property_value", "loan_term", "ln_term_orig",
    "loan_to_value_ratio", "combined_loan_to_value_ratio",
    "total_loan_costs", "total_points_and_fees",
    "origination_charges", "discount_points", "lender_credits",
    "intro_rate_period", "prepayment_penalty_term", "total_units",
    "multifamily_affordable_units"
  )
)

main <- function() {
  if (!dir.exists(CONFIG$parquet_dir)) {
    stop("Parquet directory not found: ", CONFIG$parquet_dir)
  }

  message("Opening dataset: ", CONFIG$parquet_dir)
  ds <- arrow::open_dataset(CONFIG$parquet_dir, unify_schemas = TRUE)
  message("Schema columns: ", length(ds$schema$names))
  message("Output: ", CONFIG$output_dir)
  message("Partition by year: ", CONFIG$partition_by_year)
  message("Cast numeric: ", CONFIG$cast_numeric)

  t0 <- Sys.time()
  q  <- ds

  if (CONFIG$cast_numeric) {
    cols_in_data <- intersect(CONFIG$numeric_cols, ds$schema$names)
    missing      <- setdiff(CONFIG$numeric_cols, ds$schema$names)
    if (length(missing)) {
      message("Skipping numeric cast for absent cols: ",
              paste(missing, collapse = ", "))
    }
    cast_exprs <- setNames(
      lapply(cols_in_data,
             function(c) rlang::expr(arrow::cast(!!rlang::sym(c),
                                                 arrow::float64()))),
      cols_in_data
    )
    q <- dplyr::mutate(q, !!!cast_exprs)
  }

  if (dir.exists(CONFIG$output_dir)) {
    message("Removing existing output directory")
    dir_delete(CONFIG$output_dir)
  }
  dir_create(CONFIG$output_dir)

  # Streaming write. No global sort. Optional Hive-style partitioning so each
  # year becomes its own subfolder -- this is the fastest read pattern for
  # year-filtered queries downstream.
  write_args <- list(
    dataset           = q,
    path              = CONFIG$output_dir,
    format            = "parquet",
    compression       = CONFIG$compression
  )
  if (CONFIG$partition_by_year) {
    write_args$partitioning <- "src_year"
  }

  do.call(arrow::write_dataset, write_args)

  elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 2)
  size_gb <- round(sum(file.info(
    list.files(CONFIG$output_dir, recursive = TRUE, full.names = TRUE)
  )$size, na.rm = TRUE) / 1024^3, 2)

  message(sprintf("Done in %s min. Total size: %s GB", elapsed, size_gb))
  message("Read it back with: arrow::open_dataset('", CONFIG$output_dir, "')")
}

if (sys.nframe() == 0L) main()
