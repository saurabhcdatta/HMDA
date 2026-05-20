# =============================================================================
# 03_Combine_HMDA_Parquet.R  (v2 - aware of character storage)
# -----------------------------------------------------------------------------
# Purpose : Materialize the partitioned HMDA Parquet store into a single
#           combined Parquet file. Usually NOT needed -- arrow::open_dataset()
#           treats the folder as one logical table. Use this only when a
#           downstream tool requires a single file or you want to hand off
#           the full panel as one artifact.
#
# Storage note: the build (01_Build_HMDA_Parquet.R) stores all columns as
# character to preserve HMDA "Exempt" sentinels. This script can optionally
# cast a configured set of columns to numeric at combine time, so a handoff
# file is immediately usable by recipients who don't know HMDA's quirks.
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
  sort_by_year      = TRUE,
  # If TRUE, cast the columns in numeric_cols to double at write time.
  # "Exempt" / non-numeric values become NULL in the output file.
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
  message("Writing combined file to: ", CONFIG$output_file)
  message("Cast numeric: ", CONFIG$cast_numeric)

  t0 <- Sys.time()

  q <- ds
  if (CONFIG$sort_by_year) q <- dplyr::arrange(q, src_year)

  if (CONFIG$cast_numeric) {
    cols_in_data <- intersect(CONFIG$numeric_cols, ds$schema$names)
    missing      <- setdiff(CONFIG$numeric_cols, ds$schema$names)
    if (length(missing)) {
      message("Note: these configured numeric cols are not in the dataset and ",
              "will be skipped: ", paste(missing, collapse = ", "))
    }
    # Build a mutate() call dynamically. Arrow's cast() pushes the conversion
    # into the C++ engine; bad values (e.g. "Exempt") become NULL.
    cast_exprs <- setNames(
      lapply(cols_in_data,
             function(c) rlang::expr(arrow::cast(!!rlang::sym(c),
                                                 arrow::float64()))),
      cols_in_data
    )
    q <- dplyr::mutate(q, !!!cast_exprs)
  }

  tmp_dir <- file.path(dirname(CONFIG$output_file), ".combine_tmp")
  if (dir.exists(tmp_dir)) dir_delete(tmp_dir)
  dir_create(tmp_dir)

  # Streaming write so the full panel never has to fit in RAM at once.
  arrow::write_dataset(
    q,
    path              = tmp_dir,
    format            = "parquet",
    basename_template = "part-{i}.parquet",
    compression       = CONFIG$compression,
    max_rows_per_file = .Machine$integer.max   # force a single output file
  )

  written <- list.files(tmp_dir, pattern = "^part-0\\.parquet$",
                        full.names = TRUE)
  if (length(written) != 1) {
    stop("Expected exactly one output file, got: ", length(written),
         "\nFiles: ", paste(list.files(tmp_dir), collapse = ", "))
  }

  if (file.exists(CONFIG$output_file)) file_delete(CONFIG$output_file)
  file_move(written, CONFIG$output_file)
  dir_delete(tmp_dir)

  elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 2)
  size_gb <- round(file.info(CONFIG$output_file)$size / 1024^3, 2)
  message(sprintf("Done in %s min. Output size: %s GB", elapsed, size_gb))
}

if (sys.nframe() == 0L) main()
