# =============================================================================
# 01_Build_HMDA_Parquet.R  (v2 - schema harmonization)
# -----------------------------------------------------------------------------
# Purpose : Convert HMDA LAR SAS files to a partitioned Parquet dataset with
#           harmonized column types so years can be unioned without errors.
#
# HMDA quirk: fields like ln_term_orig, property_value, income, rate_spread,
# total_units sometimes contain sentinel strings ("Exempt", "NA") in some
# years and pure numerics in others. read_sas types the whole column as
# character when sentinels are present, which breaks schema unification.
#
# Fix: write all columns as character at the build stage (lossless, preserves
# "Exempt"). Cast to numeric per-column at analysis time using hmda_to_numeric().
#
# Author  : Saurabh C. Datta
# =============================================================================

suppressPackageStartupMessages({
  library(haven)
  library(arrow)
  library(furrr)
  library(fs)
  library(logger)
})

# ---- Configuration ----------------------------------------------------------

CONFIG <- list(
  sas_files = c(
    "S:/Projects/HMDA/Time_Series/Data/hmda19.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda20.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda21.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda22.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda23.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda24.sas7bdat",
    "S:/Projects/HMDA/Time_Series/Data/hmda25.sas7bdat"
  ),
  parquet_dir       = "S:/Projects/HMDA/Time_Series/Data/parquet",
  log_dir           = file.path(Sys.getenv("USERPROFILE"), "HMDA_Logs"),
  workers           = 1L,         # HMDA LAR is too large for parallel reads
  compression       = "zstd",
  compression_level = 3L,
  overwrite         = TRUE,       # rebuild with new schema; set FALSE after
  # Schema harmonization strategy:
  #   "all_character"  -- safest, preserves "Exempt" sentinels (RECOMMENDED)
  #   "coerce_numeric" -- forces known-problem cols to numeric, "Exempt" -> NA
  schema_mode       = "all_character"
)

# ---- Logging setup ----------------------------------------------------------

dir_create(CONFIG$log_dir)
log_file <- file.path(
  CONFIG$log_dir,
  sprintf("build_parquet_%s.log", format(Sys.time(), "%Y%m%d_%H%M%S"))
)
log_appender(appender_tee(log_file))
log_threshold(INFO)

# ---- Helpers ----------------------------------------------------------------

# Columns that have known cross-year type conflicts in HMDA LAR.
HMDA_MIXED_TYPE_COLS <- c(
  "ln_term_orig", "loan_term", "property_value", "income",
  "rate_spread", "total_units", "intro_rate_period",
  "multifamily_affordable_units",
  "combined_loan_to_value_ratio", "loan_to_value_ratio",
  "interest_rate", "total_loan_costs", "total_points_and_fees",
  "origination_charges", "discount_points", "lender_credits",
  "prepayment_penalty_term"
)

harmonize_types <- function(df, mode) {
  if (mode == "all_character") {
    df[] <- lapply(df, function(x) {
      if (inherits(x, "Date") || inherits(x, "POSIXt")) format(x) else as.character(x)
    })
    return(df)
  }
  if (mode == "coerce_numeric") {
    cols <- intersect(HMDA_MIXED_TYPE_COLS, names(df))
    for (c in cols) {
      df[[c]] <- suppressWarnings(as.numeric(as.character(df[[c]])))
    }
    return(df)
  }
  stop("Unknown schema_mode: ", mode)
}

# ---- Helper: convert one SAS file ------------------------------------------

convert_one <- function(sas_path, parquet_dir, compression,
                        compression_level, overwrite, schema_mode) {
  stem <- tools::file_path_sans_ext(basename(sas_path))
  out  <- file.path(parquet_dir, paste0(stem, ".parquet"))

  if (file.exists(out) && !overwrite) {
    return(list(file = sas_path, status = "skipped", rows = NA_integer_,
                seconds = 0, error = NA_character_))
  }

  t0 <- Sys.time()
  result <- tryCatch({
    df <- haven::read_sas(sas_path)
    df$src_year <- stem
    df <- harmonize_types(df, schema_mode)

    tmp <- paste0(out, ".tmp")
    arrow::write_parquet(
      df, tmp,
      compression       = compression,
      compression_level = compression_level,
      use_dictionary    = TRUE
    )
    file_move(tmp, out)

    list(file = sas_path, status = "ok", rows = nrow(df),
         seconds = as.numeric(difftime(Sys.time(), t0, units = "secs")),
         error = NA_character_)
  }, error = function(e) {
    list(file = sas_path, status = "error", rows = NA_integer_,
         seconds = as.numeric(difftime(Sys.time(), t0, units = "secs")),
         error = conditionMessage(e))
  })

  result
}

# ---- Main -------------------------------------------------------------------

main <- function() {
  log_info("Starting HMDA Parquet build")
  log_info("Parquet output: {CONFIG$parquet_dir}")
  log_info("Schema mode: {CONFIG$schema_mode}")
  log_info("Workers: {CONFIG$workers}  overwrite: {CONFIG$overwrite}")

  dir_create(CONFIG$parquet_dir)

  missing <- CONFIG$sas_files[!file.exists(CONFIG$sas_files)]
  if (length(missing)) {
    log_error("Missing input files:\n{paste(missing, collapse = '\n')}")
    stop("Aborting: missing input files.")
  }

  start_time <- Sys.time()

  if (CONFIG$workers > 1L) {
    plan(multisession, workers = CONFIG$workers)
    on.exit(plan(sequential), add = TRUE)
    results <- future_map(
      CONFIG$sas_files, convert_one,
      parquet_dir       = CONFIG$parquet_dir,
      compression       = CONFIG$compression,
      compression_level = CONFIG$compression_level,
      overwrite         = CONFIG$overwrite,
      schema_mode       = CONFIG$schema_mode,
      .options          = furrr_options(seed = TRUE)
    )
  } else {
    results <- lapply(CONFIG$sas_files, convert_one,
                      parquet_dir       = CONFIG$parquet_dir,
                      compression       = CONFIG$compression,
                      compression_level = CONFIG$compression_level,
                      overwrite         = CONFIG$overwrite,
                      schema_mode       = CONFIG$schema_mode)
  }

  results_df <- do.call(rbind, lapply(results, as.data.frame))
  for (i in seq_len(nrow(results_df))) {
    r <- results_df[i, ]
    if (r$status == "ok") {
      log_info("OK    {basename(r$file)}  rows={r$rows}  ({round(r$seconds,1)}s)")
    } else if (r$status == "skipped") {
      log_info("SKIP  {basename(r$file)}  (already exists)")
    } else {
      log_error("FAIL  {basename(r$file)}  -- {r$error}")
    }
  }

  total_min <- round(difftime(Sys.time(), start_time, units = "mins"), 2)
  n_ok   <- sum(results_df$status == "ok")
  n_skip <- sum(results_df$status == "skipped")
  n_err  <- sum(results_df$status == "error")

  log_info("Finished. ok={n_ok} skipped={n_skip} errors={n_err} total={total_min} min")
  log_info("Log written to: {log_file}")

  if (n_err > 0) {
    stop(sprintf("Build completed with %d error(s). See log: %s", n_err, log_file))
  }

  invisible(results_df)
}

if (sys.nframe() == 0L) main()
