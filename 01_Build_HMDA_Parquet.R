# =============================================================================
# 01_Build_HMDA_Parquet.R
# -----------------------------------------------------------------------------
# Purpose : Convert HMDA LAR SAS files to a partitioned Parquet dataset.
#           Run once per data refresh. Resumable: re-running skips files
#           whose Parquet output already exists.
#
# Inputs  : HMDA LAR .sas7bdat files (one per year)
# Output  : Parquet dataset partitioned by src_year, ready for arrow::open_dataset
#
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
  log_dir           = "S:/Projects/HMDA/Time_Series/Logs",
  workers           = 3L,        # tune to RAM: workers * file_size < free_RAM
  compression       = "zstd",
  compression_level = 3L,
  overwrite         = FALSE      # set TRUE to force a full rebuild
)

# ---- Logging setup ----------------------------------------------------------

dir_create(CONFIG$log_dir)
log_file <- file.path(
  CONFIG$log_dir,
  sprintf("build_parquet_%s.log", format(Sys.time(), "%Y%m%d_%H%M%S"))
)
log_appender(appender_tee(log_file))
log_threshold(INFO)

# ---- Helper: convert one SAS file ------------------------------------------

convert_one <- function(sas_path, parquet_dir, compression,
                        compression_level, overwrite) {
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

    # Write atomically: write to .tmp then rename so a crash never leaves
    # a half-written file that the resume logic would mistake for complete.
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
  log_info("Workers: {CONFIG$workers}")

  dir_create(CONFIG$parquet_dir)

  # Validate inputs up front — fail fast if anything is missing
  missing <- CONFIG$sas_files[!file.exists(CONFIG$sas_files)]
  if (length(missing)) {
    log_error("Missing input files:\n{paste(missing, collapse = '\n')}")
    stop("Aborting: missing input files.")
  }

  start_time <- Sys.time()
  plan(multisession, workers = CONFIG$workers)
  on.exit(plan(sequential), add = TRUE)

  results <- future_map(
    CONFIG$sas_files,
    convert_one,
    parquet_dir       = CONFIG$parquet_dir,
    compression       = CONFIG$compression,
    compression_level = CONFIG$compression_level,
    overwrite         = CONFIG$overwrite,
    .options          = furrr_options(seed = TRUE)
  )

  # Summarise
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

if (sys.nframe() == 0L) {
  main()
}
