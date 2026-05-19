# =============================================================================
# 02_HMDA_Analysis_Helpers.R
# -----------------------------------------------------------------------------
# Purpose : Reusable helpers for analytical workloads (ANOVA, regression,
#           time-series) on the HMDA Parquet dataset. Source this file at
#           the top of any analysis script.
#
# Author  : Saurabh C. Datta
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(rlang)
})

# Default Parquet location. Override per-call if needed.
HMDA_PARQUET_DIR <- "S:/Projects/HMDA/Time_Series/Data/parquet"

#' Open the HMDA Parquet dataset
#'
#' Returns an Arrow Dataset object. No data is read until you call collect().
#' Schema is unified across files via unify_schemas = TRUE so new columns in
#' later years are handled automatically.
hmda_dataset <- function(path = HMDA_PARQUET_DIR) {
  if (!dir.exists(path)) {
    stop("Parquet directory not found: ", path,
         "\nRun 01_Build_HMDA_Parquet.R first.")
  }
  arrow::open_dataset(path, unify_schemas = TRUE)
}

#' Pull a model-ready slice into R memory
#'
#' All filtering and column selection is pushed down to Arrow's C++ engine,
#' so only the requested slice ever enters R memory.
#'
#' @param years    Character vector of src_year values (e.g. c("hmda22","hmda23")).
#'                 NULL = all years.
#' @param cols     Columns to keep. NULL = all columns (rarely what you want).
#' @param filters  A quosure or expression for additional row filters, e.g.
#'                 rlang::expr(action_taken %in% c(1,3) & loan_purpose == 1)
#' @param ds       Optional pre-opened dataset (saves a re-open in loops).
hmda_slice <- function(years   = NULL,
                       cols    = NULL,
                       filters = NULL,
                       ds      = NULL) {
  if (is.null(ds)) ds <- hmda_dataset()

  q <- ds
  if (!is.null(years))   q <- dplyr::filter(q, src_year %in% !!years)
  if (!is.null(filters)) q <- dplyr::filter(q, !!filters)
  if (!is.null(cols))    q <- dplyr::select(q, dplyr::all_of(cols))

  dplyr::collect(q)
}

#' Row counts by year — fast sanity check after a build
hmda_row_counts <- function(ds = NULL) {
  if (is.null(ds)) ds <- hmda_dataset()
  ds |>
    dplyr::count(src_year) |>
    dplyr::arrange(src_year) |>
    dplyr::collect()
}

# ---- Example usage (do not run on source) -----------------------------------
#
# source("02_HMDA_Analysis_Helpers.R")
#
# # 1. Sanity check
# hmda_row_counts()
#
# # 2. Pull a model slice
# model_df <- hmda_slice(
#   years   = c("hmda19","hmda20","hmda21","hmda22","hmda23","hmda24","hmda25"),
#   cols    = c("src_year","state_code","loan_amount","income",
#               "interest_rate","action_taken","loan_purpose"),
#   filters = rlang::expr(action_taken %in% c(1,3) & loan_purpose == 1 &
#                         income > 0 & loan_amount > 0)
# )
#
# # 3. Fit a panel regression with year + state fixed effects
# library(fixest)
# fit <- feols(
#   log(loan_amount) ~ log(income) + interest_rate | src_year + state_code,
#   data    = model_df,
#   cluster = ~ state_code
# )
# summary(fit)
#
# # 4. ANOVA across years
# aov_fit <- aov(log(loan_amount) ~ src_year, data = model_df)
# summary(aov_fit)
