# =============================================================================
# 02_HMDA_Analysis_Helpers.R  (v2 - numeric casting)
# -----------------------------------------------------------------------------
# Purpose : Reusable helpers for analytical workloads on the HMDA Parquet
#           dataset. Source this file at the top of any analysis script.
#
# Note on types: the build stores all columns as character to preserve
# HMDA "Exempt" sentinels and avoid cross-year type conflicts. Use
# hmda_to_numeric() to cast specific columns to numeric for modeling --
# "Exempt", "NA", and other non-numeric values become NA.
#
# Author  : Saurabh C. Datta
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(rlang)
})

HMDA_PARQUET_DIR <- "S:/Projects/HMDA/Time_Series/Data/parquet"

#' Open the HMDA Parquet dataset as one logical table
hmda_dataset <- function(path = HMDA_PARQUET_DIR) {
  if (!dir.exists(path)) {
    stop("Parquet directory not found: ", path,
         "\nRun 01_Build_HMDA_Parquet.R first.")
  }
  arrow::open_dataset(path, unify_schemas = TRUE)
}

#' Pull a model-ready slice into R memory
#'
#' @param years    Character vector of src_year values, or NULL for all.
#' @param cols     Columns to keep, or NULL for all.
#' @param filters  Optional rlang::expr() for additional row filters.
#' @param ds       Optional pre-opened dataset.
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

#' Cast character columns to numeric, coercing non-numeric values to NA
#'
#' Use after collecting a slice. "Exempt", "NA", and blanks become NA.
#' Warnings from as.numeric() are suppressed.
#'
#' @param df    A data.frame (the result of hmda_slice()).
#' @param cols  Character vector of column names to cast.
#' @return      The data.frame with those columns as numeric.
hmda_to_numeric <- function(df, cols) {
  missing <- setdiff(cols, names(df))
  if (length(missing)) {
    warning("Columns not in data, skipping: ",
            paste(missing, collapse = ", "))
    cols <- intersect(cols, names(df))
  }
  for (c in cols) {
    df[[c]] <- suppressWarnings(as.numeric(df[[c]]))
  }
  df
}

#' Row counts by year
hmda_row_counts <- function(ds = NULL) {
  if (is.null(ds)) ds <- hmda_dataset()
  ds |>
    dplyr::count(src_year) |>
    dplyr::arrange(src_year) |>
    dplyr::collect()
}

# ---- Example usage ----------------------------------------------------------
#
# source("02_HMDA_Analysis_Helpers.R")
#
# # 1. Sanity check
# hmda_row_counts()
#
# # 2. Pull a model slice (all columns come back as character)
# model_df <- hmda_slice(
#   years   = c("hmda19","hmda20","hmda21","hmda22",
#               "hmda23","hmda24","hmda25"),
#   cols    = c("src_year","state_code","loan_amount","income",
#               "interest_rate","action_taken","loan_purpose"),
#   filters = rlang::expr(action_taken == "1" & loan_purpose == "1")
# )
#
# # 3. Cast the numeric columns you need for modeling
# model_df <- hmda_to_numeric(
#   model_df,
#   cols = c("loan_amount","income","interest_rate")
# )
#
# # 4. Drop rows where coercion produced NA (i.e. "Exempt" rows)
# model_df <- na.omit(model_df)
#
# # 5. Fit
# library(fixest)
# fit <- feols(
#   log(loan_amount) ~ log(income) + interest_rate | src_year + state_code,
#   data    = model_df,
#   cluster = ~ state_code
# )
# summary(fit)
