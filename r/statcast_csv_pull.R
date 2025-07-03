#!/usr/bin/env Rscript
# ---- Pull yesterday’s Statcast CSV directly from Savant -------------

library(readr)      # fast CSV reader
library(arrow)      # Parquet writer
library(lubridate)

out_dir <- Sys.getenv("OUTPUT_DIR", "stage")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

yday <- Sys.Date() - 1  # yesterday’s date

pull_sc <- function(pt = c("batter", "pitcher")) {
  pt <- match.arg(pt)
  url <- sprintf(
    "https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=%s&game_date_gt=%s&game_date_lt=%s",
    pt, yday, yday
  )
  df <- read_csv(url, show_col_types = FALSE, progress = FALSE)
  write_parquet(
    df,
    file.path(out_dir, sprintf("statcast_%s_%s.parquet", pt, yday))
  )
  message(sprintf("✅ %s rows written for %s", nrow(df), pt))
}

pull_sc("batter")
pull_sc("pitcher")
