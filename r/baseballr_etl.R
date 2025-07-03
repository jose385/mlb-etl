#!/usr/bin/env Rscript
# Uses statcast_search() to export batter + pitcher tables:contentReference[oaicite:7]{index=7}
library(baseballr); library(arrow); library(lubridate)

out_dir <- Sys.getenv("OUTPUT_DIR", "stage")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

yday <- Sys.Date() - 1
pitch <- statcast_search(start_date = yday, end_date = yday, player_type = "pitcher")
bat   <- statcast_search(start_date = yday, end_date = yday, player_type = "batter")

write_parquet(pitch, file.path(out_dir, sprintf("statcast_pitch_%s.parquet", yday)))
write_parquet(bat,   file.path(out_dir, sprintf("statcast_bat_%s.parquet",   yday)))
print(paste("✅ wrote", nrow(pitch)+nrow(bat), "rows for", yday))

#!/usr/bin/env Rscript
# Pull yesterday’s Statcast CSV directly and write Parquet
library(readr)
library(arrow)
library(lubridate)

out_dir <- Sys.getenv("OUTPUT_DIR", "stage")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

yday <- Sys.Date() - 1

pull_sc <- function(pt = c("batter", "pitcher")) {
  pt <- match.arg(pt)
  url <- sprintf(
    "https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=%s&game_date_gt=%s&game_date_lt=%s",
    pt, yday, yday
  )
  df <- read_csv(url, show_col_types = FALSE, progress = FALSE)
  write_parquet(df, file.path(out_dir, sprintf("statcast_%s_%s.parquet", pt, yday)))
  message(sprintf("✅ %s rows written for %s", nrow(df), pt))
}

pull_sc("batter")
pull_sc("pitcher")
