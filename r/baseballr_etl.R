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
