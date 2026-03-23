# ------------------------------------------------------------------
# Script: Prepare HYPE-style Qobs.txt from hydrometric stations
# Author: CB
# Requirements:
# - HYPE_SUBID used as column headers
# - Missing values = -9999
# - Date format: yyyy-mm-dd HH:MM
# - Continuous hourly data
# ------------------------------------------------------------------

# Clear environment and set timezone
rm(list = ls()); gc()
Sys.setenv(TZ = "GMT")

# Load required libraries
library(readxl)
library(readr)
library(dplyr)
library(lubridate)
library(purrr)
library(fs)

# Optional: limit processing to a single basin for testing
HA_Selected <- 16

# Step 1: Read metadata file
metadata_path <- "K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Info/Sources_2025-08-18/Hydro_Stations_Metadata_Gen_2025-08-18.xlsx"
df <- read_excel(metadata_path)

# Step 2: Filter valid entries (must have Hydrometric_ID and ID_pad)
df_filt <- df %>% filter(!is.na(Hydrometric_ID), !is.na(ID_pad))

# Step 3: Define source paths
base_paths <- c(
  "C:/Users/CBroderick/Downloads/OPW/Q_Output_2026-03-04/Gauges",
  "C:/Users/CBroderick/Downloads/EPA/Q_Output_2026-03-04/Gauges"
)

# Step 4: Determine basins to process
unique_basins <- HA_Selected  # single basin for testing; replace with unique(df_filt$Hydrometric_ID) for all

# Step 5: Loop over basins
for (basin in unique_basins) {
  
  cat("Processing Basin:", basin, "\n")
  sub <- df_filt %>% filter(Hydrometric_ID == basin)
  
  if (nrow(sub) == 0) {
    warning("No valid stations for Basin ", basin)
    next
  }
  
  # Prepare output directory
  out_dir <- file.path(paste0("K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Qobs/Q_Output/HA_", basin))
  fs::dir_create(out_dir)
  
  # Determine station names and output filename
  station_names <- as.character(sub$ID_pad)
  out_file <- file.path(out_dir, "Qobs.txt")

  # Initialize list for station data
  station_dfs <- vector("list", length = nrow(sub))
  names(station_dfs) <- station_names
  
  # Load station data
  for (i in seq_len(nrow(sub))) {
    fid <- as.character(sub$ID_pad[i])
    station_col_name <- fid
    df_station <- NULL
    
    for (base in base_paths) {
      filepath <- file.path(base, paste0("GaugeID_", fid), "Hourly_Discharge.rds")
      if (file.exists(filepath)) {
        df_station <- tryCatch({
          read_rds(filepath) %>%
            select(Hour, Discharge) %>%
            rename(!!station_col_name := Discharge)
        }, error = function(e) {
          cat("  ✘ Error reading file for station:", fid, ":", e$message, "\n")
          NULL
        })
        if (!is.null(df_station)) break
      }
    }
    
    if (!is.null(df_station)) {
      station_dfs[[station_col_name]] <- df_station
      cat("  ✔ Loaded station:", fid, "as column:", station_col_name, "\n")
    } else {
      cat("  ✘ No file found for station:", fid, "\n")
    }
  }
  
  # Remove empty entries
  station_dfs <- station_dfs[!sapply(station_dfs, is.null)]
  if (length(station_dfs) == 0) {
    warning("No station data available for Basin ", basin)
    next
  }
  
  # Merge all station data by Hour
  joined <- reduce(station_dfs, full_join, by = "Hour") %>%
    arrange(Hour)
  
  # Create 'date' column and reorder
  station_cols <- setdiff(names(joined), "Hour")
  joined <- joined %>%
    mutate(date = format(Hour, "%Y-%m-%d %H:%M")) %>%
    select(date, all_of(station_cols))
  
  # Write header (first row)
  header <- paste(c("date", station_cols), collapse = "\t")
  cat(header, "\n", file = out_file)
  
  # Write data: tab-delimited, NA replaced with -9999
  write.table(joined,
              file = out_file,
              append = TRUE,
              sep = "\t",
              row.names = FALSE,
              col.names = FALSE,
              quote = FALSE,
              na = "-9999")
  
  cat("✔ Saved HYPE-style Qobs.txt to:", out_file, "\n")
  
  # Save list of stations with data
  ids_with_data <- names(station_dfs)
  ids_file <- file.path(out_dir, "stations_with_data.txt")
  writeLines(ids_with_data, ids_file)
  cat("✔ Saved list of stations with data to:", ids_file, "\n")
  
  # Save metadata of stations with data
  ids_meta_file <- file.path(out_dir, "stations_with_data_meta.csv")
  write.csv(df %>% filter(ID_pad %in% ids_with_data), ids_meta_file, row.names = FALSE)
  
  # --- Additional check: Verify date order and hourly continuity ---
  cat("Checking date order and missing hours...\n")
  date_times <- as.POSIXct(joined$date, format = "%Y-%m-%d %H:%M", tz = "GMT")
  
  # Check ascending order
  if (is.unsorted(date_times)) warning("Date column is not sorted in ascending order!")
  else cat("✔ Date column is sorted.\n")
  
  # --- Check for missing hours ---
  time_diffs <- as.numeric(diff(date_times), units = "hours")  # convert to hours
  missing_hours <- which(time_diffs != 1)
  
  if (length(missing_hours) > 0) {
    warning("Missing hours detected in the time series! Check the data for individual stations.")
  } else {
    cat("✔ No missing hours detected.\n")
  }
  
  cat("\nFinished processing Basin:", basin, "\n\n")
}