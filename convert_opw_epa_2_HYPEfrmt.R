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

# Logical flag to control naming scheme
SUBID <- FALSE  # Set to FALSE to use ID_pad instead

# Optional testing flag to limit processing
select_single_HA <- TRUE

# Step 1: Read metadata file
metadata_path <- "K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Info/Sources_2025-08-18/Hydro_Stations_Metadata_Gen_2025-08-18.xlsx"
df <- read_excel(metadata_path)

# Step 2: Filter valid entries (must have Hydrometric_ID and station ID)
df_filt <- df %>% filter(!is.na(Hydrometric_ID), !is.na(ID_pad))

# Step 3: Define source paths
base_paths <- c(
  "C:/Users/CBroderick/Downloads/OPW/Q_Output_2025-08-28/Gauges",
  "C:/Users/CBroderick/Downloads/EPA/Q_Output_2025-08-28/Gauges"
)

# Step 4: Determine basins to process
unique_basins <- unique(df_filt$Hydrometric_ID)
if (select_single_HA) unique_basins <- 16  # For single only

# Step 5: Process each basin
for (basin in unique_basins) {
  sub <- df_filt %>% filter(Hydrometric_ID == basin)
  cat("Processing Basin:", basin, "\n")
  
  station_dfs <- vector("list", length = nrow(sub))
  
  # Prepare output directory
  out_dir <- file.path(paste0("K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Qobs/HYPE_frmt/HA_", as.character(basin)))
  fs::dir_create(out_dir)
  
  # Determine station names and output filename based on SUBID
  if (!SUBID) {
    station_names <- as.character(sub$ID_pad)
    out_file <- file.path(out_dir, "Qobs_HR.txt")
  } else {
    station_names <- as.character(sub$HYPE_SUBID)
    out_file <- file.path(out_dir, "Qobs.txt")
  }
  
  # Assign names to the station list
  names(station_dfs) <- station_names
  
  # Load data for each station
  for (i in seq_len(nrow(sub))) {
    fid <- as.character(sub$ID_pad[i])  # Always use ID_pad to locate files
    station_col_name <- if (SUBID) as.character(sub$HYPE_SUBID[i]) else fid  # Name column by SUBID or ID_pad
    
    df_station <- NULL
    
    for (base in base_paths) {
      filepath <- file.path(base, paste0("GaugeID_", fid), "Hourly_Interp_Discharge.csv")
      if (file.exists(filepath)) {
        df_station <- read_csv(filepath, show_col_types = FALSE) %>%
          rename(!!station_col_name := Value)
        
        cat("  ✔ Loaded station:", fid, "as column:", station_col_name, "\n")
        break
      }
    }
    
    if (!is.null(df_station)) {
      station_dfs[[station_col_name]] <- df_station
    } else {
      cat("  ✘ No file for station:", fid, "\n")
    }
  }
  
  # Remove empty entries
  station_dfs <- station_dfs[!sapply(station_dfs, is.null)]
  if (length(station_dfs) == 0) {
    warning("No station data for Basin ", basin)
    next
  }
  
  # Merge all station data by Timestamp_interp
  joined <- reduce(station_dfs, full_join, by = "Timestamp_interp") %>%
    arrange(Timestamp_interp)
  
  # Format output: create 'date' column and order columns
  station_cols <- setdiff(names(joined), "Timestamp_interp")
  joined <- joined %>%
    mutate(date = format(Timestamp_interp, "%Y-%m-%d %H:%M")) %>%
    select(date, all_of(station_cols))
  
  # Write header line (tab-separated)
  header <- paste(c("date", station_cols), collapse = "\t")
  cat(header, "\n", file = out_file)
  
  # Write data (tab-delimited, no row/column names, replace NA with -9999)
  write.table(joined,
              file = out_file,
              append = TRUE,
              sep = "\t",
              row.names = FALSE,
              col.names = FALSE,
              quote = FALSE,
              na = "-9999")
  
  cat("✔ Saved HYPE-style observation file to:", out_file, "\n\n")
  
  # Save list of stations with data
  ids_with_data <- names(station_dfs)
  ids_file <- file.path(out_dir, "stations_with_data.txt")
  writeLines(ids_with_data, ids_file)
  cat("✔ Saved list of stations with data to:", ids_file, "\n\n")
  
  # Save metadata of stations with data
  ids_meta_file <- file.path(out_dir, "stations_with_data_meta.csv")
  write.csv(df %>% filter(ID_pad %in% ids_with_data), ids_meta_file, row.names = FALSE)
}
