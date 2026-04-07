# ------------------------------------------------------------------
# Script: Prepare HYPE-style Qobs.txt from hydrometric stations
#        + summary, tab-delimited station file, shapefile, and interactive map
# Author: CB
# ------------------------------------------------------------------

# -------------------------------
# 0. Clear environment & set timezone
# -------------------------------
rm(list = ls()); gc()
Sys.setenv(TZ = "GMT")

# -------------------------------
# 1. Load required libraries
# -------------------------------
library(readxl)
library(readr)
library(dplyr)
library(lubridate)
library(purrr)
library(fs)
library(ggplot2)
library(sf)
library(ggspatial)
library(leaflet)
library(htmlwidgets)

# -------------------------------
# 2. Define input/output paths
# -------------------------------
HA_Selected <- c(1:40)  # optional: limit to specific basins for testing

ha_path <- "K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Qobs/scripts/Hydrometric Areas/Hydrometric_Areas.shp"
metadata_path <- "K:/Fluvial/Hydrometric_Network/Hydrometric_Collated_Info/Sources_2025-08-18/Hydro_Stations_Metadata_Gen_2025-08-18.xlsx"

base_paths <- c(
  "C:/Users/CBroderick/Downloads/OPW/Data/Q_Output_2026-04-05/Gauges",
  "C:/Users/CBroderick/Downloads/EPA/Data/Q_Output_2026-04-05/Gauges"
)

# -------------------------------
# 3. Read and filter station metadata
# -------------------------------
df <- read_excel(metadata_path)

df_filt <- df %>%
  filter(!is.na(Hydrometric_ID), !is.na(ID_pad))

# -------------------------------
# 4. Determine basins to process
# -------------------------------
unique_basins <- HA_Selected  # for testing
# unique(df_filt$Hydrometric_ID)  # for all basins

# -------------------------------
# 5. Loop over each basin
# -------------------------------
for (basin in unique_basins) {
  
  cat("Processing Basin:", basin, "\n")
  
  # Filter stations in the current basin
  sub <- df_filt %>% filter(Hydrometric_ID == basin)
  
  if (nrow(sub) == 0) {
    warning("No valid stations for Basin ", basin)
    next
  }
  
  # -------------------------------
  # 5a. Prepare output directory
  # -------------------------------
  out_dir <- file.path(paste0("C:/Users/CBroderick/Downloads/EPA+OPW/Hydrometric_Collated_Qobs/Q_Output/HA_", basin, "/Daily_09_09"))
  fs::dir_create(out_dir)
  
  # -------------------------------
  # 5b. Load station discharge data
  # -------------------------------
  station_names <- as.character(sub$ID_pad)
  station_dfs <- vector("list", length = nrow(sub))
  names(station_dfs) <- station_names
  
  for (i in seq_len(nrow(sub))) {
    fid <- as.character(sub$ID_pad[i])
    df_station <- NULL
    
    for (base in base_paths) {
      filepath <- file.path(base, paste0("GaugeID_", fid), "Daily_Discharge_09_09.rds")
      if (file.exists(filepath)) {
        df_station <- tryCatch({
          read_rds(filepath) %>%
            select(Date_09, Discharge) %>%
            rename(!!fid := Discharge)
        }, error = function(e) {
          cat("  ✘ Error reading file for station:", fid, ":", e$message, "\n")
          NULL
        })
        if (!is.null(df_station)) break
      }
    }
    
    if (!is.null(df_station)) {
      station_dfs[[fid]] <- df_station
      cat("  ✔ Loaded station:", fid, "\n")
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
  
  # -------------------------------
  # 5c. Merge station data and write HYPE-style Qobs
  # -------------------------------
  joined <- reduce(station_dfs, full_join, by = "Date_09") %>%
    arrange(Date_09)
  
  station_cols <- setdiff(names(joined), "Date_09")
  
  joined <- joined %>%
    mutate(date = format(Date_09, "%Y-%m-%d")) %>%
    select(date, all_of(station_cols))
  
  out_file <- file.path(out_dir, "Qobs_Daily_09_09.txt")
  
  # Write header
  cat(paste(c("date", station_cols), collapse = "\t"), "\n", file = out_file)
  
  # Write data
  write.table(joined,
              file = out_file,
              append = TRUE,
              sep = "\t",
              row.names = FALSE,
              col.names = FALSE,
              quote = FALSE,
              na = "-9999")
  
  cat("✔ Saved HYPE-style Qobs.txt to:", out_file, "\n")
  
  # -------------------------------
  # 5d. Save list of stations with data & metadata
  # -------------------------------
  ids_with_data <- names(station_dfs)
  writeLines(ids_with_data, file.path(out_dir, "stations_with_data_Daily_09_09.txt"))
  write.csv(df %>% filter(ID_pad %in% ids_with_data),
            file.path(out_dir, "stations_with_data_meta_Daily_09_09.csv"),
            row.names = FALSE)
  
  # -------------------------------
  # 5e. Check date order & missing days
  # -------------------------------
  date_times <- as.POSIXct(joined$date, format = "%Y-%m-%d", tz = "GMT")
  
  if (is.unsorted(date_times)) warning("Date column not sorted!")
  time_diffs <- as.numeric(diff(date_times), units = "hours")
  if (any(time_diffs != 24)) warning("Missing days detected!")
  
  # -------------------------------
  # 5f. Summarize data per station
  # -------------------------------
  summary_df <- tibble(station = station_cols) %>%
    rowwise() %>%
    mutate(
      first_non_missing = min(date_times[!is.na(joined[[station]])], na.rm = TRUE),
      last_non_missing  = max(date_times[!is.na(joined[[station]])], na.rm = TRUE),
      percent_available = mean(!is.na(joined[[station]])) * 100
    ) %>%
    ungroup()
  
  summary_df <- left_join(summary_df, df %>% filter(ID_pad %in% station_cols),
                          by = c("station" = "ID_pad"))
  
  write.csv(summary_df,
            file.path(out_dir, "station_data_summary_Daily_09_09.csv"),
            row.names = FALSE)
  
  # -------------------------------
  # 5g. Create comma-delimited HYPE station file
  # -------------------------------
  meta_df <- read.csv(file.path(out_dir, "stations_with_data_meta_Daily_09_09.csv"),
                      stringsAsFactors = FALSE)
  
  out_df <- meta_df %>%
    mutate(
      KEY = row_number(),
      ID = ID_pad,
      NAME = Name,
      LAT = Latitude,
      LON = Longitude,
      DRAINAGE = Area,
      RADIUS = 0.004166666666666667
    ) %>%
    select(KEY, ID, NAME, LAT, LON, DRAINAGE, RADIUS)
  
  tab_out_file <- file.path(out_dir, "candidate_stations_for_hype_Daily_09_09.txt")
  write.table(out_df, file = tab_out_file, sep = ",",
              row.names = FALSE, col.names = TRUE, quote = FALSE, na = "")
  
  cat("✔ Created tab-delimited HYPE station file at:", tab_out_file, "\n")
  
  # -------------------------------
  # 5h. Create shapefile for stations
  # -------------------------------
  # Prepare shapefile with safe field names
  shapefile_sf <- meta_df %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326) %>%
    mutate(
      KEY = out_df$KEY,
      ID   = out_df$ID,    # rename original ID_pad
      NAME = out_df$NAME,
      LAT  = out_df$LAT,
      LON  = out_df$LON,
      AREA = out_df$DRAINAGE,  # avoid 'DRAINAGE' long name
      RAD  = out_df$RADIUS
    ) %>%
    select(KEY, ID, NAME, LAT, LON, AREA, RAD)
  
  shapefile_path <- file.path(out_dir, "candidate_stations_for_hype_Daily_09_09.shp")
  st_write(shapefile_sf, shapefile_path, delete_layer = TRUE, quiet = TRUE)
  
  cat("✔ Created shapefile of stations at:", shapefile_path, "\n")
  
  # -------------------------------
  # 5i. Create interactive map
  # -------------------------------
  ha_sf <- st_read(ha_path, quiet = TRUE) %>% st_transform(4326)
  if ("Hydrometric_ID" %in% names(ha_sf)) {
    ha_sf <- ha_sf %>% filter(Hydrometric_ID == basin)
  }
  
  stations_sf <- summary_df %>%
    filter(!is.na(Longitude), !is.na(Latitude)) %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326)
  
  pal <- colorNumeric(palette = "plasma", domain = stations_sf$percent_available, na.color = "grey")
  
  stations_sf$popup <- paste0(
    "<div style='font-size:13px'>",
    "<b>Station ID:</b> ", stations_sf$station, "<br>",
    "<b>% Available:</b> ", round(stations_sf$percent_available, 1), "%<br>",
    "<b>First:</b> ", stations_sf$first_non_missing, "<br>",
    "<b>Last:</b> ", stations_sf$last_non_missing, "<br>",
    "<hr style='margin:4px 0;'>",
    "<b>Area:</b> ", round(stations_sf$Area, 1), " km²<br>",
    "<b>Responsible Body:</b> ", stations_sf$Responsible_Body, "<br>",
    "<b>Reliability:</b> ", stations_sf$Reliability_OPW,
    "</div>"
  )
  
  m <- leaflet() %>%
    addProviderTiles(providers$OpenStreetMap, group = "OSM") %>%
    addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%
    addPolygons(data = ha_sf, fill = FALSE, color = "black", weight = 2, group = "Hydrometric Area") %>%
    addCircleMarkers(data = stations_sf, radius = 6, fillColor = ~pal(percent_available),
                     color = "black", weight = 1.5, stroke = TRUE, fillOpacity = 0.9, popup = ~popup,
                     group = "Stations") %>%
    addLegend("bottomright", pal = pal, values = stations_sf$percent_available,
              title = "% Data Available", opacity = 1) %>%
    addLayersControl(baseGroups = c("OSM", "Satellite"),
                     overlayGroups = c("Hydrometric Area", "EPA Rivers", "Stations"),
                     options = layersControlOptions(collapsed = FALSE))
  
  map_file <- file.path(out_dir, paste0("station_availability_map_HA_", basin, "_Daily_09_09.html"))
  saveWidget(m, map_file, selfcontained = FALSE)
  
  cat("✔ Saved interactive map to:", map_file, "\n")
  cat("\nFinished processing Basin:", basin, "\n\n")
}