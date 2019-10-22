library(synapser)
library(tidyverse)
library(optparse)
library(furrr)

TESTING <- FALSE

read_args <- function() {
  parser <- optparse::OptionParser(
    description = paste("Compute mhealthtools tremor features on a given",
                        "input table and its columns containing file handles",
                        "of accelerometer and gyroscope measurements."))
  parser <- add_option(parser, "--inputTable", help="Synapse ID of input table.")
  parser <- add_option(parser, "--accelerometerColumn", default=NULL,
                       help="Column name of accelerometer column.")
  parser <- add_option(parser, "--gyroscopeColumn", default=NULL,
                       help="Column name of gyroscope column.")
  parser <- add_option(parser, "--outputParent",
                       help="Synapse ID of the output's parent.")
  parser <- add_option(parser, "--parallel", action="store_true", default=FALSE,
                       help="Extract features in parallel.")
  parser <- add_option(parser, "--cacheDir", help="Store intermediate results.")
  parse_args(parser)
}

read_sensor_data <- function(p) {
  if (is.na(p) || is.null(p)) {
    return(NULL)
  } else if (is.character(p)) {
    d <- read_csv(p)
    if (TESTING) {
      d <- d %>% filter(t < 121)
    }
    return(d)
  } else {
    stop("The input path must be a character string.")
  }
}

load_input_table <- function(input_table, accelerometer_column,
                             gyroscope_column, intermediary_location) {
  input_table_q <- synTableQuery(paste(
    "select * from", input_table, "WHERE", accelerometer_column, "IS NOT NULL",
    "AND", gyroscope_column, "IS NOT NULL",
    {if_else(TESTING, "LIMIT 2", "")}))
  input_table <- input_table_q$asDataFrame() %>%
    as_tibble() %>%
    select(-ROW_ID, -ROW_VERSION)
  previously_computed_measurements <- list.files(intermediary_location) %>%
    purrr::map(~ str_split(., "\\.")[[1]][1]) %>%
    unlist() %>%
    tibble::enframe(name = NULL) %>%
    rename(measurement_id = value)
  input_table <- input_table %>%
      anti_join(previously_computed_measurements, by = "measurement_id")
  if (!is.null(accelerometer_column)) {
    accel_data <- synDownloadTableColumns(input_table_q, accelerometer_column)
    accel_table <- tibble(!!accelerometer_column := names(accel_data),
                          accelerometer = accel_data)
    input_table <- input_table %>%
        left_join(accel_table)
  } else {
    input_table <- input_table %>%
      mutate(accelerometer = NA)
  }
  if (!is.null(gyroscope_column)) {
    gyro_data <- synDownloadTableColumns(input_table_q, gyroscope_column)
    gyro_table <- tibble(!!gyroscope_column := names(gyro_data),
                          gyroscope = gyro_data)
    input_table <- input_table %>%
        left_join(gyro_table)
  } else {
    input_table <- input_table %>%
      mutate(gyroscope = NA)
  }
  return(input_table)
}

map_features <- function(measurement_id, sensor_location, accelerometer,
                         gyroscope, intermediary_location) {
  accel_data <- read_sensor_data(accelerometer)
  gyro_data <- read_sensor_data(gyroscope)
  sampling_rate <- mhealthtools:::get_sampling_rate(accel_data)
  window_length <- as.integer(60*sampling_rate) # one-minute long windows
  tremor_features <- mhealthtools::get_tremor_features(
    accel_data, gyro_data, window_length=window_length, window_overlap=0,
    derived_kinematics=F, detrend=T, frequency_filter = c(1, 25))
  if (is.null(tremor_features$error) && !is.null(tremor_features$extracted_features)) {
    tremor_features <- tremor_features$extracted_features
  } else {
    tremor_features <- tremor_features$error
  }
  tremor_features$measurement_id <- measurement_id
  tremor_features$sensor_location <- sensor_location
  tremor_features <- tremor_features %>%
    select(measurement_id, sensor_location, dplyr::everything())
  if (!is.null(intermediary_location)) {
    readr::write_tsv(tremor_features,
                     file.path(intermediary_location, paste0(measurement_id, ".tsv")))
  }
  return(tremor_features)
}

extract_features <- function(input_table, parallel, intermediary_location) {
  if (parallel) {
    plan(multiprocess)
  }
  if (!has_name(input_table, "sensor_location")) {
    input_table$sensor_location <- NA
  }
  relevant_input_table <- input_table %>%
    select(measurement_id, sensor_location, accelerometer, gyroscope) %>%
    mutate(intermediary_location = intermediary_location)
  features <- furrr::future_pmap_dfr(
    relevant_input_table, map_features, .progress = TRUE)
  return(features)
}

store_features <- function(features, parent) {
  fname <- "tremor_features.tsv"
  write_tsv(features, fname)
  tryCatch({
    f <- synapser::File(fname, parent=parent)
    synStore(f)
    unlink(fname)
  }, error = function(err) {
    stop(paste(conditionMessage(err), "Features not stored to Synapse:", fname))
  })
}

main <- function() {
  synLogin()
  if (TESTING) {
    args <- list()
    args$inputTable <- "syn20824031"
    args$accelerometerColumn <- "smartphone_accelerometer"
    args$gyroscopeColumn <- NULL
    args$cacheDir <- "cache"
    args$parallel <- TRUE
  } else {
    args <- read_args()
  }
  input_table <- load_input_table(input_table = args$inputTable,
                                  accelerometer_column = args$accelerometerColumn,
                                  gyroscope_column = args$gyroscopeColumn,
                                  intermediary_location = args$cacheDir)
  if (nrow(input_table)) {
    features <- extract_features(input_table, args$parallel, args$cacheDir)
  } else {
    stop("All features have already been stored to the cache directory.")
  }
  store_features(features, args$outputParent)
}

main()
