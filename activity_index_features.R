library(synapser)
library(tidyverse)
library(optparse)
library(furrr)

SIGMA_0 <- 0.0025

read_args <- function() {
  parser <- optparse::OptionParser(
    description = paste("Compute activityIndex features on a given",
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
  parse_args(parser)
}

read_sensor_data <- function(p) {
  if (is.na(p) || is.null(p)) {
    return(NULL)
  } else if (is.character(p)) {
    d <- read_csv(p) %>%
      rename(t = Timestamp, x = X, y = Y, z = Z)
    return(d)
  } else {
    stop("The input path must be a character string.")
  }
}

load_input_table <- function(input_table, accelerometer_column, gyroscope_column) {
  input_table_q <- synTableQuery(paste("select * from", input_table))
  input_table <- input_table_q$asDataFrame() %>%
    as_tibble() %>%
    select(-ROW_ID, -ROW_VERSION)
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

activity_index <- function(df, sigma0, epoch, sampling_rate = NULL) {
  if (is.null(sampling_rate)) {
    sampling_rate <- ceiling(mhealthtools:::get_sampling_rate(df))
  }
  surplus_rows <- nrow(df) %% sampling_rate
  df <- df[1:(nrow(df)-surplus_rows),]
  df <- df %>%
    rename(Index = t, X = x, Y = y, Z = z)
  activity_index <- ActivityIndex::computeActivityIndex(
    df, sigma0 = sigma0, epoch = epoch, hertz = sampling_rate)
  return(activity_index)
}

map_features <- function(measurement_id, sensor_location, accelerometer, gyroscope) {
  accel_data <- read_sensor_data(accelerometer)
  gyro_data <- read_sensor_data(gyroscope)
  sampling_rate <- ceiling(mhealthtools:::get_sampling_rate(accel_data))
  activity_index_features <- activity_index(
    df = accel_data, sigma0 = SIGMA_0, epoch = 60, sampling_rate = sampling_rate)
  activity_index_features$measurement_id <- measurement_id
  activity_index_features$sensor_location <- sensor_location
  activity_index_features$minute <- 1:nrow(activity_index_features)
  activity_index_features <- activity_index_features %>%
    select(measurement_id, sensor_location, t = RecordNo, minute, AI)
  return(activity_index_features)
}

extract_features <- function(input_table, parallel) {
  if (parallel) {
    plan(multiprocess)
  }
  if (!has_name(input_table, "sensor_location")) {
    input_table$sensor_location <- NA
  }
  relevant_input_table <- input_table %>%
    select(measurement_id, sensor_location, accelerometer, gyroscope)
  features <- furrr::future_pmap_dfr(
    relevant_input_table, map_features, .progress = TRUE)
  return(features)
}

store_features <- function(features, parent) {
  fname <- "activity_index_features.tsv"
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
  args <- read_args()
  input_table <- load_input_table(input_table = args$inputTable,
                                  accelerometer_column = args$accelerometerColumn,
                                  gyroscope_column = args$gyroscopeColumn)
  features <- extract_features(input_table, args$parallel)
  store_features(features, args$outputParent)
}

main()