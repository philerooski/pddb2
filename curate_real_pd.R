library(synapser)
library(tidyverse)
library(furrr)

TESTING = FALSE
DIARY = "syn20769648"
SENSOR_START_TIMES <- "syn20712822"
SENSOR_DATA <- "syn20542701"
OUTPUT_PROJECT <- if (TESTING) "syn11611056" else "syn18407520"

fetch_diary <- function() {
  diary_raw <- read_delim(synGet(DIARY)$path, delim=";") %>%
    rename(diary_subject_id = `Record Id`,
           reported_timestamp = `Report Creation Date`,
           report_name = `Report Name Custom`,
           report_parent = `Report Parent`,
           diary_date = Date_detailed_diary) %>%
    select(-X198, -report_name, -report_parent) %>%
    gather("measurement", "value",
           `detailed_diary_pd_6:00_6:30_Medication_state`:`detailed_diary_pd_5:30_6:00_Main_activities`) %>%
    mutate(measurement = str_replace(measurement, "detailed_diary_pd_", ""),
           reported_timestamp = lubridate::dmy_hms(
             reported_timestamp, tz = "Europe/Amsterdam"),
           diary_date = lubridate::dmy(diary_date, tz="Europe/Amsterdam")) %>%
    separate(measurement, c("start_time", "end_time", "measurement"),
             sep = "_", extra = "merge") %>%
    mutate(start_time = hms::as_hms(paste0(start_time, ":00")),
           end_time = hms::as_hms(paste0(end_time, ":00")),
           diary_start_timestamp = diary_date + start_time,
           diary_end_timestamp = diary_date + end_time) %>%
    filter(!is.na(diary_date),
           diary_date < lubridate::today())
    diary_raw$measurement <- dplyr::recode(
      diary_raw$measurement,
      Medication_state = "medication_state",
      Slowness_walking = "slowness_walking",
      Tremor = "tremor",
      Main_activities = "main_activities")
    diary_raw <- diary_raw %>%
      pivot_wider(names_from = measurement, values_from = value) %>%
      mutate(measurement_id = unlist(lapply(1:nrow(.), uuid::UUIDgenerate))) %>%
      select(measurement_id, diary_subject_id, reported_timestamp, diary_start_timestamp,
           diary_end_timestamp, medication_state, slowness_walking, tremor)
  return(diary_raw)
}

fetch_start_times <- function() {
  sensor_start_times <- read_csv(synGet(SENSOR_START_TIMES)$path) %>%
    filter(Data_Type %in% c("accel.bin", "gyro.bin", "HopkinsPD_stream_accel_*.raw")) %>%
    mutate(first_time = lubridate::as_datetime(first_time, tz = "Europe/Amsterdam"),
           last_time = lubridate::as_datetime(last_time, tz = "Europe/Amsterdam")) %>%
    filter(!(is.na(first_time) & is.na(last_time)),
           lubridate::year(first_time) >= 2017) %>%
    rename(subject_id = Ind.ID,
           device = Device,
           measurement = Data_Type) %>%
    select(subject_id, device, measurement, first_time, last_time)
  sensor_start_times$measurement <- recode(sensor_start_times$measurement,
            gyro.bin = "gyroscope", accel.bin = "accelerometer",
            `HopkinsPD_stream_accel_*.raw` = "accelerometer")
  return(sensor_start_times)
}

fetch_sensor_data <- function() {
  sensor_data_q <- synTableQuery(paste(
    "SELECT * FROM", SENSOR_DATA, "WHERE",
    "(device = 'Smartwatch' OR device = 'Smartphone')",
    "AND",
    "(measurement = 'gyroscope' OR measurement = 'accelerometer')",
    {if (TESTING) "LIMIT 2 OFFSET 2" else ""}))
  sensor_data <- as_tibble(sensor_data_q$asDataFrame())
  # Fortunately, everything with a matching md5 has matching identifying metadata
  # except in the case where the data file is empty except for column names
  duplicated_data <- sensor_data %>%
    filter(duplicated(md5))
  sensor_data <- sensor_data %>%
    anti_join(duplicated_data, by = "ROW_ID") %>%
    select(id, subject_id, context, device, measurement)
  sensor_data$path <- unlist(purrr::map(sensor_data$id, ~ synGet(.)$path))
  return(sensor_data)
}

slice_sensor_data <- function(sensor_data, diary) {
  sliced_data <- furrr::future_pmap_dfr(sensor_data, function(subject_id, context, device,
                                                    measurement, path, first_time,
                                                    last_time) {
    relevant_slices <- tryCatch({
      sensor_data_df <- read_csv(path) %>%
        filter(!duplicated(time)) %>%
        mutate(actual_time = first_time + lubridate::seconds(time)) %>%
        arrange(time)
      names(sensor_data_df) <- c("t", "x", "y", "z", "actual_time")
      last_time <- sensor_data_df$actual_time[[nrow(sensor_data_df)]]
      relevant_diary <- diary %>%
        distinct(measurement_id, diary_subject_id, diary_start_timestamp, diary_end_timestamp) %>%
        arrange(diary_start_timestamp) %>%
        filter(diary_start_timestamp >= first_time - lubridate::minutes(5),
               diary_end_timestamp <= last_time + lubridate::minutes(5),
               diary_subject_id == subject_id)
      if (nrow(relevant_diary)) {
        slices <- purrr::pmap(relevant_diary, function(measurement_id, diary_subject_id,
                                                       diary_start_timestamp, diary_end_timestamp) {
          slice <- sensor_data_df %>%
            filter(actual_time >= diary_start_timestamp + lubridate::minutes(5),
                   actual_time <= diary_end_timestamp - lubridate::minutes(5))
            if (nrow(slice)) {
              slice <- slice %>%
                mutate(measurement_id = measurement_id,
                       t = round(t - t[[1]], 3)) %>%
                select(measurement_id, dplyr::everything(), -actual_time)
            }
            return(slice)
        })
        relevant_slices <- tibble(
          subject_id = subject_id,
          context = context,
          device = device,
          measurement = measurement,
          data = slices) %>%
          filter(purrr::map(data, nrow) > 0) %>%
          mutate(measurement_id = unlist(purrr::map(data, ~ .$measurement_id[[1]])),
                 data = purrr::map(data, ~ select(., t, x, y, z))) %>%
          select(measurement_id, subject_id, context, device, measurement, data)
        return(relevant_slices)
      } else {
        return(tibble())
      }
    }, error = function(e) {
      relevant_slices <- tibble(
        subject_id = subject_id,
        context = context,
        device = device,
        measurement = measurement,
        data = NA,
        error = e$message)
      return(relevant_slices)
    })
    return(relevant_slices)
  })
  return(sliced_data)
}

replace_df_with_filehandles <- function(list_of_df) {
  file_handles <- purrr::map(list_of_df, function(df) {
    fname <- paste0(tempfile(), ".csv")
    write_csv(df, fname)
    fh <- synUploadSynapseManagedFileHandle(fname, mimetype="text/csv")
    unlink(fname)
    return(fh$id)
  })
  file_handles <- unlist(file_handles)
  return(file_handles)
}

store_sliced_data_and_diary <- function(sliced_data, diary, parent) {
  context <- sliced_data %>%
    distinct(measurement_id, context)
  diary <- diary %>%
    left_join(context, by = "measurement_id") %>%
    select(measurement_id, subject_id = diary_subject_id, context, dplyr::everything())
  data_df <- sliced_data %>%
    select(measurement_id, subject_id, device, measurement, data)
  data_df <- data_df %>%
    sample_frac(1) %>%
    mutate(data_file_handle_id = replace_df_with_filehandles(data)) %>%
    select(-data) %>%
    arrange(subject_id, device, measurement, measurement_id)
  data_fname <- "real_pd_sensor_data.csv"
  write_csv(data_df, data_fname)
  data_cols <- list(
    Column(name = "measurement_id", columnType = "STRING", maximumSize="36"),
    Column(name = "subject_id", columnType = "STRING", maximumSize="36"),
    Column(name = "device", columnType = "STRING", maximumSize="36"),
    Column(name = "measurement", columnType = "STRING", maximumSize="36"),
    Column(name = "data_file_handle_id", columnType = "FILEHANDLEID"))
  data_schema <- Schema(name = "REAL PD Accelerometer and Gyroscope Data", parent = parent,
                        columns = data_cols)
  data_table <- Table(data_schema, data_df)
  synStore(data_table)
  diary_fname <- "real_pd_diary.csv"
  write_csv(diary, diary_fname)
  diary_cols <- list(
    Column(name = "measurement_id", columnType = "STRING", maximumSize="36"),
    Column(name = "subject_id", columnType = "STRING", maximumSize="36"),
    Column(name = "context", columnType = "STRING", maximumSize="36"),
    Column(name = "reported_timestamp", columnType = "DATE"),
    Column(name = "diary_start_timestamp", columnType = "DATE"),
    Column(name = "diary_end_timestamp", columnType = "DATE"),
    Column(name = "medication_state", columnType = "INTEGER"),
    Column(name = "slowness_walking", columnType = "INTEGER"),
    Column(name = "tremor", columnType = "INTEGER"))
  diary_schema <- Schema(name = "REAL PD Self-Reported Scores", parent = parent,
                        columns = diary_cols)
  diary_table <- Table(diary_schema, diary)
  synStore(diary_table)
}

main <- function() {
  synLogin()
  diary <- fetch_diary()
  start_times <- fetch_start_times()
  sensor_data <- fetch_sensor_data() %>%
    inner_join(start_times, by = c("subject_id", "device", "measurement")) %>%
    select(-id)
  plan(multiprocess, workers = 3)
  sliced_data <- slice_sensor_data(sensor_data, diary)
  store_sliced_data_and_diary(sliced_data, diary, parent = OUTPUT_PROJECT)
}

main()