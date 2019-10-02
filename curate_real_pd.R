library(synapser)
library(tidyverse)
library(future)
library(furrr)

TESTING <- FALSE
PARALLEL_WORKERS <- 2
CACHE_DIR <- if (TESTING) "cache" else "/root/cache"
DIARY <- "syn20769648"
DIARY_CURATED <- "syn20822276"
SENSOR_START_TIMES <- "syn20712822"
SENSOR_DATA <- "syn20542701"
SENSOR_DATA_REFERENCE <- "syn20820314"
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

fetch_curated_diary <- function() {
  q <- synTableQuery(paste("select * from", DIARY_CURATED))
  diary <- as_tibble(q$asDataFrame()) %>%
    rename(diary_subject_id = subject_id) %>%
    select(measurement_id, diary_subject_id, reported_timestamp, diary_start_timestamp,
           diary_end_timestamp, medication_state, slowness_walking, tremor)
  return(diary)
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
    "AND",
    "context = 'follow_up'",
    {if (TESTING) "LIMIT 2" else ""}))
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

get_sensor_data_reference <- function() {
  sensor_data_ref <- as_tibble(synTableQuery(
    paste("SELECT * FROM", SENSOR_DATA_REFERENCE))$asDataFrame()) %>%
    select(subject_id, context, device, measurement, file_identifier) %>%
    mutate(cache_dir = as.character(fs::path(CACHE_DIR, file_identifier)))
  return(sensor_data_ref)
}

get_already_processed_sensor_data <- function(sensor_data, sensor_data_ref) {
  already_processed_sensor_data <- left_join(sensor_data, sensor_data_ref) %>%
    filter(dir.exists(cache_dir)) %>%
    select(subject_id, context, device, measurement, file_identifier, cache_dir)
  if (nrow(already_processed_sensor_data)) {
    sensor_data_processed <- already_processed_sensor_data %>%
      purrr::pmap_dfr(function(subject_id, context, device, measurement, file_identifier, cache_dir) {
        sliced_files <- dir(cache_dir)
        if (length(sliced_files)) {
          measurement_ids <- unlist(
            purrr::map(sliced_files, ~ stringr::str_split(., "\\.")[[1]][[1]]))
          paths <- unlist(
            purrr::map(sliced_files, ~ as.character(fs::path(cache_dir, .))))
          relevant_slices <- tibble(
            measurement_id = measurement_ids,
            subject_id = subject_id,
            context = context,
            device = device,
            measurement = measurement,
            file_identifier = file_identifier,
            cache_path = paths)
        } else {
          relevant_slices <- tibble(
            measurement_id = NA,
            subject_id = NA,
            context = NA,
            device = NA,
            measurement = NA,
            file_identifier = file_identifier,
            cache_path = NA)
        }
        return(relevant_slices)
      })
  } else {
    sensor_data_processed <- tibble(
      measurement_id = character(),
      subject_id = character(),
      context = character(),
      device = character(),
      measurement = character(),
      file_identifier = character(),
      cache_path = character())
  }
  return(sensor_data_processed)
}

slice_sensor_data <- function(sensor_data, diary) {
  sliced_data <- furrr::future_pmap_dfr(sensor_data, function(subject_id, context, device,
                                                    measurement, first_time, last_time,
                                                    path, cache_dir) {
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
          mutate(measurement_id = unlist(purrr::map(data, ~ .$measurement_id[[1]])))
        paths <- unlist(purrr::map(relevant_slices$measurement_id, function(measurement_id) {
                   fname <- as.character(
                     fs::path(cache_dir, paste0(measurement_id, ".csv")))
                   return(fname)
         }))
        relevant_slices$cache_path <- paths
        dir.create(cache_dir)
        purrr::map2(relevant_slices$data, relevant_slices$cache_path, function(df, path) {
           df <- df %>%
             select(t, x, y, z) %>%
             write_csv(path)
        })
        relevant_slices <- relevant_slices %>%
          select(measurement_id, subject_id, context, device, measurement, cache_path)
        return(relevant_slices)
      } else {
        dir.create(as.character(cache_dir))
        return(tibble())
      }
    }, error = function(e) {
      relevant_slices <- tibble(
        subject_id = subject_id,
        context = context,
        device = device,
        measurement = measurement,
        path = NA,
        error = e$message)
      return(relevant_slices)
    })
    return(relevant_slices)
  }, .options = future_options(scheduling = Inf))
  return(sliced_data)
}

replace_paths_with_filehandles <- function(list_of_paths) {
  file_handles <- purrr::map(list_of_paths, function(p) {
    if (!is.na(p)) {
      fh <- synUploadSynapseManagedFileHandle(p, mimetype="text/csv")
      return(fh$id)
    } else {
      return(NA)
    }
  })
  file_handles <- unlist(file_handles)
  return(file_handles)
}

store_sliced_data_and_diary <- function(sliced_data, diary, parent) {
  diary <- diary %>%
    select(measurement_id, subject_id = diary_subject_id, dplyr::everything())
  data_df <- sliced_data %>%
    select(measurement_id, subject_id, context, device, measurement, cache_path)
  data_df <- data_df %>%
    sample_frac(1) %>%
    mutate(data_file_handle_id = replace_paths_with_filehandles(cache_path)) %>%
    select(-cache_path) %>%
    arrange(subject_id, device, measurement, measurement_id)
  data_fname <- "real_pd_sensor_data.csv"
  write_csv(data_df, data_fname)
  data_cols <- list(
    Column(name = "measurement_id", columnType = "STRING", maximumSize="36"),
    Column(name = "subject_id", columnType = "STRING", maximumSize="36"),
    Column(name = "context", columnType = "STRING", maximumSize="12"),
    Column(name = "device", columnType = "STRING", maximumSize="36"),
    Column(name = "measurement", columnType = "STRING", maximumSize="36"),
    Column(name = "data_file_handle_id", columnType = "FILEHANDLEID"))
  data_schema <- Schema(name = "REAL PD Accelerometer and Gyroscope Data", parent = parent,
                        columns = data_cols)
  data_table <- Table(data_schema, data_df)
  synStore(data_table)

  ### Uncomment if curating diary from scratch `diary <- fetch_diary()`
  #diary_fname <- "real_pd_diary.csv"
  #write_csv(diary, diary_fname)
  #diary_cols <- list(
  #  Column(name = "measurement_id", columnType = "STRING", maximumSize="36"),
  #  Column(name = "subject_id", columnType = "STRING", maximumSize="36"),
  #  Column(name = "context", columnType = "STRING", maximumSize="36"),
  #  Column(name = "reported_timestamp", columnType = "DATE"),
  #  Column(name = "diary_start_timestamp", columnType = "DATE"),
  #  Column(name = "diary_end_timestamp", columnType = "DATE"),
  #  Column(name = "medication_state", columnType = "INTEGER"),
  #  Column(name = "slowness_walking", columnType = "INTEGER"),
  #  Column(name = "tremor", columnType = "INTEGER"))
  #diary_schema <- Schema(name = "REAL PD Self-Reported Scores", parent = parent,
  #                      columns = diary_cols)
  #diary_table <- Table(diary_schema, diary)
  #synStore(diary_table)
}

main <- function() {
  if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR)
  synLogin()
  #diary <- fetch_diary()
  diary <- fetch_curated_diary()
  start_times <- fetch_start_times()
  sensor_data <- fetch_sensor_data() %>%
    inner_join(start_times, by = c("subject_id", "device", "measurement")) %>%
    select(-id)
  sensor_data_ref <- get_sensor_data_reference()
  already_processed_sensor_data <- get_already_processed_sensor_data(
    sensor_data, sensor_data_ref)
  sensor_data <- sensor_data %>%
    left_join(sensor_data_ref) %>%
    anti_join(already_processed_sensor_data, by = "file_identifier") %>%
    select(subject_id, context, device, measurement, first_time, last_time, path, cache_dir)
  already_processed_sensor_data <- already_processed_sensor_data %>%
    filter(!is.na(cache_path)) %>%
    select(-file_identifier)
  plan(multiprocess, workers = PARALLEL_WORKERS)
  if (nrow(sensor_data)) { # if there are no rows, we have already sliced up all files
    sliced_data <- slice_sensor_data(sensor_data, diary)
  } else {
    sliced_data <- tibble(
      measurement_id = character(),
      subject_id = character(),
      context = character(),
      device = character(),
      measurement = character(),
      file_identifier = character(),
      cache_path = character())
  }
  sliced_data <- bind_rows(sliced_data, already_processed_sensor_data)
  store_sliced_data_and_diary(sliced_data, diary, parent = OUTPUT_PROJECT)
}

main()