library(synapser)
library(tidyverse)

DIARY = "syn20769648"
SENSOR_START_TIMES <- "syn20712822"
SENSOR_DATA <- "syn20542701"

fetch_diary <- function() {
  diary_raw <- read_delim(synGet(DIARY)$path, delim=";") %>%
    rename(subject_id = `Record Id`,
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
           end_time = hms::as_hms(paste0(end_time, ":00"))) %>%
    filter(!is.na(diary_date),
           diary_date < lubridate::today())
  return(diary_raw)
}

fetch_start_times <- function() {
  sensor_start_times <- read_csv(synGet(SENSOR_START_TIMES)$path) %>%
    filter(Data_Type %in% c("accel.bin", "gyro.bin", "HopkinsPD_stream_accel_*.raw")) %>%
    mutate(first_time = lubridate::as_datetime(first_time, tz = "Europe/Amsterdam")) %>%
    rename(subject_id = Ind.ID)
  sensor_start_times$Data_Type <- recode(sensor_start_times$Data_Type,
            gyro.bin = "gyroscope", accel.bin = "accelerometer",
            `HopkinsPD_stream_accel_*.raw` = "accelerometer")
  return(sensor_start_times)
}

fetch_sensor_data <- function() {
  sensor_data_q <- synTableQuery(paste(
    "SELECT * FROM", SENSOR_DATA, "WHERE",
    "(device = 'Smartwatch' OR device = 'Smartphone')",
    "AND",
    "(measurement = 'gyroscope' OR measurement = 'accelerometer')"))
  sensor_data <- sensor_data_q$asDataFrame() %>%
    as_tibble() %>%
    select(id, subject_id, context, device, measurement) %>%
    mutate(path = synGet(id)$path)
  return(sensor_data)
}

main <- function() {
  synLogin()
  diary <- fetch_diary()
  start_times <- fetch_start_times()
  sensor_data <- fetch_sensor_data()
}

main()