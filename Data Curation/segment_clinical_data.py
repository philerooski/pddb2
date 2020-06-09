import synapseclient as sc
import pandas as pd
import datetime


# Table3 MDS-UPDRS Part III
CIS_PD_UPDRS_P3_TABLE = "syn18435297" # start times
# Table4 Motor Task Timestamps and Scores
CIS_MOTOR_TASK_TIMESTAMPS = "syn18435302" # end times
CIS_SENSOR_DATA = "syn22144319"

# Home-based_validation_export_20181129.csv
REAL_PD_TIMESTAMPS = "syn20769652"
REAL_PD_OFF_UPDRS_START_STOP = ("OFF_UPDRS_start", "OFF_free_living_start")
REAL_PD_ON_UPDRS_START_STOP = ("ON_UPDRS_start", "ON_free_living_start")
HAUSER_DIARY_START_STOP_INTERVALS = [
  ("Time_interval_1", "Time_interval_2"),
  ("Time_interval_2", "Time_interval_3"),
  ("Time_interval_3", "Time_interval_4"),
  ("Time_interval_4", "Time_interval_5"),
  ("Time_interval_5", "Time_interval_6")]
# Synchronization_events_annotations.xls
VIDEO_TO_DEVICE_TIME = "syn20645722" # indexed by pat_id, device
REAL_UPDATED_WATCH_DATA = "syn21614548"
REAL_SMARTPHONE_DATA = "syn20542701"

CIS_TRAINING_MEASUREMENTS = "syn21291578"
REAL_TRAINING_MEASUREMENTS = "syn21292049"


def get_training_subjects(syn, training_measurements):
    f = syn.get(training_measurements)
    df = pd.read_csv(f.path)
    return df.subject_id.unique().astype(str).tolist()


def compute_cis_segments(syn, subject_ids):
    subject_id_query_str = "('" + "','".join(subject_ids) + "')"
    # ParticipantState was only recorded at Visit = 2 Weeks: Time 0/60
    start_times = syn.tableQuery(f"SELECT \"Subject ID\" as subject_id, "
                                 f"Visit as visit, DateTime as start_time "
                                 f"FROM {CIS_PD_UPDRS_P3_TABLE} "
                                 f"WHERE ParticipantState <> '' "
                                 f"AND \"Subject ID\" in {subject_id_query_str}").asDataFrame()
    # The first task performed by this participant after starting
    # their MDS-UPDRS task this visit is the end of the segment
    end_times = syn.tableQuery(f"SELECT SubjID as subject_id, Visit as visit, "
                               f"\"Start Timestamp (UTC)\" as end_time "
                               f"FROM {CIS_MOTOR_TASK_TIMESTAMPS} "
                               f"WHERE SubjID in {subject_id_query_str}").asDataFrame()
    all_times = start_times.merge(
            end_times, on = ["subject_id", "visit"]).dropna(axis=0)
    all_times = (all_times
            .assign(
                start_time = all_times["start_time"].apply(datetime.datetime.fromisoformat),
                end_time = all_times["end_time"].apply(datetime.datetime.fromisoformat)))
    all_times["duration"] = all_times["end_time"] - all_times["start_time"]
    all_times["duration"] = all_times["duration"].apply(datetime.timedelta.total_seconds)
    all_times = all_times.query("duration > 0")
    all_times = all_times.sort_values("duration").groupby(["subject_id", "start_time"]).first()
    all_times = all_times.drop("duration", axis=1)
    all_times = all_times.reset_index(drop=False)
    return all_times


#' This is for the case where our segments have clearly defined start/end times.
#' Since loading in a single sensor file can take up a large amount of memory,
#' we need to load one file at a time, then determine whether any of our segments
#' are contained within the sensor file.
def segment_from_start_to_end(sensor_data, reference):
    segments = []


#' This is for columns Time_interval_X where we are segmenting data
#' centered around a single timepoint
def segment_from_center(sensor_data, reference):
    pass


def compute_real_segments(syn, subject_ids):
    real_pd_timestamps = pd.read_table(syn.get(REAL_PD_TIMESTAMPS).path, sep=";")
    real_pd_timestamps = real_pd_timestamps.dropna(subset = ["date_screening"])
    real_pd_timestamps["date_screening"] = \
            real_pd_timestamps["date_screening"].apply(dmy_to_ymd)
    video_to_device = pd.read_excel(syn.get(VIDEO_TO_DEVICE_TIME).path)
    # Munge on real_pd_timestamps to get segment start/stop (in *video* time)
    # off
    off_timestamps = real_pd_timestamps[
            real_pd_timestamps.OFF_UPDRS_start.notnull() &
            real_pd_timestamps.OFF_free_living_start.notnull()][
                    ["Record Id", "date_screening", "OFF_UPDRS_start", "OFF_free_living_start"]]
    off_timestamps = off_timestamps.rename(
            {"OFF_UPDRS_start": "start_time", "OFF_free_living_start": "end_time"},
            axis=1)
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "start_time")
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "end_time")
    off_timestamps = off_timestamps.dropna()
    off_timestamps = off_timestamps.drop("date_screening", axis=1)
    # on
    on_timestamps = real_pd_timestamps[
            real_pd_timestamps.ON_UPDRS_start.notnull() &
            real_pd_timestamps.ON_free_living_start.notnull()][
                    ["Record Id", "date_screening", "ON_UPDRS_start", "ON_free_living_start"]]
    on_timestamps = on_timestamps.rename(
            {"ON_UPDRS_start": "start_time", "ON_free_living_start": "end_time"},
            axis=1)
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "start_time")
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "end_time")
    on_timestamps = on_timestamps.dropna()
    on_timestamps = on_timestamps.drop("date_screening", axis=1)
    # hauser diary
    hauser_time_cols = ["Time_interval_{}".format(i) for i in range(1,7)]
    hauser_timestamps = real_pd_timestamps[
            ["Record Id", "date_screening"] + hauser_time_cols]
    for c in hauser_time_cols:
        hauser_timestamps = fix_real_pd_datetimes(
                hauser_timestamps, c)
    hauser_timestamps = hauser_timestamps.dropna(
            subset = hauser_time_cols, how="all")
    hauser_timestamps = hauser_timestamps.drop("date_screening", axis=1)
    return off_timestamps, on_timestamps, hauser_timestamps


def fix_real_pd_datetimes(df, col_to_fix):
    # get rid of negative "times" (these look like negative integers)
    df[col_to_fix] = [float("nan") if str(i).startswith("-") else i
                      for i in df[col_to_fix]]
    df[col_to_fix] = [
            datetime.datetime.fromisoformat("{} {}".format(i[0], i[1])).isoformat()
            if pd.notnull(i[1]) else float("nan")
            for i in zip(df["date_screening"], df[col_to_fix])]
    return(df)


def dmy_to_ymd(s):
    s_ = s.split("-")
    s_ = list(map(int, s_))
    s_iso_format = datetime.date(s_[-1], s_[-2], s_[-3]).isoformat()
    return s_iso_format


def list_append_to_query_str(query_str, col, list_of_things):
    if isinstance(list_of_things, list):
        str_of_things = "('" + "','".join(list_of_things) + "')"
    else:
        str_of_things = ""
    if str_of_things:
        query_str = "{} {}".format(
                query_str, "AND {} IN {}".format(col, str_of_things))
    return(query_str)


def download_sensor_data(syn, table, device, subject_ids=None, measurements=None):
    query_str = "SELECT * FROM {} WHERE device = '{}'".format(table, device)
    query_str = list_append_to_query_str(query_str, "subject_id", subject_ids)
    query_str = list_append_to_query_str(query_str, "measurement", measurements)
    print(query_str)
    sensor_data = syn.tableQuery(query_str).asDataFrame()
    sensor_data["path"] = [syn.get(i).path for i in sensor_data.id]
    return sensor_data


def main():
    syn = sc.login()
    cis_training_subjects = get_training_subjects(syn, CIS_TRAINING_MEASUREMENTS)
    real_training_subjects = get_training_subjects(syn, REAL_TRAINING_MEASUREMENTS)
    cis_smartphone_data = download_sensor_data(
            syn,
            table = CIS_SENSOR_DATA,
            device = "smartphone",
            subject_ids = cis_training_subjects)
    real_smartwatch_data = download_sensor_data(
            syn,
            table = REAL_UPDATED_WATCH_DATA,
            device = "Smartwatch",
            subject_ids = real_training_subjects,
            measurements = ["accelerometer", "gyroscope"])
    real_smartphone_data = download_sensor_data(
            syn,
            table = REAL_SMARTPHONE_DATA,
            device = "Smartphone",
            subject_ids = real_training_subjects,
            measurements = ["accelerometer"])
    cis_segments = compute_cis_segments(syn, cis_training_subjects)
    real_off_segments, real_on_segments, real_hauser_intervals = \
            compute_real_segments(syn, real_training_subjects)


main()

