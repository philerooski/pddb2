import synapseclient as sc
import pandas as pd
import datetime


# Table3 MDS-UPDRS Part III
CIS_PD_UPDRS_P3_TABLE = "syn18435297" # start times
# Table4 Motor Task Timestamps and Scores
CIS_MOTOR_TASK_TIMESTAMPS = "syn18435302" # end times

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

CIS_TRAINING_MEASUREMENTS = "syn21291578"
REAL_TRAINING_MEASUREMENTS = "syn21292049"

def get_training_subjects(syn, training_measurements):
    f = syn.get(training_measurements)
    df = pd.read_csv(f.path)
    return df.subject_id.unique().astype(str)


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
    all_times = all_times.reset_index(drop=False)
    return all_times


def segment_from_reference(sensor_data, reference):
    pass


def compute_real_segments(syn, subject_ids):
    real_pd_timestamps = pd.read_table(syn.get(REAL_PD_TIMESTAMPS).path, sep=";")
    video_to_device = pd.read_excel(syn.get(VIDEO_TO_DEVICE_TIME).path)
    # Munge on real_pd_timestamps to get segment start/stop (in *video* time)
    # off
    real_pd_off_timestamps = real_pd_timestamps[
            real_pd_timestamps.OFF_UPDRS_start.notnull() &
            real_pd_timestamps.OFF_free_living_start.notnull()][
                    ["Record Id", "date_screening", "OFF_UPDRS_start", "OFF_free_living_start"]]
    real_pd_off_timestamps = real_pd_off_timestamps.rename(
            {"OFF_UPDRS_start": "start_time", "OFF_free_living_start": "end_time"},
            axis=1)
    real_pd_off_timestamps = fix_real_pd_datetimes(real_pd_off_timestamps)
    real_pd_off_timestamps = real_pd_off_timestamps.drop("date_screening", axis=1)
    # on
    real_pd_on_timestamps = real_pd_timestamps[
            real_pd_timestamps.ON_UPDRS_start.notnull() &
            real_pd_timestamps.ON_free_living_start.notnull()][
                    ["Record Id", "date_screening", "ON_UPDRS_start", "ON_free_living_start"]]
    real_pd_on_timestamps = real_pd_on_timestamps.rename(
            {"ON_UPDRS_start": "start_time", "ON_free_living_start": "end_time"},
            axis=1)
    real_pd_on_timestamps = fix_real_pd_datetimes(real_pd_on_timestamps)
    real_pd_on_timestamps = real_pd_on_timestamps.drop("date_screening", axis=1)
    return real_pd_off_timestamps, real_pd_on_timestamps


def fix_real_pd_datetimes(df):
    # get rid of negative "times" (these look like negative integers)
    df = df[~df.start_time.str.startswith("-") & ~df.end_time.str.startswith("-")]
    df["date_screening"] = df.date_screening.apply(dmy_to_ymd)
    df["start_time"] = [
            datetime.datetime.fromisoformat("{} {}".format(i[0], i[1])).isoformat()
            for i in zip(df["date_screening"], df["start_time"])]
    df["end_time"] = [
            datetime.datetime.fromisoformat("{} {}".format(i[0], i[1])).isoformat()
            for i in zip(df["date_screening"], df["end_time"])]
    return(df)


def dmy_to_ymd(s):
    s_ = s.split("-")
    s_ = list(map(int, s_))
    s_iso_format = datetime.date(s_[-1], s_[-2], s_[-3]).isoformat()
    return s_iso_format


def download_sensor_data(syn):
    # TODO download all relevent data (check if *all* sensor data is relevent)
    pass

def main():
    syn = sc.login()
    cis_training_subjects = get_training_subjects(syn, CIS_TRAINING_MEASUREMENTS)
    real_training_subjects = get_training_subjects(syn, REAL_TRAINING_MEASUREMENTS)


main()

