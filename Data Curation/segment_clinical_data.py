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
    return all_times


def main():
    syn = sc.login()
    cis_training_subjects = get_training_subjects(syn, CIS_TRAINING_MEASUREMENTS)
    real_training_subjects = get_training_subjects(syn, REAL_TRAINING_MEASUREMENTS)


main()

