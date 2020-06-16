import synapseclient as sc
import pandas as pd
import datetime
import uuid


# CIS-PD
# Table3 MDS-UPDRS Part III
CIS_PD_UPDRS_P3_TABLE = "syn18435297" # start times
# Table4 Motor Task Timestamps and Scores
CIS_MOTOR_TASK_TIMESTAMPS = "syn18435302" # end times
CIS_SENSOR_DATA = "syn22144319"

# REAL-PD
# Home-based_validation_export_20181129.csv
REAL_PD_TIMESTAMPS = "syn20769652"
REAL_START_TIMES = "syn22151606"
REAL_PD_OFF_UPDRS_START_STOP = ("OFF_UPDRS_start", "OFF_free_living_start")
REAL_PD_ON_UPDRS_START_STOP = ("ON_UPDRS_start", "ON_free_living_start")
HAUSER_DIARY_START_STOP_INTERVALS = [
  ("Time_interval_1", "Time_interval_2"),
  ("Time_interval_2", "Time_interval_3"),
  ("Time_interval_3", "Time_interval_4"),
  ("Time_interval_4", "Time_interval_5"),
  ("Time_interval_5", "Time_interval_6")]
# Synchronization_events_annotations.xls
VIDEO_TO_DEVICE_TIME = "syn20645722" # indexed by pat_id (subject_id), device
REAL_UPDATED_WATCH_DATA = "syn21614548"
REAL_SMARTPHONE_DATA = "syn20542701"

# MISC
CIS_TRAINING_LABELS = "syn21291578"
REAL_TRAINING_LABELS = "syn21292049"
OUTPUT_PROJECT = "syn22152015"
TESTING = True


def get_training_subjects(syn, training_measurements):
    f = syn.get(training_measurements)
    df = pd.read_csv(f.path)
    return df.subject_id.unique().astype(str).tolist()


def get_real_start_times(syn):
    sensor_start_times = pd.read_csv(syn.get(REAL_START_TIMES).path)
    sensor_start_times = sensor_start_times.query(
            "Data_Type == 'accel.bin' or Data_Type == 'gyro.bin' or "
            "Data_Type == 'HopkinsPD_stream_accel_*.raw'")
    sensor_start_times = sensor_start_times[
            pd.notnull(sensor_start_times.first_time) &
            pd.notnull(sensor_start_times.last_time)]
    sensor_start_times.loc[:,"first_time"] = sensor_start_times.first_time.apply(
            datetime.datetime.fromtimestamp)
    sensor_start_times.loc[:,"last_time"] = sensor_start_times.last_time.apply(
            datetime.datetime.fromtimestamp)
    # Why is this in the original curate_real_pd.R code? All DT values are >= 2017
    sensor_start_times = sensor_start_times[
            [dt.year >= 2017 for dt in sensor_start_times.first_time]]
    sensor_start_times = sensor_start_times.rename({
        "Ind.ID": "subject_id",
        "Device": "device",
        "Data_Type": "measurement",
        "first_time": "device_start_time",
        "last_time": "device_end_time"},
        axis=1)
    sensor_start_times["measurement"] = sensor_start_times.measurement.replace({
        "gyro.bin": "gyroscope",
        "accel.bin": "accelerometer",
        "HopkinsPD_stream_accel_*.raw": "accelerometer"})
    sensor_start_times = sensor_start_times[
            ["subject_id", "device", "measurement",
             "device_start_time", "device_end_time"]]
    return(sensor_start_times)


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
    all_times["segment_id"] = [str(uuid.uuid4()) for i in range(len(all_times))]
    all_times = all_times.set_index(["subject_id", "visit", "segment_id"])
    return all_times


def get_video_to_device_time_reference(syn):
    # There are different video<->device time conversions that need to be
    # applied at the subject_id/device level.
    video_to_device = pd.read_excel(syn.get(VIDEO_TO_DEVICE_TIME).path)
    video_to_device = video_to_device.rename({"pat_id": "subject_id"}, axis=1)
    video_to_device = video_to_device.query(
            "device == 'Smartphone' or device == 'Smartwatch'")
    video_to_device = video_to_device[
            video_to_device.v1.notnull() & video_to_device.t1.notnull()]
    video_to_device["offset"] = [pd.DateOffset(seconds=t-v)
            for t,v in zip(video_to_device.t1, video_to_device.v1)]
    video_to_device = video_to_device[["subject_id", "device", "offset"]]
    return(video_to_device)


#' Handles the special rules that apply to CIS-PD segments
def segment_cis_pd(sensor_data, segment_timestamps):
    cis_segments = segment_from_start_to_end(
            sensor_data = sensor_data,
            reference = segment_timestamps,
            timestamp_col = "Timestamp")
    return(cis_segments)


#' Handles the special rules that apply to REAL-PD segments
def segment_real_pd(smartphone_data, smartwatch_data,
        on_off_segment_timestamps, hauser_interval_timestamps, real_timestamps):
    on_off_smartphone_segments = segment_from_start_to_end(
            sensor_data = smartphone_data,
            reference = on_off_segment_timestamps,
            timestamp_col = "time",
            real_timestamps = real_timestamps)
    on_off_smartwatch_segments = segment_from_start_to_end(
            sensor_data = smartwatch_data,
            reference = on_off_segment_timestamps,
            timestamp_col = "time",
            real_timestamps = real_timestamps)
    hauser_smartphone_segments = segment_from_start_to_end(
            sensor_data = smartphone_data,
            reference = hauser_interval_timestamps,
            timestamp_col = "time",
            real_timestamps = real_timestamps)
    hauser_smartwatch_segments = segment_from_start_to_end(
            sensor_data = smartwatch_data,
            reference = hauser_interval_timestamps,
            timestamp_col = "time",
            real_timestamps = real_timestamps)
    on_off_segments = on_off_smartwatch_segments.append(
            other = on_off_smartphone_segments,
            ignore_index = False)
    hauser_segments = hauser_smartphone_segments.append(
            other = hauser_smartwatch_segments,
            ignore_index = False)
    return on_off_segments, hauser_segments


def align_and_load_sensor_data(path, timestamp_col, subject_id = None,
        device = None, measurement = None, real_timestamps = None):
    if real_timestamps is not None: # REAL-PD
        start_time_df = real_timestamps.query(
                "subject_id == @subject_id and "
                "device == @device and "
                "measurement == @measurement")
        if start_time_df.shape[0] == 1:
            start_time = start_time_df["video_start_time"].iloc[0]
            sensor_measurement = pd.read_csv(path)
            sensor_measurement[timestamp_col] = \
                sensor_measurement[timestamp_col].apply(
                        lambda s : start_time + pd.DateOffset(seconds=int(s)))
            timestamp_col = "t"
            sensor_measurement.columns = [timestamp_col, "x", "y", "z"]
        else: # did not find sensor start time and/or offset
            return pd.DataFrame()
    else: # CIS-PD
        sensor_measurement = pd.read_csv(path)
        sensor_measurement = sensor_measurement.drop("SubjID", axis=1)
        sensor_measurement[timestamp_col] = pd.to_datetime(
                sensor_measurement[timestamp_col])
    sensor_measurement = sensor_measurement.set_index(timestamp_col)
    sensor_measurement = sensor_measurement.sort_index()
    return sensor_measurement

"""
This is for the case where our segments have clearly defined start/end times.
Since loading in a single sensor file can take up a large amount of memory,
we need to load one file at a time, then determine whether any of our segments
are contained within the sensor file.

Args:
sensor_data     -- a dataframe like one returned from `download_sensor_data`,
                   with (at least) columns subject_id, device, measurement, and path
reference       -- a dataframe with an index containing at least the `subject_id`
                   and two datetime columns: start_time and end_time
timestamp_col   -- the name of the column *in a sensor data file* where
                   the time is recorded. E.g., "Timestamp" for CIS-PD sensor data.
real_timestamps -- For REAL-PD, timestamp information must be reinjected into
                   the sensor data. This is a dataframe indexed by subject_id,
                   device, and measurement, like that returned by
                   `get_real_start_times`. This dataframe also has columns
                   `video_start_time` and `video_end_time` which contain
                   timestamps relative to video time. I.e., the same time
                   system we are using for our segments in the `reference`
                   parameter.
"""
def segment_from_start_to_end(sensor_data, reference, timestamp_col,
                              real_timestamps = None):
    indices = []
    segments = []
    for i,r in sensor_data.iterrows():
        subject_id, device, measurement, path = \
                r["subject_id"], r["device"], r["measurement"], r["path"]
        if real_timestamps is not None: # REAL-PD
            sensor_measurement = align_and_load_sensor_data(
                    path = path,
                    timestamp_col = timestamp_col,
                    subject_id = subject_id,
                    device = device,
                    measurement = measurement,
                    real_timestamps = real_timestamps)
            if sensor_measurement.shape[0] == 0:
                continue
        else: # CIS-PD
            sensor_measurement = align_and_load_sensor_data(
                    path = path,
                    timestamp_col = timestamp_col)
        relevant_segments = reference.query("subject_id == @subject_id")
        for index, segment in relevant_segments.iterrows():
            start_time, end_time = segment["start_time"], segment["end_time"]
            segment_data = sensor_measurement.loc[start_time:end_time]
            if segment_data.shape[0]:
                segment_data = segment_data.reset_index(drop=False)
                index = index + (device, measurement) # for REAL-PD smartwatch
                indices.append(index)
                segments.append(segment_data)
    segment_index = pd.MultiIndex.from_tuples(
            indices,
            names=["subject_id", "visit", "segment_id","device", "measurement"])
    segment_df = pd.DataFrame({"segments": segments}, index=segment_index)
    return segment_df


#' This is for columns Time_interval_X where we are segmenting data
#' centered around a single timepoint
#' sensor_data is a dataframe like one returned from `download_sensor_data`,
#' with (at least) columns subject_id and path
#' `reference` is a dataframe with an index containing at least the `subject_id`
#' and two datetime columns: start_time and end_time
#' `timestamp_col` is the name of the column *in a sensor data file* where
#' the time is recorded. E.g., "Timestamp" for CIS-PD sensor data.
def segment_from_center(sensor_data, reference, timestamp_col, real_timestamps):
    indices = []
    segments = []
    for i,r in sensor_data.iterrows():
        subject_id, device, measurement, path = \
                r["subject_id"], r["device"], r["measurement"], r["path"]
        if real_timestamps is not None: # REAL-PD
            sensor_measurement = align_and_load_sensor_data(
                    path = path,
                    timestamp_col = timestamp_col,
                    subject_id = subject_id,
                    device = device,
                    measurement = measurement,
                    real_timestamps = real_timestamps)
            if sensor_measurement.shape[0] == 0:
                continue
        else: # CIS-PD
            sensor_measurement = align_and_load_sensor_data(
                    path = path,
                    timestamp_col = timestamp_col)
        relevant_segments = reference.query("subject_id == @subject_id")
        if (relevant_segments.shape[0] == 1):
            # guaranteed to be unique because there is only one record per subject
            unique_index = relevant_segments.index[0]
            interval_timestamps = relevant_segments.melt(
                    var_name = "time_interval",
                    value_name = "timestamp")
            for _, cols in interval_timestamps.iterrows():
                interval, timestamp = cols["time_interval"], cols["timestamp"]
                interval = int(interval[-1])
                start_time, end_time = (timestamp - pd.DateOffset(minutes = 10),
                                        timestamp + pd.DateOffset(minutes = 10))
                segment_data = sensor_measurement.loc[start_time:end_time]
                if segment_data.shape[0]:
                    segment_data = segment_data.reset_index(drop=False)
                    unique_index = unique_index + (interval, device, measurement)
                    indices.append(unique_index)
                    segments.append(segment_data)
    segment_index = pd.MultiIndex.from_tuples(
            indices,
            names=["subject_id", "segment_id", "interval", "device", "measurement"])
    segment_df = pd.DataFrame({"segments": segments}, index=segment_index)
    return segment_df


def compute_real_segments(syn, subject_ids):
    real_pd_timestamps = pd.read_table(syn.get(REAL_PD_TIMESTAMPS).path, sep=";")
    real_pd_timestamps = real_pd_timestamps.dropna(subset = ["date_screening"])
    real_pd_timestamps["date_screening"] = \
            real_pd_timestamps["date_screening"].apply(dmy_to_ymd)
    # Munge on real_pd_timestamps to get segment start/stop (in *video* time)
    # off
    off_timestamps = real_pd_timestamps[
            real_pd_timestamps.OFF_UPDRS_start.notnull() &
            real_pd_timestamps.OFF_free_living_start.notnull()][
                    ["Record Id", "date_screening", "OFF_UPDRS_start", "OFF_free_living_start"]]
    off_timestamps = off_timestamps.rename({
        "Record Id": "subject_id",
        "OFF_UPDRS_start": "start_time",
        "OFF_free_living_start": "end_time"},
        axis=1)
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "start_time")
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "end_time")
    off_timestamps = off_timestamps.dropna()
    off_timestamps = off_timestamps.drop("date_screening", axis=1)
    off_timestamps["segment_id"] = [
            str(uuid.uuid4()) for i in range(len(off_timestamps))]
    off_timestamps["state"] = "off"
    off_timestamps = off_timestamps.set_index(
            ["subject_id", "state", "segment_id"])
    # on
    on_timestamps = real_pd_timestamps[
            real_pd_timestamps.ON_UPDRS_start.notnull() &
            real_pd_timestamps.ON_free_living_start.notnull()][
                    ["Record Id", "date_screening", "ON_UPDRS_start", "ON_free_living_start"]]
    on_timestamps = on_timestamps.rename({
        "Record Id": "subject_id",
        "ON_UPDRS_start": "start_time",
        "ON_free_living_start": "end_time"},
        axis=1)
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "start_time")
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "end_time")
    on_timestamps = on_timestamps.dropna()
    on_timestamps = on_timestamps.drop("date_screening", axis=1)
    on_timestamps["segment_id"] = [str(uuid.uuid4()) for i in range(len(on_timestamps))]
    on_timestamps["state"] = "on"
    on_timestamps = on_timestamps.set_index(
            ["subject_id", "state", "segment_id"])
    # combine off and on
    on_off_timestamps = on_timestamps.append(off_timestamps)
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
    hauser_timestamps = hauser_timestamps.rename(
            {"Record Id": "subject_id"}, axis=1)
    hauser_timestamps["segment_id"] = [str(uuid.uuid4()) for i in range(len(hauser_timestamps))]
    hauser_timestamps = hauser_timestamps.set_index(["subject_id", "segment_id"])
    return on_off_timestamps, hauser_timestamps


def fix_real_pd_datetimes(df, col_to_fix):
    # get rid of negative "times" (these look like negative integers)
    df[col_to_fix] = [float("nan") if str(i).startswith("-") else i
                      for i in df[col_to_fix]]
    # concatenate dates and times and convert to datetime objects
    df[col_to_fix] = [
            datetime.datetime.fromisoformat("{} {}".format(i[0], i[1]))
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
    if TESTING:
        query_str = "{} {}".format(query_str, "LIMIT 1")
    print(query_str)
    sensor_data = syn.tableQuery(query_str).asDataFrame()
    sensor_data["path"] = [syn.get(i).path for i in sensor_data.id]
    return sensor_data


def main():
    syn = sc.login()

    # CIS-PD
    cis_training_subjects = get_training_subjects(syn, CIS_TRAINING_LABELS)
    cis_smartwatch_data = download_sensor_data(
            syn,
            table = CIS_SENSOR_DATA,
            device = "smartwatch",
            subject_ids = cis_training_subjects)
    cis_segment_timestamps = compute_cis_segments(syn, cis_training_subjects)
    cis_segments = segment_cis_pd(
            sensor_data = cis_smartwatch_data,
            segment_timestamps = cis_segment_timestamps)

    # REAL-PD
    real_training_subjects = get_training_subjects(syn, REAL_TRAINING_LABELS)
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
    real_on_off_segment_timestamps, real_hauser_interval_timestamps = \
            compute_real_segments(syn, real_training_subjects)
    # Since we need to do some complicated video<->device timestamp
    # conversions we will compute start/stop timestamps for REAL-PD sensor
    # data files *in video time* so that both our segments and device
    # timestamps use the same time system.
    real_start_times = get_real_start_times(syn)
    real_video_to_device_offset = get_video_to_device_time_reference(syn)
    real_timestamps = real_start_times.merge(
            real_video_to_device_offset, on=["subject_id", "device"], how="outer")
    real_timestamps = real_timestamps.dropna() # some offsets are null
    real_timestamps["video_start_time"] = [
            t+s for t,s in zip(real_timestamps.device_start_time,
                               real_timestamps.offset)]
    real_timestamps["video_end_time"] = [
            t+s for t,s in zip(real_timestamps.device_end_time,
                               real_timestamps.offset)]
    real_on_off_segments, real_hauser_segments = segment_real_pd(
        smartwatch_data = real_smartwatch_data,
        smartphone_data = real_smartphone_data,
        on_off_segment_timestamps = real_on_off_segment_timestamps,
        hauser_interval_timestamps = real_hauser_interval_timestamps,
        real_timestamps = real_timestamps)
    # TODO store segments to Synapse as annotated files (or as table file handles
    # using earlier code?)



#main()

