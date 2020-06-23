import synapseclient as sc
import pandas as pd
import multiprocessing
import datetime
import tempfile
import uuid
import pytz


# CIS-PD
# Table3 MDS-UPDRS Part III
CIS_PD_UPDRS_P3_TABLE = "syn18435297" # start times
# Table4 Motor Task Timestamps and Scores
CIS_MOTOR_TASK_TIMESTAMPS = "syn18435302" # end times
CIS_SENSOR_DATA = "syn22144319"
CIS_TRAINING_LABELS = "syn21291578"

# REAL-PD
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
VIDEO_TO_DEVICE_TIME = "syn20645722" # indexed by pat_id (subject_id), device
REAL_UPDATED_WATCH_DATA = "syn21614548"
REAL_SMARTPHONE_DATA = "syn20542701"
REAL_TRAINING_LABELS = "syn21292049"

# MISC
TESTING = True # limits number of sensor data files downloaded
OUTPUT_PROJECT = "syn11611056" if TESTING else "syn22152015"
FRAC_TO_STORE = 1 # limits fraction of segments to upload to Synapse
NUM_PARALLEL = 2 # number of processes to use when segmenting
# These are identifiers used internally in the script that help us
# do some automated actions based on which table we're dealing with.
TABLE_TYPES = ["cis_segments", "real_updrs_segments", "real_hauser_segments"]


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
                start_time = all_times["start_time"].apply(
                    datetime.datetime.fromisoformat),
                end_time = all_times["end_time"].apply(
                    datetime.datetime.fromisoformat)))
    all_times["duration"] = all_times["end_time"] - all_times["start_time"]
    all_times["duration"] = all_times["duration"].apply(
            datetime.timedelta.total_seconds)
    all_times = all_times.query("duration > 0")
    all_times = all_times.sort_values("duration").groupby(
            ["subject_id", "start_time"]).first()
    all_times = all_times.drop("duration", axis=1)
    all_times = all_times.reset_index(drop=False)
    all_times["measurement_id"] = [
            str(uuid.uuid4()) for i in range(len(all_times))]
    all_times = all_times.set_index(["subject_id", "visit", "measurement_id"])
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


# This is for CIS-PD which has timestamps objects for timestamps
def strip_timestamp_from_segment(segment, timestamp_col):
    first_time = segment[timestamp_col].min()
    segment.loc[:,timestamp_col] = segment[timestamp_col].apply(
            lambda t : (t - first_time).total_seconds())
    return segment


# This is for REAL-PD which has integer times
def normalize_time_in_segment(segment, timestamp_col):
    # some of our hauser segments are null
    if not isinstance(segment, pd.DataFrame) and pd.isnull(segment):
        return segment
    first_time = segment[timestamp_col].min()
    segment.loc[:,timestamp_col] = segment[timestamp_col].apply(
            lambda t : t - first_time)
    return segment


#' Handles the special rules that apply to CIS-PD segments
def segment_cis_pd(sensor_data, segment_timestamps):
    cis_segments = segment_from_start_to_end(
            sensor_data = sensor_data,
            reference = segment_timestamps,
            timestamp_col = "Timestamp")
    cis_segments.loc[:,"segments"] = \
            cis_segments["segments"].apply(strip_timestamp_from_segment,
                                           timestamp_col="Timestamp")
    return(cis_segments)


#' Handles the special rules that apply to REAL-PD segments
def segment_real_pd(smartphone_data, smartwatch_data, on_off_segment_timestamps,
        hauser_interval_timestamps, video_to_device_offset):
    on_off_smartphone_segments = segment_from_start_to_end(
            sensor_data = smartphone_data,
            reference = on_off_segment_timestamps,
            timestamp_col = "time",
            video_to_device_offset = video_to_device_offset)
    on_off_smartwatch_segments = segment_from_start_to_end(
            sensor_data = smartwatch_data,
            reference = on_off_segment_timestamps,
            timestamp_col = "time",
            video_to_device_offset = video_to_device_offset)
    hauser_smartphone_segments = segment_from_center(
            sensor_data = smartphone_data,
            reference = hauser_interval_timestamps,
            timestamp_col = "time",
            video_to_device_offset = video_to_device_offset)
    hauser_smartwatch_segments = segment_from_center(
            sensor_data = smartwatch_data,
            reference = hauser_interval_timestamps,
            timestamp_col = "time",
            video_to_device_offset = video_to_device_offset)
    on_off_segments = on_off_smartwatch_segments.append(
            other = on_off_smartphone_segments,
            ignore_index = False)
    on_off_segments.loc[:,"segments"] = \
            on_off_segments["segments"].apply(normalize_time_in_segment,
                                              timestamp_col="t")
    hauser_segments = hauser_smartphone_segments.append(
            other = hauser_smartwatch_segments,
            ignore_index = False)
    # This will drop null (missing) start_time and end_time values.
    # I initially considered keeping these (explicit missingingness is better
    # than implicit) but they caused too many issues for pandas downstream.
    # And we can add the index back in later if needed.
    hauser_segments = hauser_segments.dropna(axis='index')
    hauser_segments.loc[:,"segments"] = \
            hauser_segments["segments"].apply(normalize_time_in_segment,
                                              timestamp_col="t")
    return on_off_segments, hauser_segments


def align_and_load_sensor_data(path, timestamp_col, subject_id = None,
        device = None, measurement = None, video_to_device_offset = None):
    if video_to_device_offset is not None: # REAL-PD
        offset_df = video_to_device_offset.query(
                "subject_id == @subject_id and device == @device")
        if offset_df.shape[0] == 1:
            offset = offset_df["offset"].iloc[0].seconds
            sensor_measurement = pd.read_csv(path)
            sensor_measurement[timestamp_col] = \
                sensor_measurement[timestamp_col].apply(
                        lambda s : s - offset)
            timestamp_col = "t"
            if device == "Smartwatch": # watch update data includes additional col
                new_cols = [timestamp_col, "device_id", "x", "y", "z"]
            else:
                new_cols = [timestamp_col, "x", "y", "z"]
            sensor_measurement.columns = new_cols
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
video_to_device_offset -- For REAL-PD, sensor data timestamps are relative to
                   video time. This is a dataframe indexed by subject_id and
                   device like that returned by
                   `get_video_to_device_time_reference`. This dataframe also
                   has one column 'offset' which contains the number of seconds
                   to subtract from the sensor data timestamps.
"""
def segment_from_start_to_end(sensor_data, reference, timestamp_col,
                              video_to_device_offset = None):
    indices = []
    segments = []
    for i,r in sensor_data.iterrows():
        subject_id, device, measurement, path = \
                r["subject_id"], r["device"], r["measurement"], r["path"]
        if video_to_device_offset is not None: # REAL-PD
            sensor_measurement = align_and_load_sensor_data(
                    path = path,
                    timestamp_col = timestamp_col,
                    subject_id = subject_id,
                    device = device,
                    measurement = measurement,
                    video_to_device_offset = video_to_device_offset)
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
                this_index = index + (device, measurement) # for REAL-PD smartwatch
                indices.append(this_index)
                segments.append(segment_data)
    if len(segments) and len(indices):
        segment_index = pd.MultiIndex.from_tuples(
                indices,
                names=list(relevant_segments.index.names)+["device", "measurement"])
    else: # we didn't find any matching segments in the data!
        segment_index = []
    segment_df = pd.DataFrame({"segments": segments}, index=segment_index)
    return segment_df


"""
This is for columns Time_interval_X where we are segmenting data
centered around a single timepoint
sensor_data is a dataframe like one returned from `download_sensor_data`,
with (at least) columns subject_id and path
`reference` is a dataframe with an index containing at least the `subject_id`
and two datetime columns: start_time and end_time
`timestamp_col` is the name of the column *in a sensor data file* where
the time is recorded. E.g., "Timestamp" for CIS-PD sensor data.
`video_to_device_offset` is index by subject_id, device and has the time offset
between video and device (see `get_video_to_device_time_reference`)
"""
def segment_from_center(sensor_data, reference, timestamp_col, video_to_device_offset):
    indices = []
    segments = []
    for i,r in sensor_data.iterrows():
        subject_id, device, measurement, path = \
                r["subject_id"], r["device"], r["measurement"], r["path"]
        sensor_measurement = align_and_load_sensor_data(
                path = path,
                timestamp_col = timestamp_col,
                subject_id = subject_id,
                device = device,
                measurement = measurement,
                video_to_device_offset = video_to_device_offset)
        if sensor_measurement.shape[0] == 0:
            continue
        relevant_segments = reference.query("subject_id == @subject_id")
        if (relevant_segments.shape[0] == 6): # There are 6 hauser intervals
            for i, cols in relevant_segments.iterrows():
                start_time, end_time = cols["start_time"], cols["end_time"]
                this_index = i + (device, measurement, cols["time_interval"],
                                  start_time, end_time)
                if pd.isnull(start_time) or pd.isnull(end_time):
                    indices.append(this_index)
                    segments.append(None)
                else:
                    segment_data = sensor_measurement.loc[start_time:end_time]
                    if segment_data.shape[0]:
                        segment_data = segment_data.reset_index(drop=False)
                        indices.append(this_index)
                        segments.append(segment_data)
    if len(segments) and len(indices):
        segment_index = pd.MultiIndex.from_tuples(
                indices,
                names=list(relevant_segments.index.names) +
                      ["device", "measurement", "time_interval",
                       "start_time", "end_time"])
    else: # we didn't find any matching segments in the data!
        segment_index = []
    segment_df = pd.DataFrame({"segments": segments}, index=segment_index)
    return segment_df


def compute_real_segments(syn, subject_ids):
    real_pd_timestamps = pd.read_table(syn.get(REAL_PD_TIMESTAMPS).path, sep=";")
    # Munge on real_pd_timestamps to get segment start/stop (in *video* time)
    # off
    off_timestamps = real_pd_timestamps[
            real_pd_timestamps.OFF_UPDRS_start.notnull() &
            real_pd_timestamps.OFF_free_living_start.notnull()][
                    ["Record Id", "OFF_UPDRS_start", "OFF_free_living_start"]]
    off_timestamps = off_timestamps.rename({
        "Record Id": "subject_id",
        "OFF_UPDRS_start": "start_time",
        "OFF_free_living_start": "end_time"},
        axis=1)
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "start_time")
    off_timestamps = fix_real_pd_datetimes(off_timestamps, "end_time")
    off_timestamps = off_timestamps.dropna()
    off_timestamps["measurement_id"] = [
            str(uuid.uuid4()) for i in range(len(off_timestamps))]
    off_timestamps["state"] = "off"
    off_timestamps = off_timestamps.set_index(
            ["subject_id", "state", "measurement_id"])
    # on
    on_timestamps = real_pd_timestamps[
            real_pd_timestamps.ON_UPDRS_start.notnull() &
            real_pd_timestamps.ON_free_living_start.notnull()][
                    ["Record Id", "ON_UPDRS_start", "ON_free_living_start"]]
    on_timestamps = on_timestamps.rename({
        "Record Id": "subject_id",
        "ON_UPDRS_start": "start_time",
        "ON_free_living_start": "end_time"},
        axis=1)
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "start_time")
    on_timestamps = fix_real_pd_datetimes(on_timestamps, "end_time")
    on_timestamps = on_timestamps.dropna()
    on_timestamps["measurement_id"] = [
            str(uuid.uuid4()) for i in range(len(on_timestamps))]
    on_timestamps["state"] = "on"
    on_timestamps = on_timestamps.set_index(
            ["subject_id", "state", "measurement_id"])
    # combine off and on
    on_off_timestamps = on_timestamps.append(off_timestamps)
    # hauser diary
    hauser_time_cols = ["Time_interval_{}".format(i) for i in range(1,7)]
    hauser_timestamps = real_pd_timestamps[
            ["Record Id"] + hauser_time_cols]
    for c in hauser_time_cols:
        hauser_timestamps = fix_real_pd_datetimes(hauser_timestamps, c)
    hauser_timestamps = hauser_timestamps.dropna(
            subset = hauser_time_cols, how="all")
    hauser_timestamps = hauser_timestamps.rename(
            {"Record Id": "subject_id"}, axis=1)
    hauser_timestamps = hauser_timestamps.melt(
            id_vars= "subject_id",
            var_name = "time_interval",
            value_name = "timestamp")
    hauser_timestamps["measurement_id"] = [
            str(uuid.uuid4()) for i in range(len(hauser_timestamps))]
    hauser_timestamps["time_interval"] = hauser_timestamps["time_interval"].apply(
            lambda s : int(s[-1]))
    hauser_timestamps["start_time"] = hauser_timestamps["timestamp"] - 10*60
    hauser_timestamps["end_time"] = hauser_timestamps["timestamp"] + 10*60
    hauser_timestamps = hauser_timestamps.drop("timestamp", axis=1)
    hauser_timestamps = hauser_timestamps.set_index(
            ["subject_id", "measurement_id"])
    return on_off_timestamps, hauser_timestamps


def fix_real_pd_datetimes(df, col_to_fix):
    # get rid of negative "times" (these look like negative integers)
    df[col_to_fix] = [float("nan") if str(i).startswith("-") else i
                      for i in df[col_to_fix]]
    # concatenate dates and times and convert to datetime objects
    df[col_to_fix] = [
            hours_minutes_to_seconds(i)
            if pd.notnull(i) else float("nan")
            for i in df[col_to_fix]]
    return(df)


def hours_minutes_to_seconds(s):
    s_ = s.split(":")
    return 60 * 60 * int(s_[0]) + 60 * int(s_[1])


"""
Replace a single column with a file handle ID

This column should be called `segment`. Afterwards, reshape the
dataframe so that device_measurement columns are used (depending
on the dataset).
"""
def replace_col_with_filehandles(syn, df, col, parent, upload_in_parallel):
    if upload_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        df.loc[:,col] = list(mp.map(
                lambda df_ : replace_dataframe_with_filehandle(syn, df_, parent),
                df[col]))
    else:
        for col in cols:
            df.loc[:,col] = list(map(
                    lambda df_ : replace_dataframe_with_filehandle(syn, df_, parent),
                    df[col]))


def replace_dataframe_with_filehandle(syn, df, parent):
    if isinstance(df, pd.DataFrame):
        f = tempfile.NamedTemporaryFile(suffix=".csv")
        df.to_csv(f.name, index=False)
        syn_f = syn.uploadFileHandle(
                path = f.name,
                parent = parent,
                mimetype="text/csv")
        f.close()
        return syn_f["id"]
    else:
        return ""


def create_cols(table_type):
    if table_type == "cis_segments":
        cols = [sc.Column(name="measurement_id", columnType="STRING"),
                sc.Column(name="subject_id", columnType="STRING"),
                sc.Column(name="visit", columnType="STRING"),
                sc.Column(name="device", columnType="STRING"),
                sc.Column(name="measurement", columnType="STRING"),
                sc.Column(name="start_time", columnType="DATE"),
                sc.Column(name="end_time", columnType="DATE"),
                sc.Column(name="smartwatch_accelerometer", columnType="FILEHANDLEID")]
    elif table_type == "real_updrs_segments":
        cols = [sc.Column(name="measurement_id", columnType="STRING"),
                sc.Column(name="subject_id", columnType="STRING"),
                sc.Column(name="state", columnType="STRING"),
                sc.Column(name="start_time", columnType="INTEGER"),
                sc.Column(name="end_time", columnType="INTEGER"),
                sc.Column(name="smartphone_accelerometer", columnType="FILEHANDLEID"),
                sc.Column(name="smartwatch_accelerometer", columnType="FILEHANDLEID"),
                sc.Column(name="smartwatch_gyroscope", columnType="FILEHANDLEID")]
    elif table_type == "real_hauser_segments":
        cols = [sc.Column(name="measurement_id", columnType="STRING"),
                sc.Column(name="subject_id", columnType="STRING"),
                sc.Column(name="time_interval", columnType="INTEGER"),
                sc.Column(name="start_time", columnType="INTEGER"),
                sc.Column(name="end_time", columnType="INTEGER"),
                sc.Column(name="smartphone_accelerometer", columnType="FILEHANDLEID"),
                sc.Column(name="smartwatch_accelerometer", columnType="FILEHANDLEID"),
                sc.Column(name="smartwatch_gyroscope", columnType="FILEHANDLEID")]
    else:
        raise TypeError(f"table_type must be one of {', '.join(TABLE_TYPES)}")
    return cols


def store_dataframe_to_synapse(syn, df, parent, name, cols):
    df = df[[c['name'] for c in cols]]
    schema = sc.Schema(name = name, columns = cols, parent = parent)
    table = sc.Table(schema, df)
    table = syn.store(table)
    return table


def list_append_to_query_str(query_str, col, list_of_things):
    if isinstance(list_of_things, list):
        str_of_things = "('" + "','".join(list_of_things) + "')"
    else:
        str_of_things = ""
    if str_of_things:
        query_str = "{} {}".format(
                query_str, "AND {} IN {}".format(col, str_of_things))
    return(query_str)


"""
Creating file handles requires a parent. The parent in this case is
the Synapse table containing segment information and the file handle
for that segment. We don't have the file handles yet (because we first
need the parent!) so let's create the parent with a placeholder column
for the file handles.
"""
def store_placeholder_table(syn, df, parent, name, table_type):
    df = df.reset_index(drop=False) # move index to columns
    if "segments" in df.columns: # the column we are (temporarily) using for segments
        df = df.drop("segments", axis=1)
    table_cols = create_cols(table_type=table_type)
    for col in table_cols:
        col_name = col.name
        if col_name not in df.columns: # create placeholder column
            df[col_name] = None
    df = df[[c.name for c in table_cols]]
    schema = sc.Schema(name = name, columns = table_cols, parent = parent)
    placeholder_table = sc.Table(schema, df)
    placeholder_table = syn.store(placeholder_table)
    return placeholder_table



# Seriously pandas?! This issue has been open for 1.5+ years?!
def multiIndex_pivot(df, index = None, columns = None, values = None):
    # https://github.com/pandas-dev/pandas/issues/23955
    output_df = df.copy(deep = True)
    if index is None:
        names = list(output_df.index.names)
        output_df = output_df.reset_index()
    else:
        names = index
    output_df = output_df.assign(tuples_index = [tuple(i) for i in output_df[names].values])
    if isinstance(columns, list):
        output_df = output_df.assign(tuples_columns = [tuple(i) for i in output_df[columns].values])  # hashable
        output_df = output_df.pivot(index = 'tuples_index', columns = 'tuples_columns', values = values)
        output_df.columns = pd.MultiIndex.from_tuples(output_df.columns, names = columns)  # reduced
    else:
        output_df = output_df.pivot(index = 'tuples_index', columns = columns, values = values)
    output_df.index = pd.MultiIndex.from_tuples(output_df.index, names = names)
    return output_df


# This is for REAL-PD, which has multiple sensor recordings for each measurement
# for both UPDRS and Hauser.
def move_device_and_measurement_to_cols(df):
    df = df.reset_index(["device", "measurement"], drop=False)
    df["col_names"] = [f"{i.lower()}_{j.lower()}"
                       for i, j in zip(df.device, df.measurement)]
    df = df.drop(["device", "measurement"], axis=1)
    df = multiIndex_pivot(df, index=None, columns="col_names", values="segments")
    return(df)


def align_file_handles_with_synapse_table(syn, table_id, file_handle_df):
    # fetch row id / version
    synapse_table = syn.tableQuery(f"SELECT * FROM {table_id}").asDataFrame()
    # get rid of our placeholder columns
    synapse_table = synapse_table.dropna(axis=1, how="all")
    # move any potential index to columns
    file_handle_df = file_handle_df.reset_index(drop=False)
    # cast columns to the same type before merging, handle hauser specific issue
    if "time_interval" in file_handle_df.columns:
            file_handle_df.loc[:,"time_interval"] = \
                    file_handle_df["time_interval"].astype(int).astype(str)
    synapse_table = synapse_table.astype(str)
    # we will restore these when we merge
    synapse_table = synapse_table.drop(["start_time", "end_time"], axis=1)
    file_handle_df = file_handle_df.astype(str)
    file_handle_df.columns.name = None # fix bug from pivot_multiIndex function
    # merge across specific shared columns
    if "time_interval" in file_handle_df.columns:
        synapse_table_with_file_handles = synapse_table.merge(
                file_handle_df,
                on=["measurement_id", "time_interval", "subject_id"])
    elif "state" in file_handle_df.columns:
        synapse_table_with_file_handles = synapse_table.merge(
                file_handle_df,
                on=["measurement_id", "subject_id", "state"])
    else: # CIS-PD
        synapse_table_with_file_handles = synapse_table.merge(file_handle_df)
    # restore original index
    index_subset = synapse_table.index[
            [i in synapse_table_with_file_handles.measurement_id.values
             for i in synapse_table.measurement_id.values]]
    synapse_table_with_file_handles = \
            synapse_table_with_file_handles.set_index(index_subset)
    return synapse_table_with_file_handles


def download_sensor_data(syn, table, device, subject_ids=None, measurements=None,
                         context=None):
    query_str = "SELECT * FROM {} WHERE device = '{}'".format(table, device)
    query_str = list_append_to_query_str(query_str, "subject_id", subject_ids)
    query_str = list_append_to_query_str(query_str, "measurement", measurements)
    if context is not None:
        query_str = f"{query_str} AND context='{context}'"
    if TESTING:
        query_str = "{} {}".format(query_str, "LIMIT 2")
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
    # shuffle records so that file handle integer contains no useful information
    shuffled_cis_segments = cis_segments.sample(frac=FRAC_TO_STORE)
    # store a placeholder table for our filehandles
    cis_table = store_placeholder_table(
            syn = syn,
            df = cis_segment_timestamps,
            parent = OUTPUT_PROJECT,
            name = "CIS-PD UPDRS Segmented Smartwatch Measurements",
            table_type = "cis_segments")
    # replace the dataframes with file handles
    replace_col_with_filehandles( # replaces in-place
            syn,
            df = shuffled_cis_segments,
            col = "segments",
            parent = cis_table.schema.id,
            upload_in_parallel = True)
    # put our dataframe in the same format as the placeholder table
    shuffled_cis_segments = shuffled_cis_segments.rename(
            {"segments": "smartwatch_accelerometer"}, axis=1)
    # re-align our file handes with the placeholder table
    realigned_cis_segments = align_file_handles_with_synapse_table(
            syn = syn,
            table_id = cis_table.schema.id,
            file_handle_df = shuffled_cis_segments)
    # backup before storing in case Synapse is feeling moody
    realigned_cis_segments.to_csv("cis_backup.csv", index=True)
    # store to synapse
    syn.store(sc.Table(cis_table.schema.id, realigned_cis_segments))

    # REAL-PD
    real_training_subjects = get_training_subjects(syn, REAL_TRAINING_LABELS)
    real_smartwatch_data = download_sensor_data(
            syn,
            table = REAL_UPDATED_WATCH_DATA,
            device = "Smartwatch",
            subject_ids = real_training_subjects,
            measurements = ["accelerometer", "gyroscope"],
            context = "home_visit")
    real_smartphone_data = download_sensor_data(
            syn,
            table = REAL_SMARTPHONE_DATA,
            device = "Smartphone",
            subject_ids = real_training_subjects,
            measurements = ["accelerometer"],
            context = "home_visit")
    real_on_off_segment_timestamps, real_hauser_interval_timestamps = \
            compute_real_segments(syn, real_training_subjects)
    # As our 'gold standard' for sensor_data timestamps we are using
    # an offset from video time. These offsets will be subtracted from
    # the timestamp values in the sensor data files.
    real_video_to_device_offset = get_video_to_device_time_reference(syn)
    real_on_off_segments, real_hauser_segments = segment_real_pd(
        smartwatch_data = real_smartwatch_data,
        smartphone_data = real_smartphone_data,
        on_off_segment_timestamps = real_on_off_segment_timestamps,
        hauser_interval_timestamps = real_hauser_interval_timestamps,
        video_to_device_offset = real_video_to_device_offset)
    # shuffle records so that file handle integer contains no useful information
    shuffled_on_off_segments = real_on_off_segments.sample(frac=FRAC_TO_STORE)
    shuffled_hauser_segments = real_hauser_segments.sample(frac=FRAC_TO_STORE)
    # store a placeholder table for our filehandles
    real_on_off_table = store_placeholder_table(
            syn = syn,
            df = real_on_off_segment_timestamps,
            parent = OUTPUT_PROJECT,
            name = "REAL-PD UPDRS Segmented Smartphone and Smartwatch Measurements",
            table_type = "real_updrs_segments")
    real_hauser_table = store_placeholder_table(
            syn = syn,
            df = real_hauser_interval_timestamps,
            parent = OUTPUT_PROJECT,
            name = "REAL-PD Hauser Diary Segmented Smartphone and Smartwatch Measurements",
            table_type = "real_hauser_segments")
    # replace the dataframes with file handles
    replace_col_with_filehandles( # replaces in-place
            syn,
            df = shuffled_on_off_segments,
            col = "segments",
            parent = real_on_off_table.schema.id,
            upload_in_parallel = True)
    replace_col_with_filehandles( # replaces in-place
            syn,
            df = shuffled_hauser_segments,
            col = "segments",
            parent = real_hauser_table.schema.id,
            upload_in_parallel = True)
    # put our dataframes in the same format as the placeholder table
    shuffled_on_off_segments = move_device_and_measurement_to_cols(
            shuffled_on_off_segments)
    shuffled_hauser_segments = move_device_and_measurement_to_cols(
            shuffled_hauser_segments)
    # re-align our file handes with the placeholder table
    realigned_on_off_segments = align_file_handles_with_synapse_table(
            syn = syn,
            table_id = real_on_off_table.schema.id,
            file_handle_df = shuffled_on_off_segments)
    realigned_hauser_segments = align_file_handles_with_synapse_table(
            syn = syn,
            table_id = real_hauser_table.schema.id,
            file_handle_df = shuffled_hauser_segments)
    # Replace null file handle values (which are strings at this point)
    for c in ["smartphone_accelerometer", "smartwatch_accelerometer", "smartwatch_gyroscope"]:
        realigned_on_off_segments.loc[:,c] = \
                realigned_on_off_segments[c].replace({"nan": ""})
        realigned_hauser_segments.loc[:,c] = \
                realigned_hauser_segments[c].replace({"nan": ""})

    # backup before storing in case Synapse is feeling moody
    realigned_on_off_segments.to_csv("real_on_off_backup.csv", index=True)
    realigned_hauser_segments.to_csv("real_hauser_backup.csv", index=True)
    # store to synapse
    syn.store(sc.Table(real_on_off_table.schema.id, realigned_on_off_segments))
    syn.store(sc.Table(real_hauser_table.schema.id, realigned_hauser_segments))


#main()

