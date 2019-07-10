'''
Stores three tables to Synapse (under `TABLE_OUTPUT`):

MC10 Home Sensor Measurements
    columns measurement_id, sensor_location, mc10_accelerometer,
    mc10_gyroscope, mc10_emg
Smartwatch Home Sensor Measurements
    columns measurement_id, smartwatch_accelerometer
Motor Task Home State Info
    columns measurement_id, subject_id, start_utc, stop_utc, on_off,
    tremor, dyskinesia, activity_intensity
'''

import os
import uuid
import argparse
import tempfile
import datetime
import multiprocessing
import synapseclient as sc
import synapseutils as su
import pandas as pd

TESTING = True
DIARY = "syn18435314"
MC10_MEASUREMENTS = "syn18822536" if TESTING else "syn18435632"
SMARTWATCH_MEASUREMENTS = "syn18822537" if TESTING else "syn18435623"
SMARTWATCH_SENSOR_NAME = "smartwatch"
MC10_SENSOR_NAME = "mc10"
FRAC_TO_STORE = 0.02 if TESTING else 1
TABLE_OUTPUT = "syn11611056" if TESTING else "syn18407520"
DIARY_COL_MAP = {
        "SubjID": "subject_id",
        "Timestamp": "timestamp",
        "Reported Timestamp": "reported_timestamp",
        "Measurement Name": "measurement",
        "Value": "value"}


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-in-parallel", action="store_const",
            const=True, default = False)
    parser.add_argument("--upload-in-parallel", action="store_const",
            const=True, default = False)
    args = parser.parse_args()
    return(args)


def read_syn_table(syn, synapse_id, q = "select * from {}"):
    q = syn.tableQuery(q.format(synapse_id))
    df = q.asDataFrame()
    return df


def parse_info_from_filename(fname, sensor):
    if sensor == SMARTWATCH_SENSOR_NAME:
        _, subject_id, year_month = os.path.splitext(fname)[0].split("_")
        year, month = tuple(map(int, year_month.split("-")))
    elif sensor == MC10_SENSOR_NAME:
        subject_id = int(os.path.splitext(fname)[0].split("_")[1])
        year, month = None, None
    else:
        raise TypeError("sensor must be one of {} or {}".format(
            SMARTWATCH_SENSOR_NAME, MC10_SENSOR_NAME))
    return subject_id, year, month


def find_relevant_diary_entries(fname, diary, sensor):
    def is_match(t, year, month, f_subject_id, diary_subject_id):
        if int(f_subject_id) == int(diary_subject_id):
            if year is None or month is None:
                return True
            elif isinstance(year, int) and isinstance(month, int):
                return year == t.year and month == t.month
            else:
                raise TypeError("Year and month must both be integers.")
        else:
            return False
    subject_id, year, month = parse_info_from_filename(fname, sensor)
    relevent_entries = diary.apply(
            lambda s : is_match(s.timestamp, year, month, subject_id, s.subject_id),
            axis = 1)
    return diary[relevent_entries]


def new_data_column(syn, diary, col, parent, filtering_prefix, sensor,
                    download_in_parallel):
    relevant_entities = download_relevant_children(
            syn, parent, filtering_prefix, diary, sensor, download_in_parallel)
    all_sliced_measurements = pd.DataFrame()
    for syn_id in relevant_entities:
        f, measurement_ids = (relevant_entities[syn_id]["synapse_file"],
                              relevant_entities[syn_id]["measurement_ids"])
        sliced_measurements = slice_sensor_measurement(f, diary, measurement_ids, sensor)
        sliced_measurements = sliced_measurements.rename(
                {"sensor_data": col}, axis = 1)
        relevant_entities[syn_id]["data"] = sliced_measurements
    if len(relevant_entities):
        all_sliced_measurements = pd.concat(
                [relevant_entities[syn_id]["data"] for syn_id in relevant_entities],
                axis=0)
    return(all_sliced_measurements)


def download_relevant_children(syn, parent, filtering_prefix, diary, sensor,
                               download_in_parallel=False):
    """
    Returns
    -------
    dict with key synapse_id (str) and values
    synapse_file (File), measurement_ids (list)
    """
    _, _, entity_info = list(su.walk(syn, parent))[0]
    entity_info = [(i, j) for i, j in entity_info if i.startswith(filtering_prefix)]
    relevant_entities = {}
    for fname, syn_id in entity_info:
        relevant_entries = find_relevant_diary_entries(fname, diary, sensor)
        if len(relevant_entries):
            relevant_entities[syn_id] = {"synapse_file": None,
                                         "measurement_ids": relevant_entries.measurement_id}
    ordered_synapse_ids = list(relevant_entities.keys())
    if download_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        children = mp.map(syn.get, ordered_synapse_ids)
    else:
        children = list(map(syn.get, ordered_synapse_ids))
    for syn_id, f in zip(ordered_synapse_ids, children):
        relevant_entities[syn_id]["synapse_file"] = f
    return relevant_entities


def slice_from_diary(sensor_measurement, diary_entry, sensor):
    """
    Returns
    -------
    a pandas DataFrame with columns location and sensor_data
    """
    result = pd.DataFrame(columns = ["sensor_location", "sensor_data"])
    ten_minutes = datetime.timedelta(minutes=10)
    start, stop = (diary_entry.loc["timestamp"] - ten_minutes,
                   diary_entry.loc["timestamp"] + ten_minutes)
    relevant_range = sensor_measurement[start:stop]
    if len(relevant_range) == 0:
        pass
    elif sensor == "mc10":
        for location in relevant_range.Location.unique():
            local_range = relevant_range.query("Location == @location")
            local_range = local_range.drop(["SubjID", "Location"], axis = 1)
            local_range = local_range.reset_index(drop=False)
            time_zero = local_range.Timestamp.iloc[0]
            local_range.Timestamp = local_range.Timestamp - time_zero
            local_range.Timestamp = local_range.Timestamp.apply(
                    lambda td : td.total_seconds())
            location = "_".join(location.split())
            result = result.append({"sensor_location": location,
                                    "sensor_data": local_range},
                                   ignore_index=True)
    elif sensor == "smartwatch":
        local_range = relevant_range.drop(["SubjID"], axis = 1)
        local_range = local_range.reset_index(drop=False)
        time_zero = local_range.Timestamp.iloc[0]
        time_end = local_range.Timestamp.iloc[-1]
        local_range.Timestamp = local_range.Timestamp - time_zero
        local_range.Timestamp = local_range.Timestamp.apply(
                lambda td : td.total_seconds())
        result = result.append({"sensor_location": None,
                                "sensor_data": local_range},
                               ignore_index=True)
    #result.index = pd.Index([score.name] * len(result))
    return result


def slice_sensor_measurement(f, diary, relevant_measurement_ids, sensor):
    sensor_measurement = pd.read_csv(f.path)
    sensor_measurement.Timestamp = pd.to_datetime(sensor_measurement.Timestamp)
    sensor_measurement.set_index("Timestamp", drop=True, inplace=True)
    sensor_measurement.sort_index(inplace=True)
    relevant_diary_entries = diary.loc[relevant_measurement_ids,["timestamp"]]
    measurements = relevant_diary_entries.apply(
            lambda diary_entry : slice_from_diary(sensor_measurement, diary_entry, sensor),
            axis = 1)
    measurements = pd.concat(measurements.values, axis=0)
    return(measurements)


def replace_cols_with_filehandles(syn, df, cols, upload_in_parallel):
    if upload_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        for col in cols:
            df.loc[:,col] = list(mp.map(
                    lambda df_ : replace_dataframe_with_filehandle(syn, df_),
                    df[col]))
    else:
        for col in cols:
            df.loc[:,col] = list(map(
                    lambda df_ : replace_dataframe_with_filehandle(syn, df_),
                    df[col]))


def replace_dataframe_with_filehandle(syn, df):
    if isinstance(df, pd.DataFrame):
        f = tempfile.NamedTemporaryFile(suffix=".csv")
        df.to_csv(f.name, index=False)
        syn_f = syn.uploadSynapseManagedFileHandle(
                f.name, mimetype="text/csv")
        f.close()
        return syn_f["id"]
    else:
        return ""


def read_diary(syn):
    diary = read_syn_table(syn, DIARY)
    diary = diary.rename(DIARY_COL_MAP, axis = 1)
    diary.timestamp = pd.to_datetime(diary.timestamp)
    diary.reported_timestamp = pd.to_datetime(diary.reported_timestamp)
    diary = diary.sort_values(
            ["subject_id", "timestamp", "measurement", "reported_timestamp"])
    diary = diary.drop_duplicates(
            ["subject_id", "timestamp", "measurement"], keep="last")
    grouped_diary = diary.groupby(["subject_id", "timestamp"])
    def assign_measurement_id(group):
        group["measurement_id"] = str(uuid.uuid4())
        return group
    diary_with_id = grouped_diary.apply(assign_measurement_id)
    reshaped_diary = diary_with_id.pivot(
            index="measurement_id", columns="measurement", values="value")
    diary_with_id_col_subset = diary_with_id.drop(
            ["measurement", "value", "reported_timestamp"], axis=1)
    diary_with_id_col_subset = diary_with_id_col_subset.set_index(
            "measurement_id", drop=False)
    diary_with_id_col_subset = diary_with_id_col_subset.drop_duplicates(
            ["subject_id", "timestamp"])
    final_diary = diary_with_id_col_subset.join(reshaped_diary)
    final_diary = final_diary.rename({
        "Activity Intensity": "activity_intensity",
        "Dyskinesia": "dyskinesia",
        "On/Off": "on_off",
        "Tremor": "tremor"}, axis=1)
    return(final_diary)


def move_index_to_column(df):
    df.reset_index(drop=False, inplace=True)
    df.rename({"index": "measurement_id"}, axis=1, inplace=True)


def create_cols(table_type, syn=None):
    if table_type == MC10_SENSOR_NAME:
        cols = [sc.Column(name="measurement_id", columnType="STRING"),
                sc.Column(name="sensor_location", columnType="STRING"),
                sc.Column(name="mc10_accelerometer", columnType="FILEHANDLEID"),
                sc.Column(name="mc10_gyroscope", columnType="FILEHANDLEID"),
                sc.Column(name="mc10_emg", columnType="FILEHANDLEID")]
    elif table_type == SMARTWATCH_SENSOR_NAME:
        cols = [sc.Column(name="measurement_id", columnType="STRING"),
                sc.Column(name="smartwatch_accelerometer", columnType="FILEHANDLEID")]
    elif table_type == "scores":
        cols = list(syn.getTableColumns(SCORES))
        for c in cols:
            c.pop('id')
            if c['name'] in SCORES_COL_MAP:
                c['name'] = SCORES_COL_MAP[c['name']]
        cols = [sc.Column(name="measurement_id",
                          columnType="STRING")] + cols
    else:
        raise TypeError("table_type must be one of [{}, {}, {}]".format(
            MC10_SENSOR_NAME, SMARTWATCH_SENSOR_NAME, "scores"))
    return cols


def store_dataframe_to_synapse(syn, df, parent, name, cols):
    schema = sc.Schema(name = name, columns = cols, parent = parent)
    table = sc.Table(schema, df)
    table = syn.store(table)
    return table


def main():
    args = read_args()
    syn = sc.login()
    diary = read_diary(syn)

    # curate dataframes containing respective data measurements
    mc10_accelerometer = new_data_column(
            syn,
            diary = diary,
            col = "mc10_accelerometer",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9A",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    mc10_gyroscope = new_data_column(
            syn,
            diary = diary,
            col = "mc10_gyroscope",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9B",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    mc10_emg = new_data_column(
            syn,
            diary = diary,
            col = "mc10_emg",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9C",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    smartwatch_accelerometer = new_data_column(
            syn,
            diary = diary,
            col = "smartwatch_accelerometer",
            parent = SMARTWATCH_MEASUREMENTS,
            filtering_prefix = "Table8",
            sensor = "smartwatch",
            download_in_parallel = args.download_in_parallel)
    smartwatch_accelerometer = smartwatch_accelerometer.drop(
            "sensor_location", axis=1)

    # move measurement_id from index to column
    for df in [mc10_accelerometer, mc10_gyroscope,
               mc10_emg, smartwatch_accelerometer]:
        move_index_to_column(df)

    # combine mc10 measurements into a single file
    merged_mc10 = pd.DataFrame()
    if len(mc10_accelerometer) and len(mc10_gyroscope):
        merged_mc10 = mc10_accelerometer.merge(mc10_gyroscope, how="outer")
    if len(merged_mc10) and len(mc10_emg):
        merged_mc10 = merged_mc10.merge(mc10_emg, how="outer")

    # shuffle records so that file handle integer contains no useful information
    shuffled_mc10 = merged_mc10.sample(frac=FRAC_TO_STORE)
    shuffled_smartwatch = smartwatch_accelerometer.sample(frac=FRAC_TO_STORE)

    # replace the dataframes with file handles
    replace_cols_with_filehandles( # replaces in-place
            syn,
            df = shuffled_mc10,
            cols = ["mc10_accelerometer", "mc10_gyroscope", "mc10_emg"],
            upload_in_parallel = args.upload_in_parallel)
    replace_cols_with_filehandles( # replaces in-place
            syn,
            df = shuffled_smartwatch,
            cols = ["smartwatch_accelerometer"],
            upload_in_parallel = args.upload_in_parallel)

    # make the dataframes look pretty
    shuffled_mc10.sort_values(["measurement_id", "sensor_location"], inplace=True)
    shuffled_smartwatch.sort_values("measurement_id", inplace=True)

    # backup in case we just created a bajillion file handles but
    # are rejected during table store
    shuffled_mc10.to_csv("mc10_backup.csv", index=False)
    shuffled_smartwatch.to_csv("smartwatch_backup.csv", index=False)
    scores.to_csv("scores_backup.csv", index=False)

    # store to synapse
    shuffled_mc10_table = store_dataframe_to_synapse(
            syn,
            df = shuffled_mc10,
            parent = TABLE_OUTPUT,
            name = "MC10 Home Sensor Measurements",
            cols = create_cols(MC10_SENSOR_NAME))
    shuffled_smartwatch_table = store_dataframe_to_synapse(
            syn,
            df = shuffled_smartwatch,
            parent = TABLE_OUTPUT,
            name = "Smartwatch Home Sensor Measurements",
            cols = create_cols(SMARTWATCH_SENSOR_NAME))
    scores_table = store_dataframe_to_synapse(
            syn,
            df = scores,
            parent = TABLE_OUTPUT,
            name = "Motor Task Home Timestamps and Scores",
            cols = create_cols("scores", syn=syn))


if __name__ == "__main__":
    main()
