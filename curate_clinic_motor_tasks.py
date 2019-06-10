'''
Tidy and push table `SCORES` to Synapse as a Table object, but with additional
columns `device` for device, `sensor_type` for sensor type, `sensor_location`
for sensor location, and `sensor_data` containing the sensor measurements which
correspond to the task being performed.
'''

import os
import uuid
import argparse
import tempfile
import multiprocessing
import synapseclient as sc
import synapseutils as su
import pandas as pd

SCORES = "syn18435302"
MC10_MEASUREMENTS = "syn18822536" #"syn18435632"
SMARTWATCH_MEASUREMENTS = "syn18822537" #"syn18435623"
SMARTWATCH_SENSOR_NAME = "smartwatch"
MC10_SENSOR_NAME = "mc10"
MC10_DATA_OUTPUT = "syn18879920" # "syn18879914"
SMARTWATCH_DATA_OUTPUT = "syn18879977" # "syn18879976"
TABLE_OUTPUT = "syn18407520"
TASK_CODE_MAP = { # synchronize with MJFF Levodopa release
        "Drnkg": "drnkg",
        "Drwg": "drwg",
        "Fldg": "fldng",
        "FtnL": "ftnl",
        "FtnR": "ftnr",
        "NtsBts": "ntblt",
        "RamL": "raml",
        "RamR": "ramr",
        "Sheets": "orgpa",
        "Sitng": "sittg",
        "SitStand": "ststd",
        "Stndg": "stndg",
        "Typg": "typng",
        "Wlkg": "wlkgs",
        "WlkgCnt": "wlkgc"}


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


def find_relevant_scores(fname, scores, sensor):
    def is_match(t, year, month, f_subject_id, score_subject_id):
        if int(f_subject_id) == int(score_subject_id):
            if year is None or month is None:
                return True
            elif isinstance(year, int) and isinstance(month, int):
                return year == t.year and month == t.month
            else:
                raise TypeError("Year and month must both be integers.")
        else:
            return False
    subject_id, year, month = parse_info_from_filename(fname, sensor)
    relevent_scores = scores.apply(
            lambda s : is_match(s.start_utc, year, month, subject_id, s.subject_id),
            axis = 1)
    return scores[relevent_scores]

def get_measurement_type(fname):
    if fname.startswith("Table9A") or fname.startswith("Table8"):
        return "accelerometer"
    elif fname.startswith("Table9B"):
        return "gyroscope"
    elif fname.startwith("Table9C"):
        return "emg"
    else:
        return None

def new_data_column(syn, scores, col, parent, filtering_prefix, sensor,
                    download_in_parallel):
    relevant_entities = download_relevant_children(
            syn, parent, filtering_prefix, scores, sensor, download_in_parallel)
    all_sliced_measurements = pd.DataFrame()
    for syn_id in relevant_entities:
        f, task_ids = (relevant_entities[syn_id]["synapse_file"],
                       relevant_entities[syn_id]["task_ids"])
        sliced_measurements = slice_sensor_measurement(f, scores, task_ids, sensor)
        sliced_measurements = sliced_measurements.rename(
                {"sensor_data": col}, axis = 1)
        relevant_entities[syn_id]["data"] = sliced_measurements
    if len(relevant_entities):
        all_sliced_measurements = pd.concat(
                [relevant_entities[syn_id]["data"] for syn_id in relevant_entities],
                axis=0)
    return(all_sliced_measurements)


def download_relevant_children(syn, parent, filtering_prefix, scores, sensor,
                               download_in_parallel=False):
    """
    Returns
    -------
    dict with key synapse_id (str) and values synapse_file (File), task_ids (list)
    """
    _, _, entity_info = list(su.walk(syn, parent))[0]
    entity_info = [(i, j) for i, j in entity_info if i.startswith(filtering_prefix)]
    relevant_entities = {}
    for fname, syn_id in entity_info:
        relevant_scores = find_relevant_scores(fname, scores, sensor)
        if len(relevant_scores):
            relevant_entities[syn_id] = {"synapse_file": None,
                                         "task_ids": relevant_scores.task_id}
    ordered_synapse_ids = list(relevant_entities.keys())
    if download_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        children = mp.map(syn.get, ordered_synapse_ids)
    else:
        children = list(map(syn.get, ordered_synapse_ids))
    for syn_id, f in zip(ordered_synapse_ids, children):
        relevant_entities[syn_id]["synapse_file"] = f
    return relevant_entities


def slice_from_score(sensor_measurement, score, sensor):
    """
    Returns
    -------
    a pandas DataFrame with columns location and sensor_data
    """
    result = pd.DataFrame(columns = ["sensor_location", "sensor_data"])
    start, stop = score.loc["start_utc"], score.loc["stop_utc"]
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
        result = result.append({"sensor_location": None,
                                "sensor_data": local_range},
                               ignore_index=True)
    result.index = pd.Index([score.name] * len(result))
    return result


def slice_sensor_measurement(f, scores, relevant_task_ids, sensor):
    sensor_measurement = pd.read_csv(f.path)
    sensor_measurement.Timestamp = pd.to_datetime(sensor_measurement.Timestamp)
    sensor_measurement.set_index("Timestamp", drop = True, inplace=True)
    sensor_measurement.sort_index(inplace=True)
    relevant_scores = scores.loc[relevant_task_ids,["start_utc","stop_utc"]]
    measurements = relevant_scores.apply(
            lambda score : slice_from_score(sensor_measurement, score, sensor),
            axis = 1)
    measurements = pd.concat(measurements.values, axis=0)
    return(measurements)


def replace_cols_with_filehandles(syn, df, cols, parent, upload_in_parallel):
    if upload_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        for col in cols:
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
        syn_f = sc.File(f.name, parent=parent, contentType="text/csv")
        syn_f = syn.store(syn_f)
        f.close()
        return syn_f["dataFileHandleId"]
    else:
        return ""


def slice_files(score_rows, files):
    for synapse_id, f in files.items():
        slices = slice_sensor_measurement(f, score_rows[synapse_id])
    return slices


def filter_relevant_scores(sensor_measurement, scores):
    start_time = sensor_measurement.index[0]
    stop_time = sensor_measurement.index[-1]
    subject_id = sensor_measurement.SubjID.iloc[0]
    relevant_scores = scores.query(
            "subject_id == @subject_id and "
            "start_utc >= @start_time and "
            "stop_utc <= @stop_time")
    return relevant_scores


def clean_scores(scores):
    # TODO: What to do with column `Side` and `Validated`?
    scores = scores.rename({"SubjID": "subject_id",
                            "Visit": "visit",
                            "Task": "task",
                            "TaskAbb": "task_code",
                            "Start Timestamp (UTC)": "start_utc",
                            "Stop Timestamp (UTC)": "stop_utc",
                            "Tremor - Left": "tremor_left",
                            "Tremor - Right": "tremor_right",
                            "Bradykinesia - Left": "bradykinesia_left",
                            "Bradykinesia - Right": "bradykinesia_right",
                            "Dyskinesia - Left": "dyskinesia_left",
                            "Dyskinesia - Right": "dyskinesia_right",
                            "Overall": "overall",
                            "Validated": "validated",
                            "Side": "side"}, axis = 1)
    scores_subset = scores.loc[:,["subject_id", "start_utc", "stop_utc",
                                  "tremor_left", "tremor_right",
                                  "bradykinesia_left", "bradykinesia_right",
                                  "dyskinesia_left", "dyskinesia_right"]]
    # TODO: further process once we determine what column `Side` is for
    scores_subset = pd.melt(
            scores_subset,
            id_vars = ["subject_id", "start_utc", "stop_utc"],
            value_vars = ["tremor_left", "tremor_right",
                          "bradykinesia_left", "bradykinesia_right",
                          "dyskinesia_left", "dyskinesia_right"],
            value_name = "score")
    scores.task_code = scores.task_code.map(TASK_CODE_MAP)
    scores.start_utc = pd.to_datetime(scores.start_utc)
    scores.stop_utc = pd.to_datetime(scores.stop_utc)
    invalid_scores = scores[(pd.isnull(scores.start_utc)) | (pd.isnull(scores.stop_utc))]
    scores = scores.drop(invalid_scores.index)
    task_ids = [uuid.uuid4() for i in range(len(scores))]
    scores["task_id"] = task_ids
    scores = scores.set_index("task_id", drop = False)
    return scores


def store_dataframe_to_synapse(df, parent, name, cols):
    schema = sc.Schema(name = name, columns = cols, parent = parent)
    table = sc.Table(schema, df)
    table = syn.store(table)
    return table


def main():
    args = read_args()
    syn = sc.login()
    scores = clean_scores(read_syn_table(syn, SCORES))

    mc10_accelerometer = new_data_column(
            syn,
            scores = scores,
            col = "mc10_accelerometer",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9A",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    mc10_gyroscope = new_data_column(
            syn,
            scores = scores,
            col = "mc10_gyroscope",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9B",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    mc10_emg = new_data_column(
            syn,
            scores = scores,
            col = "mc10_emg",
            parent = MC10_MEASUREMENTS,
            filtering_prefix = "Table9C",
            sensor = "mc10",
            download_in_parallel = args.download_in_parallel)
    smartwatch_accelerometer = new_data_column(
            syn,
            scores = scores,
            col = "smartwatch_accelerometer",
            parent = SMARTWATCH_MEASUREMENTS,
            filtering_prefix = "Table8",
            sensor = "smartwatch",
            download_in_parallel = args.download_in_parallel)

    mc10_accelerometer = mc10_accelerometer.reset_index(drop=False).rename(
            {"index": "task_id"})
    mc10_gyroscope = mc10_gyroscope.reset_index(drop=False).rename(
            {"index": "task_id"})
    mc10_emg = mc10_emg.reset_index(drop=False).rename(
            {"index": "task_id"})
    smartwatch_accelerometer = smartwatch_accelerometer.reset_index(drop=False).rename(
            {"index": "task_id"})

    merged_mc10 = pd.DataFrame()
    if len(mc10_accelerometer) and len(mc10_gyroscope):
        merged_mc10 = mc10_accelerometer.merge(mc10_gyroscope, how="outer")
    if len(merged_mc10) and len(mc10_emg):
        merged_mc10 = merged_mc10.merge(mc10_emg, how="outer")

    shuffled_mc10 = merged_mc10.sample(frac=1)
    shuffled_smartwatch = smartwatch_accelerometer.sample(frac=1)

    replace_cols_with_filehandles( # replaces in-place
            syn,
            df = shuffled_mc10,
            cols = ["mc10_accelerometer", "mc10_gyroscope", "mc10_emg"],
            parent = MC10_DATA_OUTPUT,
            upload_in_parallel = args.upload_in_parallel)
    replace_cols_with_filehandles( # replaces in-place
            syn,
            df = shuffled_smartwatch,
            cols = ["smartwatch_accelerometer"],
            parent = SMARTWATCH_DATA_OUTPUT,
            upload_in_parallel = args.upload_in_parallel)

    # shuffled_mc10 =
    #shuffled_smartwatch = store_dataframe_to_synapse

    #store_dataframe_to_synapse(


    #smartwatch_rows, smartwatch_files = download_relevant_children(
    #        syn, SMARTWATCH_MEASUREMENTS, scores,
    #        args.download_in_parallel, sensor = SMARTWATCH_SENSOR_NAME)
    #mc10_rows, mc10_files = download_relevant_children(
    #        syn, MC_10_MEASUREMENTS, scores,
    #        args.download_in_parallel, sensor = MC10_SENSOR_NAME)
    #sliced_smartwatch_rows = slice_files(smartwatch_rows, smartwatch_files)
    #sliced_mc10_rows = slice_files(mc10_rows, mc10_files)


if __name__ == "__main__":
    main()
