'''
Tidy and push table `SCORES` to Synapse as a Table object, but with additional
columns `device` for device, `sensor_type` for sensor type, `sensor_location`
for sensor location, and `sensor_data` containing the sensor measurements which
correspond to the task being performed.
'''

import os
import argparse
import multiprocessing
import synapseclient as sc
import synapseutils as su
import pandas as pd

SCORES = "syn18435302"
MC10_MEASUREMENTS = "syn18822536" #"syn18435632"
SMARTWATCH_MEASUREMENTS = "syn18822537" #"syn18435623"
SMARTWATCH_SENSOR_NAME = "smartwatch"
MC10_SENSOR_NAME = "mc10"
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

def download_relevant_children(syn, parent, scores, sensor,
                               download_in_parallel=False):
    """
    Returns
    -------
    1. dataframe of mapping Synapse ID to relevant scores (pandas DataFrame)
    2. dictionary mapping synapse ID to Synapse File object
    """
    _, _, entity_info = list(su.walk(syn, parent))[0]
    relevant_score_rows = {}
    relevant_entities = []
    for fname, syn_id in entity_info:
        measurement_type = get_measurement_type(fname)
        relevant_scores = find_relevant_scores(fname, scores, sensor)
        if len(relevant_scores):
            relevant_scores["device"] = sensor
            relevant_scores["sensor_type"] = measurement_type
            relevant_score_rows[syn_id] = relevant_scores
            relevant_entities.append(syn_id)
    if download_in_parallel:
        mp = multiprocessing.dummy.Pool(4)
        children = mp.map(syn.get, relevant_entities)
    else:
        children = list(map(syn.get, relevant_entities))
    id_to_file_mapping = {k: v for k, v in zip(relevant_entities, children)}
    return relevant_score_rows, id_to_file_mapping


def slice_from_score(sensor_measurement, score):
    """
    Returns
    -------
    a dict mapping location of sensor to a pandas DataFrame with
    x, y, z measurements
    """
    result = pd.DataFrame(columns = ["sensor_location", "sensor_data"])
    start, stop = score.loc["start_utc"], score.loc["stop_utc"]
    relevant_range = sensor_measurement[start:stop]
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
    result.index = pd.Index([score.name] * len(result))
    return result


def slice_sensor_measurement(f, scores):
    sensor_measurement = pd.read_csv(f.path)
    sensor_measurement.Timestamp = pd.to_datetime(sensor_measurement.Timestamp)
    sensor_measurement.set_index("Timestamp", drop = True, inplace=True)
    sensor_measurement.sort_index(inplace=True)
    relevant_scores = filter_relevant_scores(sensor_measurement, scores)
    measurements = relevant_scores.apply(
            lambda score : slice_from_score(sensor_measurement, score),
            axis = 1)
    measurements = pd.concat(measurements.values, axis=0)
    merged_results = pd.merge(relevant_scores, measurements,
                              left_index=True, right_index=True)
    return(merged_results)

def replace_dataframe_with_filehandle(df):
    pass

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
    return scores


def main():
    args = read_args()
    syn = sc.login()
    scores = clean_scores(read_syn_table(syn, SCORES))
    smartwatch_rows, smartwatch_files = download_relevant_children(
            syn, SMARTWATCH_MEASUREMENTS, scores,
            args.download_in_parallel, sensor = SMARTWATCH_SENSOR_NAME)
    mc10_rows, mc10_files = download_relevant_children(
            syn, MC_10_MEASUREMENTS, scores,
            args.download_in_parallel, sensor = MC10_SENSOR_NAME)
    sliced_smartwatch_rows = slice_files(smartwatch_rows, smartwatch_files)
    sliced_mc10_rows = slice_files(mc10_rows, mc10_files)


if __name__ == "__main__":
    main()
