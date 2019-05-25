'''
Tidy and push table `SCORES` to Synapse as a Table object, but with additional
columns `sensor` for sensor type, `location` for sensor location, and
`sensor_data` containing the sensor measurements which correspond to the task
being performed.
'''

import argparse
import multiprocessing
import synapseclient as sc
import synapseutils as su
import pandas as pd

SCORES = "syn18435302"
MC_10_MEASUREMENTS = "syn18822536" #"syn18435632"
SMARTWATCH_MEASUREMENTS = "syn18822537" #"syn18435623"
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


def download_children(syn, parent, download_in_parallel=False):
    _, _, entity_info = list(su.walk(syn, parent))[0]
    if download_in_parallel:
        synapse_ids = [j for i, j in entity_info]
        mp = multiprocessing.dummy.Pool(4)
        entities = mp.map(syn.get, synapse_ids)
        children = {entity_info[i][0]: entities[i] for i in range(len(entities))}
    else:
        children = {i: syn.get(j) for i, j in entity_info}
    return children


def slice_from_score(sensor_measurement, score):
    result = {}
    start, stop = score.loc["start_utc"], score.loc["stop_utc"]
    relevant_range = sensor_measurement[start:stop]
    for location in relevant_range.Location.unique():
        local_range = relevant_range.query("Location == @location")
        local_range = local_range.drop(["SubjID", "Location"], axis = 1)
        local_range = local_range.reset_index(drop=False)
        time_zero = local_range.Timestamp.iloc[0]
        local_range.Timestamp = local_range.Timestamp - time_zero
        location = "_".join(location.split())
        result[location] = local_range
    return result


def slice_sensor_measurement(f, scores):
    sensor_measurement = pd.read_csv(f.path)
    sensor_measurement.Timestamp = pd.to_datetime(sensor_measurement.Timestamp)
    sensor_measurement.set_index("Timestamp", drop = True, inplace=True)
    sensor_measurement.sort_index(inplace=True)
    relevant_scores = filter_relevant_scores(sensor_measurement, scores)
    results = relevant_scores.apply(
            lambda score : slice_from_score(sensor_measurement, score),
            axis = 1)
    return(results)


def slice_files(files, sensor):
    for fname, f in files.items():



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
    # TODO: only download relevant files (those present in scores)
    #       during download_children by matching subject_id values
    #       in filename. Copy some sample files over to test folders
    #       for testing
    args = read_args()
    syn = sc.login()
    scores = clean_scores(read_syn_table(syn, SCORES))
    smartwatch_files = download_children(syn, SMARTWATCH_MEASUREMENTS,
                                         args.download_in_parallel)
    mc10_files = download_children(syn, MC_10_MEASUREMENTS,
                                      args.download_in_parallel)


if __name__ == "__main__":
    main()
