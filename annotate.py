import os
import settings
import pandas as pd


def count_performance_rows():
    """ () -> dict

    Return the number of times a particular loan_id shows up in Performance.txt and provide the foreclosure_status.
    """
    counts = {}
    with open(os.path.join(settings.PROCESSED_DIR, "Performance.txt"),
              'r') as f:
        for i, line in enumerate(f):
            if i == 0:
                # Skip header row
                continue
            loan_id, date = line.split('|')
            loan_id = int(loan_id)
            if loan_id not in counts:
                counts[loan_id] = {
                    "foreclosure_status": False,
                    "performance_count": 0
                }
            counts[loan_id]["performance_count"] += 1
            if len(date.strip()) > 0:
                counts[loan_id]["foreclosure_status"] = True

    return counts


def get_performance_summary_value(loan_id, key, counts):
    """ (int, str, dict) -> dict

    Return the foreclosure_status or performance_count for
    any particular loan_id
    """
    value = counts.get(loan_id, {
        "foreclosure_status": False,
        "performance_count": 0
    })
    return value[key]


def annotate(acquisition, counts):
    """ (DataFrame, dict) -> DataFrame

    Prepare the pandas DataFrame for input into a machine learning model. String values must be converted to numbers for it to be useful in the algorithm.
    """
    acquisition["foreclosure_status"] = acquisition["id"].apply(
        lambda x: get_performance_summary_value(x, "foreclosure_status", counts)
    )
    acquisition["performance_count"] = acquisition["id"].apply(
        lambda x: get_performance_summary_value(x, "performance_count", counts)
    )

    # Convert string columns into categories
    for column in [
            "channel", "seller", "first_time_homebuyer", "loan_purpose",
            "property_type", "occupancy_status", "property_state",
            "product_type", "relocation_indicator"
    ]:
        acquisition[column] = acquisition[column].astype('category').cat.codes

    # Convert dates into month/year columns
    for start in ["first_payment", "origination"]:
        column = "{}_date".format(start)
        acquisition["{}_month".format(start)] = pd.to_numeric(
            acquisition[column].str.split('/').str.get(0))
        acquisition["{}_year".format(start)] = pd.to_numeric(
            acquisition[column].str.split('/').str.get(1))
        del acquisition[column]

    # Fill missing values with -1 and delete rows with less than the minimum tracking quarters
    acquisition = acquisition.fillna(-1)
    acquisition = acquisition[
        acquisition["performance_count"] > settings.MINIMUM_TRACKING_QUARTERS]

    return acquisition


def read():
    """ () -> DataFrame
    Read in file to pandas DataFrame
    """
    acquisition = pd.read_csv(
        os.path.join(settings.PROCESSED_DIR, "Acquisition.txt"), sep="|")
    return acquisition


def write(acquisition):
    """ (DataFrame) -> NoneType
    Write contents from DataFrame into .csv file
    """
    acquisition.to_csv(
        os.path.join(settings.PROCESSED_DIR, "train.csv"), index=False)


if __name__ == "__main__":
    print("Beginning of annotate.py script...")
    acquisition = read()
    counts = count_performance_rows()
    acquisition = annotate(acquisition, counts)
    write(acquisition)
    print("Finished annotate.py script...")
