"""TimeSeries generator class"""

import csv
import tempfile
from pathlib import Path
import pandas as pd


class TimeseriesGenerator:
    """[summary]
    Time Series Generator to produce time-series data for testing. It contains
    functions to perform various manipulation on the time series data.
    """

    def __init__(self) -> None:
        pass

    def split_time_series_by_features(
        self,
        input_file_path,
        features,
        delimiter=",",
        output_location=tempfile.mkdtemp(),
    ):
        """Split time series data on the features.

        Arguments:
            input_file_path (mandatory): location of the time series file.
            features ([str], mandatory): List of features to split time series.
            delimiter ([str], optional): input file delimiter. Defaults to ",".
            output_location ([str], optional): output directory to store split
                                               dataframes.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        Path(output_location).mkdir(parents=True, exist_ok=True)
        ts_df = pd.read_csv(input_file_path, delimiter=delimiter)
        for feature in features:
            drop_list = features.copy()
            drop_list.remove(feature)
            sub_df = ts_df.drop(labels=drop_list, axis=1)
            output_file_extension = Path(input_file_path).suffix
            sub_df.to_csv(
                path_or_buf=output_location
                + "/ds_"
                + feature.replace(" ", "_")
                + output_file_extension,
                sep=delimiter,
                quoting=csv.QUOTE_NONE,
                index=False,
            )

        print("files saved to location: " + output_location)
