import os
from typing import Dict, NamedTuple

import pandas as pd


class CandleColor(NamedTuple):
    GREEN: str = "green"
    GREY: str = "grey"
    RED: str = "red"


class DataAnalyser:
    INPUT_FILE_EXTENSION = ".csv"
    OPEN_PRICE_LABEL = "Open"
    CLOSE_PRICE_LABEL = "Close"

    def __init__(self, data_file: str) -> None:
        """Initializes the DataAnalyzer class with a path to a data file.

        Args:
            data_file (str): input data file in CSV format
        """
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"File {data_file} does not exists on disk.")
        else:
            if not os.path.splitext(data_file)[1] == self.INPUT_FILE_EXTENSION:
                raise Exception(
                    f"{os.path.splitext(data_file)[1]} extension not supported."
                )

        self.data_file = data_file
        self.data: pd.DataFrame = None

    def lazyload_data(self) -> None:
        """Lazy loads data stored in the input file."""
        if self.data == None:
            self.data = pd.read_csv(self.data_file)

    def count_candles_by_color(self) -> Dict[CandleColor, int]:
        """Counts how many green/red/gray candles are in the input file.
        - Open price < Close price => GREEN
        - Open price > Close price => RED
        - Open price == Close price => GREY

        Returns:
            Dict[CandleColor, int]: stores the number of similar candles from the dataset
        """

        candle_color_d: Dict[CandleColor, int] = {
            CandleColor.GREEN: 0,
            CandleColor.RED: 0,
            CandleColor.GREY: 0,
        }

        for _, row in self.data.iterrows():
            if row[self.CLOSE_PRICE_LABEL] > row[self.OPEN_PRICE_LABEL]:
                candle_color_d[CandleColor.GREEN] += 1
            elif row[self.CLOSE_PRICE_LABEL] < row[self.OPEN_PRICE_LABEL]:
                candle_color_d[CandleColor.RED] += 1
            else:
                candle_color_d[CandleColor.GREY] += 1

        return candle_color_d

    def count_consecutive_candles(self, group_size: int, color: CandleColor) -> int:
        """Calculates the total number of candle groups that share the same color.

        Note that 4 consecutive green candles means the following sequence:
        - red -> green -> green -> green -> green -> red

        If group_size==3, the sequence from above is not valid, thus won't be counted.

        Args:
            group_size (int): group size
            color (CandleColor): candle color

        Raises:
            ValueError: _description_

        Returns:
            int: total number of candle groups of size "group_size" that share the same "color"
        """

        df = self.data
        if color == CandleColor.GREEN:
            df["diff"] = df[self.CLOSE_PRICE_LABEL] - df[self.OPEN_PRICE_LABEL] > 0
        elif color == CandleColor.RED:
            df["diff"] = df[self.CLOSE_PRICE_LABEL] - df[self.OPEN_PRICE_LABEL] < 0
        elif color == CandleColor.GREY:
            df["diff"] = df[self.CLOSE_PRICE_LABEL] - df[self.OPEN_PRICE_LABEL] == 0
        else:
            raise ValueError(
                f"Invalid color: {color}. Supported values are: {CandleColor._fields}."
            )

        valid_sequences = df["diff"].rolling(window=group_size).sum() == group_size

        # Since each sequence of GROUP_SIZE valid rows will have four 'True' values in a row, we only count the first occurrence
        # We can mark the first of each group by checking the previous value.
        first_of_sequence = valid_sequences & (
            ~valid_sequences.shift(1, fill_value=False)
        )

        # Sum up the True values in 'first_of_sequence' to get the total count of valid groups
        count_valid_groups = first_of_sequence.sum()
        return count_valid_groups

    def group_candles_by_percentage_change(self, bucket_size: int) -> Dict[int, int]:
        """
        Groups candles in buckets based on their percentage movement and bucket_size.
        If bucket_size==5, we are going to create the following keys in the dictionary: 0,+-5,+-10,...+-95.\n
        If the candles price has:
        - raised by 7%, we place it in dict[5].
        - raised by 16%, we place it in dict[15].
        - fallen by -11%, we place it in dict[-15].

        A "lower approximation" to the keys from dictionary is performed.

        Args:
            bucket_size (int): bucket size.

        Returns:
            Dict[int, int]: counts how many candles are in each bucket. Key 10 stores the number of candles
            that had a raised between 10% and 15% (if bucket_size==5).
        """
        if bucket_size <= 0 or bucket_size >= 100:
            raise Exception("Bucket_Size must be an integer between 0 and 100.")

        buckets_dict = {}

        for _, row in self.data.iterrows():
            change = compute_percentage_change(
                open_price=float(row[self.OPEN_PRICE_LABEL]),
                close_price=float(row[self.CLOSE_PRICE_LABEL]),
            )

            targetKey = int(change * 100 / bucket_size) * bucket_size
            if change < 0:
                targetKey = targetKey - bucket_size
            buckets_dict[targetKey] = buckets_dict.setdefault(targetKey, 0) + 1

        return buckets_dict


def compute_percentage_change(open_price: float, close_price: float) -> float:
    """Return the percentage movement of a candle given the open and close prices.

    Args:
        open_price (float): start price
        close_price (float): end price

    Returns:
        float: percentage movement (it can be negative if open_price > close_price)
    """
    return close_price / open_price - 1
