import os
from typing import Dict, Tuple

import pandas as pd


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
                raise Exception(f"{os.path.splitext(data_file)[1]} extension not supported.")

        self.data_file = data_file
        self.data = None

   
    def lazyload_data(self) -> None:
        """Lazy loads data stored in the input file."""
        if self.data == None:
            self.data = pd.read_csv(self.data_file)


    def count_candles_by_color(self) -> Tuple[int, int, int]:
        """Count green, red and gray candles from the data file.
        A green candle is a candle that has Open price < Close price.
        A red candle is a candle that has Open price > Close price.
        A gray candle is candle that has Open price == Close price. 

        Returns:
            Tuple[int, int, int]: (green_candles_count, red_candles_count, gray_candles_count)
        """

        green_candles_count = 0
        red_candles_count = 0
        gray_candles_count = 0
        
        for _, row in self.data.iterrows():
            if row[self.CLOSE_PRICE_LABEL] > row[self.OPEN_PRICE_LABEL]:
                green_candles_count += 1
            elif row[self.CLOSE_PRICE_LABEL] < row[self.OPEN_PRICE_LABEL]:
                red_candles_count += 1
            else:
                gray_candles_count += 1
                
        return green_candles_count, red_candles_count, gray_candles_count
    
    def group_candles_by_percentage_change(self, bucket_size: int) -> Dict[int, int]:
        """
        Groups candles in buckets based on their percentage change and bucket size.
        If bucket size is 5, we are going to create the following keys in the dictionary: 0,+-5,+-10,+-15,...+-95.\n
        If the price raised with 7%, we are going to place that candle in dict[5].\n
        If the price raised with 16%, we are going to place the candle in dict[15].\n
        If the price felt with -11%, we are going to place the candle in dict[-15].\n
        We compute the dictionary key as following:
            - K% > 0 => key = int(K * 100 / bucket_size) * bucket_size
            - K% < 0 => key = int(K * 100 / bucket_size - 1) * bucket_size

        Args:
            bucket_size (int): bucket size.

        Returns:
            Dict[int, int]: Dictionary that counts how many candles are in each bucket.Note that bucket 10 is identified \
                by dict[10] and stores the number of candles that had a raised between 10% and 15% (if bucket_size is 5).
        """
        if bucket_size <= 0 or bucket_size >= 100:
            raise Exception("Bucket Size must be an integer between 0 and 100.")
        
        buckets_dict = create_buckets(bucket_size)

        for _, row in self.data.iterrows():
            change = compute_percentage_change(
                open_price=float(row[self.OPEN_PRICE_LABEL]),
                close_price=float(row[self.CLOSE_PRICE_LABEL])
            )

            targetKey = int(change * 100 / bucket_size) * bucket_size
            if change < 0:
                targetKey = targetKey - bucket_size

            buckets_dict[targetKey] += 1

        return buckets_dict


def compute_percentage_change(open_price: float, close_price: float) -> float:
    """Return the percentage movement of candle given the open and close prices.

    Args:
        open_price (float): start price
        close_price (float): end price

    Returns:
        float: percentage movement (it can be negative if open_price > close_price)
    """
    return close_price / open_price - 1


def create_buckets(bucket_size: int) -> Dict[int, int]:
    """Creates dictionary entries with values between -100 and 100 based on bucket_size.
    Example: for bucket_size = 3 we are going to create the following entries: 0,3,6,9,...99,-3,-6,-9,...-99.

    Args:
        bucket_size (int): bucket size.

    Returns:
        Dict[int, int]: dictionay with computed keys that have their values equal to 0.
    """
    keys = list(range(0, 100, bucket_size)) + list(range(0, -100, -bucket_size))
    my_dict = {key: 0 for key in keys}
    return my_dict
