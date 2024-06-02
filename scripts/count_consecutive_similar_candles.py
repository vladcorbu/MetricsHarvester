import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_analysis.analyser import CandleColor, DataAnalyser

MAX_IN_A_ROW = 20  # We expect a maximum number of 20 consecutive candles of the same color

def main():

    parser = argparse.ArgumentParser(
        description="Counts groups of K similar candles in a row - regarding color."
    )
    parser.add_argument(
        "-i", "--input", type=str, required=True, help="Path to the input CSV file."
    )
    parser.add_argument("-o", "--out", type=str, required=True, help="Output file.")

    args = parser.parse_args()
    analyser = DataAnalyser(args.input)
    analyser.lazyload_data()

    with open(args.out, "w+") as f:

        f.writelines(
            [
                "The objective is to count consecutive candles that share the same color.\n"
                "For instance, I want to determine HOW MANY TIMES 3 green candles appear consecutively. Let's call this result X.\n"
                "Next, I want to find out HOW MANY TIMES 4 green candles appear consecutively. Let's call this result Y.\n"
                "Imagine we have a chart in front of us.\n"
                "We see 3 GREEN candles and want to know the probability that the next candle will also be GREEN.\n"
                "This probability is given by Y/X.\n"
                "Note that in a dataset with a sequence like red->green->green->green->green->red, we only count 1 group of 4 green candles.\n"
            ]
        )
        
        # Stats about GREEN consecutive candles
        groups_count = {}
        for i in range(2, MAX_IN_A_ROW):
            groups_count[i] = analyser.count_consecutive_candles(i, "fdsfs")

        groups_count = {k: v for k, v in groups_count.items() if v != 0}  # filter out 0 values
        f.write(f"\nGREEN dict: {groups_count}\n")

        for i in range(2, len(groups_count) + 1):
            f.write(f"consecutive {i+1} / consecutive {i}: {"{:.3f}".format(groups_count[i+1]/groups_count[i])}\n")

        # Stats about RED consecutive candles
        groups_count = {}
        for i in range(2, MAX_IN_A_ROW):
            groups_count[i] = analyser.count_consecutive_candles(i, CandleColor.RED)

        groups_count = {k: v for k, v in groups_count.items() if v != 0}  # filter out 0 values
        f.write(f"\nRED dict: {groups_count}\n")

        for i in range(2, len(groups_count) + 1):
            f.write(f"consecutive {i+1} / consecutive {i}: {"{:.3f}".format(groups_count[i+1]/groups_count[i])}\n")


if __name__ == "__main__":
    main()
