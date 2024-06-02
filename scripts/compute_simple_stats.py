import argparse
import os
import sys
from typing import Dict

# Ensure the project root is in the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_analysis.analyser import CandleColor, DataAnalyser

SUPPORTED_COMMANDS_LIST = [
    "count_by_color",
    "group_by_percentage_change",
]


def display_help() -> None:
    print(
        f"""{sys.argv[0]} usage:
1. command: python {sys.argv[0]} -cmd count_by_color -i infile.csv -o outfile
2. command: python {sys.argv[0]} -cmd group_by_percentage_change -i infile.csv -bz 2 -o outfile """
    )
    exit(1)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Get statistics based on Open and Close prices in a CSV file."
    )
    parser.add_argument(
        "-i", "--input", type=str, required=True, help="Path to the input CSV file."
    )
    parser.add_argument("-o", "--output", type=str, required=True, help="Path to the output file.")
    parser.add_argument(
        "-cmd",
        "--command",
        type=str,
        required=True,
        help=f"Supported commands: {SUPPORTED_COMMANDS_LIST}",
    )
    parser.add_argument(
        "-iz",
        "--interval_size",
        type=str,
        required=False,
        help="Only used with group_by_percentage_change command.",
    )
    return parser.parse_args()


def count_by_color(analyser: DataAnalyser, output_file: str):
    with open(output_file, "w+") as f:
        f.writelines(
            [
                "This file contains the frequency of green & red & doji candles in the dataset.\n",
                'In the context of trading, a candle that does not change the price is often called a "doji".\n\n',
            ]
        )
        colors = analyser.count_candles_by_color()
        f.write(f"Green candles: {colors[CandleColor.GREEN]}.\n")
        f.write(f"Red candles: {colors[CandleColor.RED]}.\n")
        f.write(f"Doji candles: {colors[CandleColor.GREY]}.\n")


def group_by_percentage_change(analyser: DataAnalyser, output_file: str, interval_size: int):
    with open(output_file, "w+") as f:

        candles_grouping = analyser.group_candles_by_percentage_change(interval_size)

        for lower_bound in sorted(candles_grouping.keys()):
            interval = f"[{lower_bound}% -> {lower_bound + interval_size - 0.01}%]"
            if candles_grouping.get(lower_bound) != 0:
                f.write(f"Interval {interval} has {candles_grouping.get(lower_bound)} samples!\n")


def main():

    if len(sys.argv) == 1:
        display_help()

    args = parse_arguments()

    try:
        analyser = DataAnalyser(args.input)
        analyser.lazyload_data()
    except Exception as e:
        print(f"Error: Cannot load data - {e}")
        exit(1)

    if args.command == "count_by_color":
        count_by_color(analyser, args.output)
    elif args.command == "group_candles_by_percentage_change":
        if args.interval_size is None:
            print("Error: interval_size is required for the command group_by_percentage_change.")
            display_help()
        group_by_percentage_change(analyser, args.output, args.interval_size)
    else:
        print(f"Wrong command! Supported commands are: {SUPPORTED_COMMANDS_LIST}.")
        display_help()


if __name__ == "__main__":
    main()
