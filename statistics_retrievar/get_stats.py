import argparse
import sys
from typing import Dict, Tuple

import pandas as pd

SUPPORTED_COMMANDS_LIST = [
    "count_candles_by_color", 
    "group_candles_by_percentage_movement"
]
def count_candles_by_color(file_path: str) -> Tuple[int, int, int]:
    data = pd.read_csv(file_path)

    green_candles_count = 0
    red_candles_count = 0
    no_change_candles_count = 0

    for _, row in data.iterrows():
        if row['Close'] > row['Open']:
            green_candles_count += 1
        elif row['Close'] < row['Open']:
            red_candles_count += 1
        else:
            no_change_candles_count += 1
            
    return green_candles_count, red_candles_count, no_change_candles_count


def getPercentageMovement(open_price: float, close_price: float) -> float:
    return close_price / open_price - 1

def create_dictionary(step: int) -> Dict[int, int]:
    keys = list(range(0, 100, step)) + list(range(0, -100, -step))
    my_dict = {key: 0 for key in keys}
    return my_dict

def group_candles_by_percentage_movement(file_path: str, interval_length: int) -> Dict[int, int]:
    
    data = pd.read_csv(file_path)
    d = create_dictionary(interval_length)

    for _, row in data.iterrows():
        percentage_change = getPercentageMovement(
            open_price=float(row['Open']),
            close_price=float(row['Close'])
        )

        targetKey = int(percentage_change * 100 / interval_length) * interval_length
        if percentage_change < 0:
            targetKey = targetKey -interval_length

        d[targetKey] = d.get(targetKey) + 1

    return d

def help():
    print(f"""{sys.argv[0]} usage:
1. command: python {sys.argv[0]} -cmd count_candles_by_color -i infile.csv -o outfile
2. command: python {sys.argv[0]} -cmd group_candles_by_percentage_movement -i infile.csv -bz 2 -o outfile """)
    exit(1)

def main():
    
    if len(sys.argv) == 1:
        help()
    
    parser = argparse.ArgumentParser(description='Get statistics based on Open and Close prices in a CSV file.')
    parser.add_argument('-i', '--input', type=str, required=True, help='Path to the input CSV file.')
    parser.add_argument('-o', '--output', type=str, required=True, help='Path to output file.')
    parser.add_argument('-cmd', '--command', type=str, required=True, help=f'Action to be performed.\
                        Supported actions are: {" ".join([f"{index + 1}.{item}" for index, item in enumerate(SUPPORTED_COMMANDS_LIST)])}')
    parser.add_argument('-bz', '--bucket_size', type=int, required=False, help='Optional. Bucket size used when grouping candles.')
    
    args = parser.parse_args()
    
    cmd = args.command
    infile = args.input
    outfile = args.output
    
    with open(outfile, 'w+') as f:
     
        if cmd == "count_candles_by_color":
            
            green_candles_count, red_candles_count, no_change_candles_count = count_candles_by_color(infile)
            f.write(f"Green candles: {green_candles_count}.\n")
            f.write(f"Red candles: {red_candles_count}.\n")
            f.write(f"No-change candles: {no_change_candles_count}.\n")
            
        elif cmd == "group_candles_by_percentage_movement":
            
            bucket_size = args.bucket_size
            if bucket_size <= 0 or bucket_size >= 100:
                print("Bucket Size must be an integer between 0 and 100.")
            percentage_d: Dict[int, int] = group_candles_by_percentage_movement(infile, bucket_size)
            for key in sorted(percentage_d.keys()):
                interval_info = f"[{key}% -> {key+bucket_size-0.01}%]"
                if percentage_d.get(key) != 0:
                    f.write(f"Interval {interval_info} has {percentage_d.get(key)} samples.\n")
    
        else:
            print(f"Wrong command! Supported commands are: {SUPPORTED_COMMANDS_LIST}.")
     
        
if __name__ == "__main__":
    main()
