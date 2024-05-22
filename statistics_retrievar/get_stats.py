import argparse
import sys
from typing import Dict

from data_analyser import DataAnalyser

SUPPORTED_COMMANDS_LIST = [
    "count_candles_by_color", 
    "group_candles_by_percentage_change"
]

def help() -> None:
    print(f"""{sys.argv[0]} usage:
1. command: python {sys.argv[0]} -cmd count_candles_by_color -i infile.csv -o outfile
2. command: python {sys.argv[0]} -cmd group_candles_by_percentage_change -i infile.csv -bz 2 -o outfile """)
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
    
    analyser = DataAnalyser(args.input)
    analyser.lazyload_data()
    
    with open(args.output, 'w+') as f:
     
        if args.command == "count_candles_by_color":
            
            green_candles_count, red_candles_count, gray_candles_count = analyser.count_candles_by_color()
            f.write(f"Green candles: {green_candles_count}.\n")
            f.write(f"Red candles: {red_candles_count}.\n")
            f.write(f"No-change candles: {gray_candles_count}.\n")
            
        elif args.command == "group_candles_by_percentage_change":
            
            candles_grouping: Dict[int, int] = analyser.group_candles_by_percentage_change(args.bucket_size)
            for key in sorted(candles_grouping.keys()):
                interval_info = f"[{key}% -> {key + args.bucket_size - 0.01}%]"
                # Zero values will be skipped when writing the results.   
                if candles_grouping.get(key) != 0:
                    f.write(f"Interval {interval_info} has {candles_grouping.get(key)} samples.\n")
    
        else:
            print(f"Wrong command! Supported commands are: {SUPPORTED_COMMANDS_LIST}.")
     
        
if __name__ == "__main__":
    main()
