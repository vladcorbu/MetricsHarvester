import pandas as pd
import argparse

def count_price_actions(file_path):
    data = pd.read_csv(file_path)

    go_up = 0
    go_down = 0
    no_change = 0

    for _, row in data.iterrows():
        if row['Close'] > row['Open']:
            go_up += 1
        elif row['Close'] < row['Open']:
            go_down += 1
        else:
            no_change += 1
            

    return go_up, go_down, no_change

def main():
    parser = argparse.ArgumentParser(description='Count price actions based on Open and Close prices in a CSV file.')
    parser.add_argument('-i', '--input', required=True, help='Path to the CSV file')
    
    args = parser.parse_args()
    
    go_up, go_down, no_change = count_price_actions(args.input)

    # Print the results
    print("Price went up:", go_up, "times")
    print("Price went down:", go_down, "times")
    print("Price remained the same:", no_change, "times")

if __name__ == "__main__":
    main()
