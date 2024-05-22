import os

from data_analyser import DataAnalyser


def main():
    analyser = DataAnalyser(f"./data/bitcoin_info.csv")
    analyser.lazyload_data()

    my_dict = {}
    maxi = 20
    for i in range (2,maxi):
        my_dict[i] = analyser.count_consecutive_candles(i, "green")
    
    print("GREEN dict: ", my_dict)
    for i in range(2,maxi-1):
        print(f"consecutive {i+1} / consecutive {i}: {my_dict[i+1]/my_dict[i]}")
        
    for i in range (2,maxi):
        my_dict[i] = analyser.count_consecutive_candles(i, "red")
    
    print("RED dict: ", my_dict)
    for i in range(2,maxi-1):
        print(f"consecutive {i+1} / consecutive {i}: {my_dict[i+1]/my_dict[i]}")
        

if __name__ == "__main__":
    main()