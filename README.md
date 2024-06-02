# MetricsHarvester

## Generate BTC dataset. Timeframe: 2017 - 2024. Interval: 15 minutes.
```bash 
python data_ingester.py # will create bitcoin_info.csv
```

## Generate statistics based on BTC dataset.
```bash
1. python script/compute_simple_stats.py -cmd count_candles_by_color -i data/bitcoin_2017_2024.csv -o stats_results/count_candles_by_color_results.txt

2. python script/compute_simple_stat.py -cmd group_candles_by_percentage_change -i data/bitcoin_2017_2024.csv -o stats_results/percentage_movement_bucket_size_2.txt -bz 2

3.  python ./statistics_retrievar/similar_candles_in_a_row.py > results.txt # requirest ./data/bitcoin_info.csv
```
