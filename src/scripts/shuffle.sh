(head -n 1 ../../data/raw/2015_Green_Taxi_Trip_Data.csv && tail -n +2 ../../data/raw/2015_Green_Taxi_Trip_Data.csv | shuf -n 5000000) > ../../data/processed/shuffled.csv
