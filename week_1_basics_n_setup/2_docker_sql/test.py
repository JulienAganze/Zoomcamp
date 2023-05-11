import os 
import pandas as pd
parquet_name ='output.parquet'
csv_name ='output.csv'
url=" https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    # download the dataset
    
os.system(f"wget {url} -O {parquet_name}")

    #parquet_name = 'yellow_tripdata_2021-01.parquet'
df = pd.read_parquet(parquet_name)
    #csv_output = 'yellow_tripdata_2021-01.csv'
df.to_csv(csv_name, index=False)