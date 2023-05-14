#!/usr/bin/env python
# coding: utf-8
import os 
import argparse
import subprocess
import pyarrow
from time import time


import pandas as pd

from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db =params.db
    table_name = params.table_name
    url = params.url
    
    parquet_name ='output.parquet'
    csv_name ='output.csv'

    # # download the csv
    # proc = subprocess.run([
    #     "wget",
    #     url,
    #     "-O",
    #     parquet_name,
    # ])
    # assert proc.returncode == 0, (url, proc.returncode)

    # download the dataset
    
    os.system(f"wget {url} -O {parquet_name}")

    #parquet_name = 'yellow_tripdata_2021-01.parquet'
    df = pd.read_parquet(parquet_name)
    # #csv_output = 'yellow_tripdata_2021-01.csv'
    df.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name,iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    #del df['airport_fee']


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        
        
        df = next(df_iter)
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        #del df['airport_fee']
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')


    parser.add_argument("--user",help='user name for postgres')
    parser.add_argument("--password",help='password for postgres')
    parser.add_argument("--host",help='host for postgres')
    parser.add_argument("--port",help='port for postgres')
    parser.add_argument("--db",help='database name for postgres')
    parser.add_argument("--table_name",help='name of the table where we will write the results to')
    parser.add_argument("--url",help='url of the csv file')


    args = parser.parse_args()

    main(args)




    


