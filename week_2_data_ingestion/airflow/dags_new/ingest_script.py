#!/usr/bin/env python
# coding: utf-8
import os 

import subprocess
import pyarrow
from time import time


import pandas as pd

from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file):

   
    #parquet_name ='output.parquet'
    parquet_name =csv_file
    csv_name ='output.csv'
    #print(port,password,user,host,db)
    #print(type(port))
    print(table_name,csv_file)

    #engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #engine.connect()
    #print('Connection established successfully')


    # # download the csv
    # proc = subprocess.run([
    #     "wget",
    #     url,
    #     "-O",
    #     parquet_name,
    # ])
    # assert proc.returncode == 0, (url, proc.returncode)

    # download the dataset
    
    #os.system(f"wget {url} -O {parquet_name}")

    #parquet_name = 'yellow_tripdata_2021-01.parquet'






    df = pd.read_parquet(parquet_name)
    # #csv_output = 'yellow_tripdata_2021-01.csv'
    df.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print('Connection established successfully')

    t_start = time()
    df_iter = pd.read_csv(csv_name,iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    del df['airport_fee']


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted the first chunk..., took %.3f second' % (t_end - t_start))

    while True:
        t_start = time()
        
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        del df['airport_fee']
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))





    



