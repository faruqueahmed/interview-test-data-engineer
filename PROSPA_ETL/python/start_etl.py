#!/usr/bin/env python
# coding: utf-8

 
import os
import time
import json
import pymysql
import datetime
import pandas as pd
from sqlalchemy import create_engine

### initialize params
with open(os.getcwd()+'/config/config.json') as config_file:
        params = json.load(config_file)
        stage_filedir=os.getcwd()+params['stage_filedir']  
        badfile_dir=os.getcwd()+params['badfile_dir']
        log_filedir=os.getcwd()+params['log_filedir']
        processed_filedir=os.getcwd()+params['processed_filedir']
print(stage_filedir)
processing_time=int(time.time()) 

supplier_columns=['S_SUPPKEY','S_NAME','S_ADDRESS' ,'S_NATIONKEY','S_PHONE' ,'S_ACCTBAL','S_COMMENT','FILENAME','INGEST_TIME']
orders_columns=['O_ORDERKEY','O_CUSTKEY','O_ORDERSTATUS','O_TOTALPRICE','O_ORDERDATE','O_ORDERPRIORITY','O_CLERK','O_SHIPPRIORITY','O_COMMENT','FILENAME','INGEST_TIME']
nation_columns=['N_NATIONKEY','N_NAME','N_REGIONKEY','N_COMMENT','FILENAME','INGEST_TIME']
region_columns=['R_REGIONKEY','R_NAME','R_COMMENT','FILENAME','INGEST_TIME']
part_columns=['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT','FILENAME','INGEST_TIME']
partsupp_columns=['PS_PARTKEY','PS_SUPPKEY','PS_AVAILQTY','PS_SUPPLYCOST','PS_COMMENT','FILENAME','INGEST_TIME']
customer_columns=['C_CUSTKEY','C_NAME','C_ADDRESS','C_NATIONKEY','C_PHONE','C_ACCTBAL','C_MKTSEGMENT','C_COMMENT','FILENAME','INGEST_TIME']
lineitem_columns=['L_ORDERKEY','L_PARTKEY','L_SUPPKEY','L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT','FILENAME','INGEST_TIME']

 
def get_db_connection():    
    with open(os.getcwd()+'/config/config.json') as config_file:
        params = json.load(config_file)
        stage_filedir=os.getcwd()+params['stage_filedir']  
        badfile_dir=os.getcwd()+params['badfile_dir']
        log_filedir=os.getcwd()+params['log_filedir']
        processed_filedir=os.getcwd()+params['processed_filedir']
        user = params['db_user']
        passw = params['dbpassword']  
        host =  params['hostname']
        port = params['port']
        database=params['databasename']
        conn_str='mysql+pymysql://'+user+':'+passw+'@'+host+':3306/'+database
        cnx = create_engine(conn_str, echo=False)
        return cnx
cnx=get_db_connection()    

 
def save_df(connection,df,table_name):
    df.to_sql(name=table_name, con=connection, if_exists = 'append', index=False )
    return
def archive_processed_data(stage_filedir,file):
    processed_file= stage_filedir+'/'+file
    print('processed_file :'+processed_file)
    archived_file=processed_filedir+'/'+str(processing_time)+'_'+file
    print('archived_file :'+archived_file)
    os.rename(processed_file, archived_file)

 
def load_CSV(stage_filedir,csv_file,cols):
    filepath=stage_filedir+'/'+csv_file
    df = pd.read_csv(filepath, sep='|' , header=None)
    df = df.drop(len (df.columns)-1, 1)
    df['filename']=csv_file             
    df['ingest_time']=processing_time
    df.columns = cols  
    #list(df.columns.values)
    print("Loading CSV to Dataframe:"+filepath)
    return df
 

def start_ETL():
    stage_filepath=stage_filedir
    file_list=os.listdir(stage_filepath)
    print('Start saving ......')
    print(stage_filepath)
    for file in file_list:
        if "customer" in file.lower():        
            save_df(cnx,load_CSV(stage_filedir,file,customer_columns),'CUSTOMER')
            archive_processed_data(stage_filedir,file)
             
        elif "lineitem" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,lineitem_columns),'LINEITEM') 
            archive_processed_data(stage_filedir,file)
        
        elif "nation" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,nation_columns),'NATION')
            archive_processed_data(stage_filedir,file)
        
        elif "orders" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,orders_columns),'ORDERS')
            archive_processed_data(stage_filedir,file)
        
        elif "part" in file.lower()  and "partsupp" not in  file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,part_columns),'PART')
            archive_processed_data(stage_filedir,file)
        
        elif "partsupp" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,partsupp_columns),'PARTSUPP')
            archive_processed_data(stage_filedir,file)
        
        elif "region" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,region_columns),'REGION')
            archive_processed_data(stage_filedir,file)
        
        elif "supplier" in file.lower():
            save_df(cnx,load_CSV(stage_filedir,file,supplier_columns),'SUPPLIER')
            archive_processed_data(stage_filedir,file)
 

start_ETL()

