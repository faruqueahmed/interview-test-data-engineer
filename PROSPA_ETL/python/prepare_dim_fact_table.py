#!/usr/bin/env python
# coding: utf-8

 


import os
import json

import pymysql
with open(os.getcwd()+'/config/config.json') as config_file:
        params = json.load(config_file)

def get_db_connection():
    with open(os.getcwd()+'/config/config.json') as config_file:
        user = params['db_user']
        passw = params['dbpassword']  
        host =  params['hostname']
        port = params['port']
        database=params['databasename']
        conn = pymysql.connect(host,user,passw,database )
        return conn

    
def process_CUSTOMER_STATUS():    
    conn=get_db_connection()
    stmt1  = "TRUNCATE TABLE CUSTOMER_STATUS;"
    stmt2="""INSERT INTO CUSTOMER_STATUS
    SELECT C1.C_CUSTKEY, C1.C_ACCTBAL  ,
        CASE 
            WHEN C1.C_ACCTBAL < LOWERQUARTILE     THEN 'LOW'
            WHEN C1.C_ACCTBAL < UPPERQUARTILE    THEN 'MID'
            ELSE 'HIGH'
        END AS C_STATUS
    FROM CUSTOMER C1
    CROSS JOIN (
        SELECT 
        MIN(C_ACCTBAL) ,
        MAX(C_ACCTBAL) ,
        ((MIN(C_ACCTBAL) + AVG(C_ACCTBAL)) / 2) AS LOWERQUARTILE,
        ((MAX(C_ACCTBAL) + AVG(C_ACCTBAL)) / 2) AS UPPERQUARTILE
        FROM CUSTOMER
        ) C2"""
    with conn.cursor() as cursor:
        conn.cursor()
        cursor.execute(stmt1)
        conn.commit()
        print("CUSTOMER_STATUS truncated")
        cursor.execute(stmt2)
        conn.commit()
        print("CUSTOMER_STATUS refreshed")
 
 


def process_CUSTOMER_DIM():    
    conn=get_db_connection()     
    stmt=""" INSERT INTO CUSTOMER_DIM  
    SELECT C.C_CUSTKEY,
      C.C_NAME,
      C.C_ADDRESS,
      C.C_NATIONKEY,
      C.C_PHONE,
      C.C_ACCTBAL,
      C.C_MKTSEGMENT,
      CS.C_STATUS,
      N.N_NAME,
      R.R_NAME, 
      C.FILENAME,
      C.INGEST_TIME
      FROM CUSTOMER C,  NATION N, REGION R , CUSTOMER_STATUS CS
     WHERE C.C_NATIONKEY = N.N_NATIONKEY
     AND N.N_REGIONKEY = R.R_REGIONKEY 
     AND C.C_CUSTKEY =CS.C_CUSTKEY  and C.INGEST_TIME in (select max(ingest_time) from ORDERS)
     """
    with conn.cursor() as cursor:
        conn.cursor()
        cursor.execute(stmt)
        conn.commit()
        print("CUSTOMER_DIM processed")     

 

def process_CUSTOMER_DIM():    
    conn=get_db_connection()     
    stmt=""" INSERT INTO CUSTOMER_DIM  
    SELECT C.C_CUSTKEY,
      C.C_NAME,
      C.C_ADDRESS,
      C.C_NATIONKEY,
      C.C_PHONE,
      C.C_ACCTBAL,
      C.C_MKTSEGMENT,
      CS.C_STATUS,
      N.N_NAME,
      R.R_NAME, 
      C.FILENAME,
      C.INGEST_TIME
      FROM CUSTOMER C,  NATION N, REGION R , CUSTOMER_STATUS CS
     WHERE C.C_NATIONKEY = N.N_NATIONKEY
     AND N.N_REGIONKEY = R.R_REGIONKEY 
     AND C.C_CUSTKEY =CS.C_CUSTKEY  and C.INGEST_TIME in (select max(ingest_time) from ORDERS)
     """
    with conn.cursor() as cursor:
        conn.cursor()
        cursor.execute(stmt)
        conn.commit()
        print("CUSTOMER_DIM processed")


 
def process_PART_SUPP_DIM():    
    conn=get_db_connection()     
    stmt=""" INSERT INTO PART_SUPP_DIM  
    SELECT 
      PS_PARTKEY     ,
      PS_SUPPKEY     ,
      PS_AVAILQTY   ,
      PS_SUPPLYCOST  ,
      S_NAME        ,
      S_ADDRESS    ,
      S_NATIONKEY   ,
      S_PHONE       ,
      S_ACCTBAL     ,
      P_NAME        ,
      P_MFGR       ,
      P_BRAND       ,
      P_TYPE        ,
      P_SIZE        ,
      P_CONTAINER   ,
      P_RETAILPRICE ,
      P.INGEST_TIME 
 FROM  
	PART P,  SUPPLIER S, PARTSUPP PS
 WHERE  PS.PS_SUPPKEY = S.S_SUPPKEY
	AND P.P_PARTKEY= PS.PS_PARTKEY and P.INGEST_TIME in (select max(ingest_time) from ORDERS)
     """
    with conn.cursor() as cursor:
        conn.cursor()
        cursor.execute(stmt)
        conn.commit()
        print("PART_SUPP_DIM processed")


 

def process_ORDER_ITEM_FACT():    
    conn=get_db_connection()     
    stmt=""" INSERT INTO ORDER_ITEM_FACT 
    SELECT
          L_ORDERKEY      ,
          L_PARTKEY       ,
          L_SUPPKEY       ,
          L_LINENUMBER    ,
          L_QUANTITY      ,
          L_EXTENDEDPRICE ,
          L_DISCOUNT      ,
          L_TAX           ,
          L_RETURNFLAG    ,
          L_LINESTATUS    ,
          L_SHIPDATE      ,
          L_COMMITDATE    ,
          L_RECEIPTDATE   ,
          L_SHIPINSTRUCT  ,
          L_SHIPMODE      ,
          L_COMMENT       ,
         ( L_QUANTITY *L_EXTENDEDPRICE - L_DISCOUNT) LINEITEM_REVENUE  ,
          O_CUSTKEY      ,
          O_ORDERSTATUS  , 
    CASE
        WHEN
            MOD(YEAR(O_ORDERDATE), 2) = 0
        THEN
            STR_TO_DATE(CONCAT('2016', SUBSTRING(O_ORDERDATE, 5, 6)),
                    '%Y-%m-%d')
        ELSE STR_TO_DATE(CONCAT('2017', SUBSTRING(O_ORDERDATE, 5, 6)),
                '%Y-%m-%d')
    END AS ODATE2    
FROM  ORDERS O , LINEITEM L
WHERE  O.O_ORDERKEY = L.L_ORDERKEY  and L.INGEST_TIME in (select max(ingest_time) from ORDERS)
     """
    with conn.cursor() as cursor:
        conn.cursor()
        cursor.execute(stmt)
        conn.commit()
        print("ORDER_ITEM_FACT processed")
        
 

 
def main():
	process_CUSTOMER_STATUS()
	process_CUSTOMER_DIM() 
	process_CUSTOMER_DIM()  
	process_PART_SUPP_DIM()
	process_ORDER_ITEM_FACT()




if __name__ == "__main__":
    main()

 



