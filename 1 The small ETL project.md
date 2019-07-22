
### Install required packages

Scripts "./scripts/install_packages.sh" will install additional Python library using pip


```python
! sh ./scripts/install_packages.sh
```

    Requirement already satisfied: pandas in /home/ahmed/my_project_env/lib/python3.6/site-packages (0.25.0)
    Requirement already satisfied: pytz>=2017.2 in /home/ahmed/my_project_env/lib/python3.6/site-packages (from pandas) (2019.1)
    Requirement already satisfied: numpy>=1.13.3 in /home/ahmed/my_project_env/lib/python3.6/site-packages (from pandas) (1.16.4)
    Requirement already satisfied: python-dateutil>=2.6.1 in /home/ahmed/my_project_env/lib/python3.6/site-packages (from pandas) (2.8.0)
    Requirement already satisfied: six>=1.5 in /home/ahmed/my_project_env/lib/python3.6/site-packages (from python-dateutil>=2.6.1->pandas) (1.12.0)
    Requirement already satisfied: sqlalchemy in /home/ahmed/my_project_env/lib/python3.6/site-packages (1.3.5)
    Requirement already satisfied: pymysql in /home/ahmed/my_project_env/lib/python3.6/site-packages (0.9.3)


### Create table
Scripts "./python/exec_ddl.py" will create the required tables 


```python
!python ./python/exec_ddl.py
```

    Tables created successfully


###  Move the CSV file to the landing area
Proposed csv(pipe delimeted)files are in data.zip file. Following command will uncompress the zip files and files will be placed in landing area.


```python
! unzip ../data.zip -d ./DATA/STAGE/
```

    Archive:  ../data.zip
      inflating: ./DATA/STAGE/customer.tbl  
      inflating: ./DATA/STAGE/lineitem.tbl  
      inflating: ./DATA/STAGE/nation.tbl  
      inflating: ./DATA/STAGE/orders.tbl  
      inflating: ./DATA/STAGE/part.tbl   
      inflating: ./DATA/STAGE/partsupp.tbl  
      inflating: ./DATA/STAGE/region.tbl  
      inflating: ./DATA/STAGE/supplier.tbl  



```python
!ls  ./DATA/STAGE/
```

    customer.tbl  nation.tbl  partsupp.tbl	region.tbl
    lineitem.tbl  orders.tbl  part.tbl	supplier.tbl


### Start ETL
Scripts "./python/start_etl.py" will load data into source image. Data curation was done and *.tbl files contain additional pipe(|)


```python
!python  ./python/start_etl.py
```

    /home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE
    Start saving ......
    /home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/lineitem.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/lineitem.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_lineitem.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/supplier.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/supplier.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_supplier.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/partsupp.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/partsupp.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_partsupp.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/customer.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/customer.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_customer.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/nation.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/nation.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_nation.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/orders.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/orders.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_orders.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/part.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/part.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_part.tbl
    Loading CSV to Dataframe:/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/region.tbl
    processed_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/STAGE/region.tbl
    archived_file :/home/ahmed/interview-test-data-engineer/PROSPA_ETL/DATA/PROCESSED/1563813027_region.tbl


###  Prepare the Fact and Dimension tables



```python
!python  ./python/prepare_dim_fact_table.py	
```

    CUSTOMER_STATUS truncated
    CUSTOMER_STATUS refreshed
    CUSTOMER_DIM processed
    CUSTOMER_DIM processed
    PART_SUPP_DIM processed
    ORDER_ITEM_FACT processed



```python

```
