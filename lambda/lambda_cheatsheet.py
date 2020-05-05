import json
import requests

def lambda_handler(event, context):
    transfer_log()


def transfer_log():
    proxies = {
        "http":  "http://address",
        "https": "https://address",
    }
    URL="http://address"
    response = requests.get(URL, proxies=proxies)
    if response.ok:
        print("response ok: {}".format(response.content))
    else:
    	print("response not okay. status code: {}".format(response.status_code))


import boto3
import botocore
from botocore.exceptions import ClientError
import sys
import os
import time
import configparser
import logging
import zipfile
import glob

global SF_SECRET
global SF_ACCOUNT 
global SF_USER
global SF_PASS
global SF_DATABASE
global SF_WAREHOUSE
global SF_ROLE
global STAGE_LIST
global TABLE_LIST
global AWS_SECRET
global AWS_ACCESS_KEY
global AWS_SECRET_KEY
global SCHEMAS
global SF_SCHEMA
global KW_TBLS
global PB_TBLS
global SF_STAGING
global SF_TABLE
global EXTERNAL_FILE
global TOTAL_RECORD

    
def get_secret(secret_name):
    """
    Description: Connects to AWS and retrieves the specified secret
    args: secret_name is the key to access AWS Secret Manager to retrieve k/v pair
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-west-2',
        endpoint_url='https://secretsmanager.us-west-2.amazonaws.com'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The requested secret  { secret_name }  was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to: {}".format(e))
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:{}".format(e))
        else:
            print("Unknown error: ", e)
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']
        return secret


def set_global_vars():
    """
    Description: Open and read config file to assign variables for SF connection
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    print('Reading config file')
    
    # Declare global vars
    global SF_SECRET
    global SF_ACCOUNT 
    global SF_USER
    global SF_PASS
    global SF_DATABASE
    global SF_WAREHOUSE
    global SF_ROLE
    global STAGE_LIST
    global TABLE_LIST
    global AWS_SECRET
    global AWS_ACCESS_KEY
    global AWS_SECRET_KEY
    global AWS_BUCKET
    global AWS_OBJECT
    global AWS_FILE
    global LAMBDA_TMP
    global SCHEMAS
    global SF_SCHEMA
    global KW_TBLS
    global PB_TBLS
    global SF_STAGING
    global SF_TABLE
    global EXTERNAL_FILE
    global INSERT_STATUS
    
    # Assign global vars
    if (os.environ['env'] == 'DEV'):
        SF_SECRET = config['DEV_SNOWFLAKE_ACC']['SECRET']
        SF_ACCOUNT = config['DEV_SNOWFLAKE_ACC']['SNOWFLAKE_ACCOUNT']
        SF_DATABASE = config['DEV_SNOWFLAKE_DB']['DATABASE']
        SF_WAREHOUSE = config['DEV_SNOWFLAKE_DB']['WAREHOUSE']
        SF_ROLE = config['DEV_SNOWFLAKE_DB']['ROLE']
        SCHEMAS = config['DEV_SNOWFLAKE_DB']['SCHEMAS']
        KW_TBLS = config['DEV_SNOWFLAKE_DB']['KENWORTH'].split(',')
        PB_TBLS = config['DEV_SNOWFLAKE_DB']['PETERBILT'].split(',')
        AWS_SECRET = config['DEV_AWS']['SECRET']
        
        #Get source code from S3 bucket
        AWS_BUCKET = config['DEV_SOURCE_CODE']['BUCKET']
        AWS_OBJECT = config['DEV_SOURCE_CODE']['OBJECT']
        AWS_FILE = config['DEV_SOURCE_CODE']['FILE']
        LAMBDA_TMP = config['DEV_SOURCE_CODE']['TMP']
        
        # Call secret manager, and clean/parse response for AWS access
        secret_mngr = get_secret(AWS_SECRET).replace('{', '').replace('}', '').replace('"', '').split(':')
        AWS_ACCESS_KEY = str(secret_mngr[0]).strip()
        AWS_SECRET_KEY = str(secret_mngr[1]).strip()
        print('Retrieved AWS secret')
        
        # Call secret manager, and clean/parse response for Snowflake access
        snowflake_secret = get_secret(SF_SECRET).replace('{', '').replace('}', '').replace('"', '').split(':')
        SF_USER = str(snowflake_secret[0]).strip()
        SF_PASS = str(snowflake_secret[1]).strip()
        print('Retrieved Snowflake secret') 
    else:
        # Add PROD-env options here 
        return
    

def connect_to_sf(sf_role, sf_warehouse, sf_database, sf_schema):
    """
    Description: Create and return a connection to Snowflake
    """
    con = snowflake.connector.connect(
        user=SF_USER,
        account=SF_ACCOUNT,
        password=SF_PASS
    )
    
    cur = con.cursor()
    cur.execute("USE ROLE {role}".format(role = sf_role))
    cur.execute("USE WAREHOUSE {warehouse}".format(warehouse = sf_warehouse))
    cur.execute("USE DATABASE {db}".format(db = sf_database))
    cur.execute("USE SCHEMA {schema}".format(schema = sf_schema))
    print('Connected to Snowflake')
    return con

def pb_guideline_table_creation(cur, s3_file):
    """Description: Handle dynamic table for PB guideline table
    args: cur is a cursor to make Snowflake operations
          s3_file is the file name
    """
    
    load_status = False
    try:
        df = pd.read_csv(s3_file)
        dataTypeMap = df.columns.to_series().groupby(df.dtypes).groups
    
        mapping = {}
        for col in df.columns:
            columnQuote = "\"" + col + "\""
            mapping[columnQuote] = None


        for keys, values in dataTypeMap.items():
            for k in mapping:
                if keys == "int64":
                    for i in values:
                        newV = "\"" + i + "\""
                        name = newV + " number(38,0)"
                        if newV == k:
                            mapping[k] = name

                elif keys == "float64":
                    for i in values:
                        newV = "\"" + i + "\""
                        name = newV + " number(38,2)"
                        if newV == k:
                            mapping[k] = name
                else:
                    for i in values:
                        newV = "\"" + i + "\""
                        name = newV + " varchar(255)"
                        if newV == k:
                            mapping[k] = name
        
        tableCol = ""
        for keys, value in mapping.items():
            tableCol = tableCol + value + ", "
        tableCol = tableCol[:len(tableCol) -2:]
        
        createStage = "create or replace table ZZZ_LAMBDA_PB_GUIDELINE_LOOKUP_STAGE (" + tableCol + ");"
        createFinal = "create or replace table ZZZ_LAMBDA_PB_GUIDELINE_LOOKUP (" + tableCol + ", LOAD_DATE_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP());"
        
        cur.execute(createStage)
        cur.execute(createFinal)
        
        print("Tables Recreated: ZZZ_LAMBDA_PB_GUIDELINE_LOOKUP and ZZZ_LAMBDA_PB_GUIDELINE_LOOKUP_STAGE")
        load_status = True

    except Exception as e:
        print('Error creating PB_GUIDELINE_LOOKUP: ', e)
    return load_status
    
    
def insert_into_staging(cur, s3_file, staging):
    """
    Description: Load the data from S3 file into staging table on Snowflake 
    args: cur is a cursor to make Snowflake operations 
          s3_file is the file name 
          staging is the staging table name 
    """
    load_status = False
    try: 
        # Clean staging table 
        cur.execute("""
            TRUNCATE TABLE {table_name}
        """.format(
        table_name= staging))
        
        cur.execute("""
        COPY INTO {staging_tbl} FROM {file_name}
        CREDENTIALS = (
            aws_key_id='{aws_access_key_id}',
            aws_secret_key='{aws_secret_access_key}')
        FILE_FORMAT=(
                    type = csv
                    field_delimiter = ',' 
                    skip_header = 1
                    validate_utf8=false
                    encoding = 'UTF8'
                    Escape_Unenclosed_field = None
                    field_optionally_enclosed_by = '"'
                    error_on_column_count_mismatch = false,
                    null_if = ('NULL', 'null', '', 'N/A'))
        """.format(
        file_name = s3_file,
        staging_tbl = staging,
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY))
        load_status = True
        print('Data loaded into staging table: ', staging)
    except Exception as e:
        print('Error loading data into staging table: ', e)
    return load_status
    
    
def insert_into_final(cur, staging, final):
    """
    Description: Load the data from the staging table into the final table on Snowflake
    args: cur is the cursor to make Snowflake operations 
          staging is the staging table name 
          final is the final table name 
    """

    load_status = False
    try: 
        cur.execute("""
            INSERT INTO {final_tbl}
            SELECT *, CURRENT_TIMESTAMP() FROM {staging_tbl}
        """.format(
        final_tbl = final,
        staging_tbl = staging))
        print('Data loaded into final table: ', final)
        print('Data load complete')
    except Exception as e:
        print('Error loading data into final table: ', e)
        raise e
    return load_status
    
def update_final_table(cur, staging, final):
    """ 
    Description: deletes the old data from the final table once the new data have load
    args: cur is the cursor to make Snowflake Operations
          final is the final table name
    """
    try:
        #Delete old data from final table
        cur.execute("""DELETE from {table_name} where LOAD_DATE_TIME in (SELECT LOAD_DATE_TIME from {table_name}
        WHERE date_trunc('MINUTE', LOAD_DATE_TIME) != 
        (SELECT max(date_trunc('MINUTE', LOAD_DATE_TIME)) FROM {table_name}))""".format (table_name = final))
        print('Removing old data complete')
        
        # Truncate staging table 
        cur.execute("""
            TRUNCATE TABLE {table_name}
        """.format(
        table_name= staging))
        print('Truncated staging table: ', staging)
    except Exception as e:
        print('Error deleting data from final table: ', e)
        raise e

def check_row_count(cur, final):
    """
    Description: checks the number of rows in the file, and the number of rows inserted 
    args: cur is the cursor to make Snowflake operations 
          final is the final table name 
    """
    #Get the Total Row Inserted
    global TOTAL_RECORD
    try:
        TOTAL_RECORD = cur.execute("""
            SELECT COUNT(LOAD_DATE_TIME) FROM {final_name} WHERE date_trunc('MINUTE', LOAD_DATE_TIME) = 
            (SELECT max(date_trunc('MINUTE', LOAD_DATE_TIME)) FROM {final_name})
        """.format(
        final_name = final)).fetchone() 
        print("Total records inserted: ", TOTAL_RECORD[0]) 
    except Exception as e:
        print('Error selecting from tables:', final, 'to get row count') 
        
    return TOTAL_RECORD[0]

#Have to run before lambda_handler
set_global_vars()
s3 = boto3.resource('s3')
s3.meta.client.download_file(AWS_BUCKET, AWS_OBJECT, AWS_FILE)

#Unzip with zipfile
fileUnzip = AWS_FILE
with zipfile.ZipFile(fileUnzip, 'r') as zip_ref:
    zip_ref.extractall(LAMBDA_TMP)
    
sys.path.append(LAMBDA_TMP)

import snowflake.connector
import pandas as pd

def lambda_handler(event, context):
    global TOTAL_RECORDS
    global SF_STAGING
    global SF_TABLE
    global INSERT_SUCCESS 
    TOTAL_RECORDS = 0
    INSERT_SUCCESS = "Insert failed." 
    
    start = time.time()
    
    # Parsing event for bucket, file name
    bkt = event['Records'][0]['s3']['bucket']['name'] 
    obj = event['Records'][0]['s3']['object']['key']
    file_path = str(bkt + '/' + obj)
    
    #Get file for Pandas
    panda_obj = s3.Object(bkt, obj).get()["Body"]
    
    # Parse and put together the location in S3
    fldr = os.path.dirname(file_path) 
    file = os.path.basename(file_path) 
    EXTERNAL_FILE = 's3://' + fldr + '/' + file 
    print('Received object: ', EXTERNAL_FILE)
    
    try:
        # Assign schema, table names to global variables
        SF_SCHEMA = SCHEMAS
        if 'KW' in fldr: 
            if 'ADJUSTER' in file:
                SF_STAGING = KW_TBLS[1]
                SF_TABLE = KW_TBLS[0]
            elif 'BASE_TRUCK_VALUE' in file: 
                SF_STAGING = KW_TBLS[3]
                SF_TABLE = KW_TBLS[2]
            elif 'CUSTOMER_SIZE' in file:
                SF_STAGING = KW_TBLS[5]
                SF_TABLE = KW_TBLS[4]
            elif 'DNET' in file:
                SF_STAGING = KW_TBLS[7]
                SF_TABLE = KW_TBLS[6]
            elif 'CONTROL_SHEET' in file:
                SF_STAGING = KW_TBLS[9]
                SF_TABLE = KW_TBLS[8]
            else:
                print('Unrecognized file name for', SF_SCHEMA)
                return
        
        elif 'PB' in fldr:
            if 'CUSTOMER_SIZE' in file:
                SF_STAGING = PB_TBLS[1]
                SF_TABLE = PB_TBLS[0]
            elif 'GUIDELINE' in file:
                SF_STAGING = PB_TBLS[3]
                SF_TABLE = PB_TBLS[2]
            else:
                print('Unrecognized file name for', SF_SCHEMA)
                return 

        else:
            print('File not identified as either Kenworth or Peterbilt. Exiting.')
            
        print('Using Schema: ', SF_SCHEMA, 'Inserting into:', SF_STAGING, ', and', SF_TABLE)
        
        try:
            con = connect_to_sf(SF_ROLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA)
            cur = con.cursor()
        except Exception as e:
            print('Error connecting to Snowflake: ', e)
        
        #TODO: return success/fail through SNS
        if 'KW' in SF_TABLE:
            insert_into_staging(cur, EXTERNAL_FILE, SF_STAGING)
            insert_into_final(cur, SF_STAGING, SF_TABLE)
            update_final_table(cur, SF_STAGING, SF_TABLE)
        elif 'PB' in SF_TABLE:
            if 'GUIDELINE' in SF_TABLE:
                if(pb_guideline_table_creation(cur, panda_obj)):
                    insert_into_staging(cur, EXTERNAL_FILE, SF_STAGING)
                    insert_into_final(cur, SF_STAGING, SF_TABLE)
                    update_final_table(cur, SF_STAGING, SF_TABLE)
                else:
                    return
        else:
            print('Error: table not recognized')
            
        TOTAL_RECORDS = check_row_count(cur, SF_TABLE)

        con.close()
        print('Snowflake connection closed')
        
        INSERT_SUCCESS = "Insert successful."
    except Exception as e:
        INSERT_STATUS += 'Error: '
        INSERT_STATUS += e
        con.close()
    finally:
        print("End of Lambda")
