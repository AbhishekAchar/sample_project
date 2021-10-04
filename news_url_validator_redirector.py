# The script will be scheduled on docker

########### UPDATE ##########
# Date updated : 16/06/2020 / Name : Abhishek A / Description : Validates and gets redirected url for each url
# Date updated : 21/12/2020 / Name : Abhishek A / Description : Added code to re-try while getting data from mysql and check dataframe is empty before calling  parallelize function
import json
import requests
import sys
import traceback
from datetime import datetime
import os
from time import time
import logging
from pecten_utils import miscellaneous as misc
from pecten_utils.duplication_handler import DuplicationHandler as Handler
from pecten_utils.Storage import Storage
import argparse
import pandas as pd
from pecten_utils.BigQueryLogsHandler import BigQueryLogsHandler
from google.cloud import storage as google_storage
from pecten_utils.process_flow.process_flow_utility import ProcessFlow
from pecten_utils.process_flow.process_logger import ProcessLogger
from bs4 import BeautifulSoup
from sqlalchemy import create_engine,text
from sqlalchemy.exc import IntegrityError
import multiprocessing
import numpy as np
import re

#function will parallelize in processing the urls
def parallelize(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = multiprocessing.Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

#function will validate the urls
def run_get_valid_redirected_url(df):
    def get_valid_redirected_url(url):
        try:
            resp = requests.get(url,headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"},timeout=10)
            if resp.status_code in [200, 403, 400]:
                return ("valid",resp.url)
            return ("invalid",url)
        except Exception as e:
            return ("invalid",url)
    df.loc[:,['url_state','news_url']] = df['news_url'].apply(get_valid_redirected_url).values.tolist()
    return df

#function will check whether url is valid and redirectable
def get_valid_url(data):
    print('Url validator and redirector started')
    try:
        columns = ["news_url", "url_id", "url_state","last_process","news_collection_date",'pecten_source','news_type']
        query = "SELECT {}  FROM  {} where url_state='unparsed' and title_deduped=1 order by ROW_NUMBER() over (Partition BY  pecten_source order by news_collection_date )  limit 1200".format(', '.join(columns),args.table_name)
        try:
            row = args.storage.get_sql_data_text_query(os.environ.get('MYSQL_CONNECTION_STRING'), query)
            result = row.fetchall()
        except:
            try:
                row = args.storage.get_sql_data_text_query(os.environ.get('MYSQL_CONNECTION_STRING'), query)
                result = row.fetchall()
            except Exception as e:
                args.logger.error(str(traceback.format_exc()),
                          extra={"operation": "ERROR while get data from MYSQL",
                                 "script_type": "collection", "criticality": 3,
                                 "data_loss": "retrievable"})
        if result:
            data = pd.DataFrame(result,columns=columns)
            if data.empty:
                return
            data['news_url'] = data['news_url'].str.decode('utf-8')
            original = data.copy(deep=True)
            start = time()
            args.duplicate_invalid_count = 0
            data = parallelize(data,run_get_valid_redirected_url,n_cores = multiprocessing.cpu_count())
            print(time()-start)
            data['last_process'] = "news_url_validator_redirector"
            key_col = "url_id"
            diff_df = pd.merge(data,original,on=key_col,suffixes=["","_y"]).drop_duplicates(subset=['url_state','news_url','url_id'],keep=False)
            diff_df = diff_df.drop(columns=[i for i in diff_df.columns if "_y" in i])
            diff_df['url_state'] = diff_df.drop(columns=['news_collection_date','pecten_source','news_type']).apply(lambda x:update_mysql_with_valid_urls(args,x.to_dict(),key_col,original[original['url_id']==x['url_id']]['news_url'].iloc[0]),axis=1)
            for group,group_df in diff_df.groupby([diff_df['news_collection_date'].apply(lambda x:x.strftime("%Y-%m-%d")),'pecten_source','news_type']):
                additional_logs = {}
                input_count = data[np.logical_and(data['news_collection_date'].apply(lambda x:x.strftime("%Y-%m-%d"))==group[0],np.logical_and(data['pecten_source']==group[1],data['news_type']==group[2]))].shape[0]
                additional_logs['invalid_url_count'] = group_df[group_df['url_state']=="invalid"].shape[0]
                output_count = input_count - additional_logs['invalid_url_count']
                logs = args.process_logger.create_standard_process_log(group[0],group[1],input_count,output_count,**additional_logs)['logs']
                logs_file_path = 'logs_folder/{}/{}_{}.json'.format(group[2],args.process_name,group[0])
                args.run_process_flow.append_json_data_bucket_file(args.bucket_name, args.process_name, logs, logs_file_path)
                print(logs)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        print("ERROR while processing at url validator and redirector")
        args.logger.error(str(traceback.format_exc()),
                          extra={"operation": "ERROR at validty check and updating MYSQL",
                                 "script_type": "collection", "criticality": 3,
                                 "data_loss": "retrievable"})

##Function to set logger and to get the url collector function
def main(args):
    storage = Storage(args.google_key_path)
    args.storage = storage
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = BigQueryLogsHandler(args.storage, args)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    args.logger = logger
    args.handler = Handler()
    client = google_storage.Client()
    bucket_ = client.get_bucket(args.bucket_name)
    project_id = args.project_id
    args.process_name = os.path.basename(__file__).split('.')[0]
    args.process_logger = ProcessLogger(args.process_name)
    args.run_process_flow = ProcessFlow(args,process_name=args.process_name)
    try:
        get_valid_url(args)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        args.logger.error(str(traceback.format_exc()),
                          extra={"operation": "ERROR at validty check and updating MYSQL",
                                 "script_type": "collection", "criticality": 3,
                                 "data_loss": "retrievable"})
          

        
def update_sql(args,sql_connection_string,query,original_url):
    try:
        s = text(query)
        try:
            rows = args.conn.execute(s)
        except:
            engine = create_engine(sql_connection_string)
            args.conn = engine.connect()
            rows = args.conn.execute(s)
        return re.findall(r'url_state=\"([^"]+)\"',query)[0]
    except IntegrityError as e:
        try:
            query = re.sub(r'news_url=\"[^"]+\"','news_url="{}"'.format(original_url),query)
            query = re.sub(r'url_state=\"[^"]+\"','url_state="invalid"',query)
            s = text(query)
            rows = args.conn.execute(s)
            return "invalid"
        except:
            engine = create_engine(sql_connection_string)
            args.conn = engine.connect()
            rows = args.conn.execute(s)
            return "invalid"
    except Exception as e:
        raise e 

def update_mysql_with_valid_urls(args, dic,key_col,original_url):
    query_string = "update {} set ".format(args.table_name) 
    for key,val in dic.items():
        if key_col != key:
            query_string += '{}="{}",'.format(key,val)
    query_string = query_string[:-1] + " where {} = '{}';".format(key_col, dic[key_col])
    try:
        try:
            validity = update_sql(args,args.param_connection_string, query_string,original_url)
            return validity
        except:
            validity = update_sql(args,args.param_connection_string, query_string,original_url)
            return validity
    except Exception as e:
        print(e)
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    args.python_path = os.environ.get('PYTHON_PATH', '')
    args.google_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
    args.param_connection_string = os.environ.get('MYSQL_CONNECTION_STRING', '')
    args.environment = os.environ.get('ENVIRONMENT', '')
    args.sqltable_name = os.environ.get('SQL_TABLE', '')
    Table = args.sqltable_name
    Column_list = ["BUCKET", "SOURCE_FOLDER", "DESTINATION_FOLDER", "LOGS_FOLDER", "PROCESSED_BUCKET",
                   "FAILED_BUCKET", "ADDITIONAL_FOLDERS", "ML_MODEL_BUCKET","TABLE_NAME"]
    Where = lambda x: x["SCRIPT_NAME"] == "news_url_validator_redirector"
    args.parameters = misc.get_parameters(connection_string=args.param_connection_string, table=Table,
                                          column_list=Column_list, where=Where)
    args.bucket_name = args.parameters["BUCKET"]
    args.source_folder = args.parameters["SOURCE_FOLDER"]
    args.logs_folder = args.parameters["LOGS_FOLDER"]
    args.destination_folder = args.parameters["DESTINATION_FOLDER"]
    args.irrelevant_folder = args.parameters["ADDITIONAL_FOLDERS"]
    args.processed_bucket = args.parameters["PROCESSED_BUCKET"]
    args.failed_bucket = args.parameters["FAILED_BUCKET"]
    args.model = args.parameters["ML_MODEL_BUCKET"]
    args.table_name = args.parameters["TABLE_NAME"]
    args.project_id = os.environ.get("PROJECT_ID", "pecten-project")
    sys.path.insert(0, args.python_path)
    print(args)
    main(args)
