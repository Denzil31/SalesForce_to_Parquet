import sys
import json
import shutil
import logging
import argparse
import configparser

import pandas as pd

from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from simple_salesforce import Salesforce
from simple_salesforce.bulk import SFBulkType
from simple_salesforce.exceptions import SalesforceAuthenticationFailed



def read_config(config_file: Path) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)
    return config


def get_sf_conn(sf_username: str, sf_passwd: str, sf_token: str, sf_domain: str) -> Salesforce:
    try:
        return Salesforce(username=sf_username, password=sf_passwd,
                        security_token=sf_token, domain=sf_domain)
    except SalesforceAuthenticationFailed as e:
        logging.error(f'Invalid credentials in Config file.', exc_info=e)
        sys.exit(1)
    except Exception as e:
        logging.error(f'Could not connect to Salesforce', exc_info=e)
        sys.exit(1)


def execute(sf: Salesforce, list_sf_objects: list, threads: int) -> None:
    with ThreadPoolExecutor(max_workers=int(threads)) as executor:
        genr = {executor.submit(get_sf_data, sf, json_sf_object): 
                json_sf_object for json_sf_object in list_sf_objects}
        for future in as_completed(genr):
            obj = genr[future]
            try:
                data = future.result()
            except Exception as e:
                logging.warning(f'{obj["obj_name"]} generated exception: {e}')
            else:
                logging.info(f'{obj["obj_name"]} has {data} fields.')


def pre_steps():
    for dir in ['csv', 'pq']:
        shutil.rmtree(args.output_path/dir, ignore_errors=True)
        (args.output_path/dir).mkdir(exist_ok=True, parents=True)    

    if not (Path(CONFIG_FILE).exists() and Path(JSON_FILE).exists()):
        logging.error(f'{CONFIG_FILE} or {JSON_FILE} missing.')
        sys.exit(1)

    args.log_path.mkdir(exist_ok=True, parents=True)


def get_sf_data(sf: Salesforce, sf_object: dict) -> int:
    sf_obj_name = sf_object['obj_api_name']
    field_names = [str(_['field_api_name']).strip() for _ in sf_object['fields']]
    logging.debug(f'{sf_obj_name} has fields {",".join(field_names)}')

    query = f'SELECT {",".join(field_names)} FROM {sf_obj_name}'

    if args.exec_type == 'BULK':
        sf_bulk = SFBulkType(sf_obj_name, sf.bulk_url, sf.bulk.headers, sf.session)
        data = sf_bulk.query_all(query)
        if not data:
            logging.info(f'{sf_object} has no data.')
            return 0
        df = pd.DataFrame.from_dict(data,orient='columns').drop('attributes',axis=1)
    else: # NORMAL
        data = sf.query_all(query)
        if not data:
            logging.info(f'{sf_object} has no data.')
            return 0
        df = pd.DataFrame.from_dict(data['records']).drop('attributes', axis=1)

    field_types = [_['type'] for _ in sf_object['fields']]
    dict_fields = dict(zip(field_names,field_types))
    logging.debug(dict_fields)
    df = df.astype(dict_fields)

    df.to_csv(args.output_path/'csv'/f'{sf_obj_name}.csv', index=False)
    df.to_parquet(args.output_path/'pq'/f'{sf_obj_name}.parquet', 
                engine='pyarrow', compression='snappy', index=False)

    logging.debug(df.dtypes)
    return len(df.index)


def get_json_data(json_file: Path) -> list:
    with open(json_file) as file:
        return json.load(file)


def get_args() -> argparse.ArgumentParser:
    argp = argparse.ArgumentParser(description='Multi-thread enabled Salesforce Data to Parquet/CSV script')
    argp.add_argument('config', help='Config file name', type=Path)
    argp.add_argument('json', help='Json file name', type=Path)
    argp.add_argument('--exec_type', help='Select execution type (default NORMAL)', choices=['BULK', 'NORMAL'], default='NORMAL')
    argp.add_argument('--log_level', help='Select Log Level (default INFO)', choices=['INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO')
    argp.add_argument('--log_path', help='Log path (default ./logs)', default=Path('./logs'), type=Path)
    argp.add_argument('--output_path', help='Output path (defaults to source file path)', default='', type=Path)
    return argp.parse_args()

def init_logger() -> None:
    log_map = {
        'INFO':logging.INFO,
        'DEBUG':logging.DEBUG,
        'WARNING':logging.WARNING,
        'ERROR':logging.ERROR,
        'CRITICAL':logging.CRITICAL
    }
    logging.basicConfig(format='[%(asctime)s %(levelname)-8s] %(message)s', 
                        level=log_map[args.log_level],
                        handlers=[logging.FileHandler(f'{args.log_path}/salesforce_script.log'), 
                                    logging.StreamHandler()])



if __name__ == "__main__":

    start_time = datetime.now()

    args = get_args()

    CONFIG_FILE = args.config
    CSV_FILE = "SalesForce_Objects.csv"
    JSON_FILE = args.json

    pre_steps()
    init_logger()


    config = read_config(CONFIG_FILE)
    SF_USERNAME = config['salesforce']['username']
    SF_PASSWORD = config['salesforce']['password']
    SF_TOKEN = config['salesforce']['token']
    SF_DOMAIN = config['salesforce']['domain']
    THREAD_COUNT = config['proc']['threads']


    sf = get_sf_conn(SF_USERNAME, SF_PASSWORD, SF_TOKEN, SF_DOMAIN)

    json_data = get_json_data(JSON_FILE)

    # sequential execution
    # for json_sf_object in json_data:
    #     get_sf_data(sf, json_sf_object)
    
    # parallel json
    execute(sf, json_data, THREAD_COUNT)

    logging.info(f'--- Script took {datetime.now() - start_time} ---')
