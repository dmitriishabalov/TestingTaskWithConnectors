import os
import pandas as pd


DATABASE = os.environ.get('DATABASE')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')

filepath = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
tablename = 'api_raw_data'


def extract_data():
    api_df = pd.read_json(filepath).set_index('id')
    return api_df


def load_data(api_df: pd.DataFrame):
    conn = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    api_df.to_sql(tablename, con=conn, if_exists='append', index=True)


def run_process():
    load_data(extract_data())
