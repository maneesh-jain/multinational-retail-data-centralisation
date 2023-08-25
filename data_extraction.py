import boto3
import pandas as pd
import requests
import tabula

class DataExtractor:

    def __init__(self, engine=''):
        self.engine = engine

    def read_rds_table(self, table):
        '''Reads an AWS RDS database into a Pandas Dataframe.'''
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_table(table, conn, index_col='index')

    def retrieve_pdf_data(self, pdf_path):
        '''Reads a multi-page PDF table into a Pandas Dataframe using Tabula.'''
        dfs = tabula.read_pdf(pdf_path, pages='all')
        combined_df = []
        for n in range(len(dfs)):
            combined_df.append(dfs[n]) # each page in the pdf <dfs[n]> is a df
        return pd.concat(combined_df, ignore_index=True) # concatenate all the dataframes into one

    def list_number_of_stores(self, url, headers):
        '''Lists the number of stores in the AWS RDS database using Requests.'''
        response = requests.get(url, headers=headers)
        return response.json()

    def retrieve_stores_data (self, url, headers, no_stores):
        '''Retrieves data for every store using Requests into a Pandas Dataframe.'''
        combined_df = []
        for s in range(no_stores):
            addr = url + str(s)
            response = requests.get(addr, headers=headers)
            if response.status_code == 200:
                data = response.json()
                print(data)
                new_df = pd.DataFrame(data, index=[s]) # .drop(columns='index') << optional
                combined_df.append(new_df)
        return pd.concat(combined_df, ignore_index=True) # concatenate all the dataframes into one

    def extract_from_s3_to_csv(self, file_address):
        '''Extracts data from AWS S3 using Boto and saves to a CSV file locally.'''
        address_list = file_address.split('/')
        bucket = address_list[2]
        object_key = address_list[3]
        save_location = object_key
        s3 = boto3.client('s3')
        data = s3.download_file(bucket, object_key, save_location)
        print(data)
        with open(save_location, mode='r') as f:
            df = pd.read_csv(f)
        df.drop(columns=df.columns[0], axis=1,  inplace=True)
        return df

    def extract_from_s3_to_json(self, bucket, object_key):
        '''Extracts data from AWS S3 using Boto and saves to a JSON file locally.'''
        save_location = object_key
        s3 = boto3.client('s3')
        data = s3.download_file(bucket, object_key, save_location)
        print(data)
        with open(save_location, mode='r') as f:
            df = pd.read_json(f)
        df.drop(columns=df.columns[0], axis=1,  inplace=True)
        return df
