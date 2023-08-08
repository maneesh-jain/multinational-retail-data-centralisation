# %% connect to AWS db, read into Pandas df
from database_utils import DatabaseConnector 
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

db_creds = 'C:\\Users\\Maneesh\\Documents\\AiCore\lessons\\retail\\db_creds.yaml'
db = DatabaseConnector(db_creds)
tables = db.list_db_tables()

print(f'Tables in database:\n{tables}\n') # list tables

# %% clean dataframes
engine = db.init_db_engine()
de = DataExtractor(engine)

legacy_users = de.read_rds_table('legacy_users')
df_to_clean = DataCleaning(legacy_users)
cleaned_users = df_to_clean.clean_users()

orders_table = de.read_rds_table('orders_table')
df_to_clean = DataCleaning(orders_table)
cleaned_orders = df_to_clean.clean_orders_table()

print('=== All Dataframes cleaned ===\n')

# %% upload 'users' df to local SQL db
db.upload_to_db(cleaned_users, 'dim_users')
print('=== Dataframe uploaded to PostgreSQL database ===\n')

# %% extract card_details from PDF into df
from data_extraction import DataExtractor
de = DataExtractor()
URL = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
card_details = de.retrieve_pdf_data(URL)

# %% clean 'card_details' df
from data_cleaning import DataCleaning
df_to_clean = DataCleaning(card_details)
cleaned_card_details = df_to_clean.clean_card_data()

# %% upload 'card_details' df to local SQL db
db = DatabaseConnector()
db.upload_to_db(cleaned_card_details, 'dim_card_details')
print('=== Dataframe uploaded to PostgreSQL database ===\n')

# %% list number of stores in AWS db
from data_extraction import DataExtractor
de = DataExtractor()
url_num_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
url_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' # add store number
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer your_token_here", 
    "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
de.list_number_of_stores(url_num_stores, headers)

# %% retrieve data for each store from AWS db, read into Pandas df
from data_extraction import DataExtractor
de = DataExtractor()
url_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' # add store number
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer your_token_here", 
    "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
store_details = de.retrieve_stores_data(url_store, headers, 451)

# %% clean store data
from data_cleaning import DataCleaning
df_to_clean = DataCleaning(store_details)
cleaned_store_details = df_to_clean.clean_store_details()

# %% upload 'store_details' df to local SQL db
from database_utils import DatabaseConnector 
db = DatabaseConnector()
db.upload_to_db(cleaned_store_details, 'dim_store_details')
print('=== Dataframe uploaded to PostgreSQL database ===\n')

# %% download products data from S3
from data_extraction import DataExtractor
de = DataExtractor()
products = de.extract_from_s3_to_csv('s3://data-handling-public/products.csv')

# %% clean products data
from data_cleaning import DataCleaning
df_to_clean = DataCleaning(products)
cleaned_products = df_to_clean.convert_product_weights()
cleaned_products = df_to_clean.clean_products_data()

# %% upload 'products' df to local SQL db
from database_utils import DatabaseConnector 
db = DatabaseConnector()
db.upload_to_db(cleaned_products, 'dim_products')
print('=== Dataframe uploaded to PostgreSQL database ===\n')

# %% upload 'orders_table' df to local SQL db
from database_utils import DatabaseConnector 
db = DatabaseConnector()
db.upload_to_db(cleaned_orders, 'orders_table')
print('=== Dataframe uploaded to PostgreSQL database ===\n')

# %% download date_details from S3
from data_extraction import DataExtractor
de = DataExtractor()
date_details = de.extract_from_s3_to_json('data-handling-public', 'date_details.json')

# %% clean'date_details'
from data_cleaning import DataCleaning
df_to_clean = DataCleaning(date_details)
cleaned_date_details = df_to_clean.clean_date_details()

# %% upload 'date_details' df to local SQL db
from database_utils import DatabaseConnector 
db = DatabaseConnector()
db.upload_to_db(cleaned_date_details, 'dim_date_times')
print('=== Dataframe uploaded to PostgreSQL database ===\n')
