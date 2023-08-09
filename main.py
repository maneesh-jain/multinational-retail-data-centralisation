from database_utils import DatabaseConnector 
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

'''Connects to AWS, lists tables in database.'''
db_creds = 'C:\\Users\\Maneesh\\Documents\\AiCore\lessons\\retail\\db_creds.yaml'
db = DatabaseConnector(db_creds)
tables = db.list_db_tables()
print(f"Tables in AWS database: {tables}\n")

def users_and_orders():
    '''Reads users and orders tables from AWS into dataframes, cleans, uploads to SQL database.'''
    engine = db.init_db_engine()
    de = DataExtractor(engine)

    legacy_users = de.read_rds_table('legacy_users')
    df_to_clean = DataCleaning(legacy_users)
    cleaned_users = df_to_clean.clean_users()
    db.upload_to_db(cleaned_users, 'dim_users')

    orders_table = de.read_rds_table('orders_table')
    df_to_clean = DataCleaning(orders_table)
    cleaned_orders = df_to_clean.clean_orders_table()
    db.upload_to_db(cleaned_orders, 'orders_table')

def card_details():
    '''Extracts card_details from PDF into df, cleans, uploads to SQL database.'''
    de = DataExtractor()
    URL = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_details = de.retrieve_pdf_data(URL)
    df_to_clean = DataCleaning(card_details)
    cleaned_card_details = df_to_clean.clean_card_data()
    db.upload_to_db(cleaned_card_details, 'dim_card_details')

def get_number_of_stores():
    '''Lists number of stores in AWS database.'''
    from data_extraction import DataExtractor
    de = DataExtractor()
    url_num_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    url_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' # add store number
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer your_token_here", 
        "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    num_stores = de.list_number_of_stores(url_num_stores, headers)
    print(num_stores)

def stores():
    '''Retrieves data for every store from AWS, reads into Pandas Dataframe, cleans, uploads to SQL database.'''
    from data_extraction import DataExtractor
    de = DataExtractor()
    url_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' # add store number
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer your_token_here", 
        "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_details = de.retrieve_stores_data(url_store, headers, 451)
    df_to_clean = DataCleaning(store_details)
    cleaned_store_details = df_to_clean.clean_store_details()
    db = DatabaseConnector()
    db.upload_to_db(cleaned_store_details, 'dim_store_details')

def products():
    '''Downloads products data from S3, cleans, uploads to SQL database.'''
    from data_extraction import DataExtractor
    de = DataExtractor()
    products = de.extract_from_s3_to_csv('s3://data-handling-public/products.csv')
    df_to_clean = DataCleaning(products)
    cleaned_products = df_to_clean.convert_product_weights()
    cleaned_products = df_to_clean.clean_products_data()
    db = DatabaseConnector()
    db.upload_to_db(cleaned_products, 'dim_products')

def date_details():
    '''Downloads date_details from S3, cleans, uploads to SQL database.'''
    from data_extraction import DataExtractor
    de = DataExtractor()
    date_details = de.extract_from_s3_to_json('data-handling-public', 'date_details.json')
    df_to_clean = DataCleaning(date_details)
    cleaned_date_details = df_to_clean.clean_date_details()
    db = DatabaseConnector()
    db.upload_to_db(cleaned_date_details, 'dim_date_times')

users_and_orders()
card_details()
get_number_of_stores()
stores()
products()
date_details()
