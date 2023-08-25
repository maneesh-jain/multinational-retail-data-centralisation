# Multinational Retail Data Centralisation
A multinational company that sells various goods worldwide wants to centralize and analyze sales data, which is currently spread across multiple sources. This project provides a solution to store the data in a centralized database, acting as a single source of truth. It also offers functionalities to query the database and generate up-to-date metrics for the business.

## Dependencies
The following packages need to be installed first:
- Python - for managing the data, and calling the other packages
- Pandas - for working with DataFrames
- YAML - for reading and parsing YAML files
- SQLAlchemy - for database connectivity and operations
- Requests - for making API requests
- Tabula - for extracting data from PDF files
- Boto3 - for interacting with Amazon S3

## How to run
1. Create the _sales_data_ database using pgAdmin
2. Install the dependencies listed above using 'pip install' or 'conda install'
3. Run _main.py_ to extract, clean and upload the data into tables in the _sales_data_ database
4. Run the SQL table transformations (using pgAdmin or VSCode) in the following order:
    1. _orders_table_to_type.sql_
    2. _dim_users_table_to_type.sql_
    3. _dim_store_details_to_type.sql_
    4. _dim_products_add_weight_class.sql_
    5. _dim_products_to_type.sql_
    6. _dim_date_times_to_type.sql_
    7. _dim_card_details_to_type.sql_
    8. _add_primary_keys.sql_
    9. _add_foreign_keys.sql_
5. Run _SQL_queries.sql_ (using pgAdmin or VSCode) to perform the queries which provide the required metrics (see below)

## Python files & libraries

#### 1. main
- This coordinates and runs the code in the other libraries. It performs no downloading, cleaning or uploading of data itself

#### 2. database_utils
- This initialises and connects to the AWS RDS and PostgreSQL databases. It reads the necessary credentials from YAML files

``` python
def init_db_engine(self):
        data = self.read_db_creds() # reads the YAML credentials file
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['HOST']
        PASSWORD = data['PASSWORD']
        USER = data['USER']
        DATABASE = data['DATABASE']
        PORT = data['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
```

#### 3. data_extraction
- This extracts the data sets, which are in various formats (CSV, PDF, JSON), from the sources mentioned above into Pandas dataframes (see _Data sets,..._ below)

#### 4. data_cleaning
- This creates a DataCleaning class, and uses its methods to clean the Pandas dataframes by calling the functions in the _data_transformations_ library

#### 5. data_transformations
- This does the bulk of the heavy lifting to transform the dataframes. It contains several functions e.g. to remove NULL values, remove erroneous data, convert dates and phones numbers, check emails, and convert numbers to integers or floating point as appropriate

Example:

``` python
def convert_to_integer(df, col):
    '''Extracts numeric characters only from column
    (using Regex) to convert to an integer.'''
    df[col] = df[col].str.extract(pat='(\d+)', expand=False)
    df[col] = df[col].astype('int64')
    print(f"'{col}' has been converted to integer")
    return df
```

## Data sets, sources, formats, extraction methods

The data sets provided for analysis include:
- **dim_card_details** - over 15,000 payment card details, sourced from S3 as a multi-page PDF, and read into a Pandas Dataframe using Tabula

```python
def retrieve_pdf_data(self, pdf_path):
    dfs = tabula.read_pdf(pdf_path, pages='all')
    combined_df = []
    for n in range(len(dfs)):
        combined_df.append(dfs[n]) # each page in the pdf <dfs[n]> is a df
    # concatenate all the dataframes into one
    return pd.concat(combined_df, ignore_index=True) 
```

- **dim_date_times** - maps 120,000 date IDs to dates and times of day, extracted from S3 to JSON using Boto3

```python
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
```

- **dim_products** - catalogue of 1,800 products, extracted from S3 to CSV using Boto3
- **dim_store_details** - details of 440 stores, sourced from AWS via an API
- **dim_users** - contact details of 15,000 customers, sourced from RDS

``` python
    def read_rds_table(self, table):
        '''Reads an AWS RDS database into a Pandas Dataframe.'''
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            return pd.read_sql_table(table, conn, index_col='index')
```

- **orders_table** - central table with details of over 120,000 customer orders, sourced from RDS

## PostgreSQL database schema and queries

The following star-based schema is created, with the _orders_table_ at the centre:

![database schema](https://github.com/maneesh-jain/multinational-retail-data-centralisation/blob/main/ERD.png?raw=true)

The following business queries are then answered by running _SQL_queries.sql_

1. How many stores does the business have and in which countries?
2. Which locations currently have the most stores?
3. Which months produce the average highest cost of sales typically?
4. How many sales are coming from online? 

``` postgresql
SELECT COUNT(store_code) AS numbers_of_sales, SUM(product_quantity) as product_quantity_count, 
CASE
    WHEN store_code = 'WEB-1388012W' THEN 'Web'
ELSE 'Offline'
END AS location
FROM orders_table
GROUP BY location;
```

5. What percentage of sales come through each type of store? 

``` postgresql
SELECT  store_type, 
        COUNT(store_type) AS total_sales, 
        COUNT(store_type) * 100 / 
            (SELECT COUNT(*) FROM orders_table) AS "percentage_total(%)" 
            -- sub-query above executes before the GROUP BY
FROM orders_table
JOIN dim_store_details
    ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC;
```

6. Which month in each year produced the highest cost of sales? 
7. What is our staff headcount? 
8. Which German store type is selling the most? 
9. How quickly is the company making sales? 
