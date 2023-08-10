# Multinational Retail Data Centralisation

## Objective
To extract, clean, upload and analyse data sets relating to a (dummy) multi-national retailer

## Philosophy
My goal was to write code that is easy to read and follow, whilst applying the DRY (don't repeat yourself) principle. To that end, functions and methods are re-used wherever possible

## Stages
The project so far (10 August 2023) has the following stages:
1. Download various data sets using Python (see below) from AWS RDS and S3
2. Clean the data in Python using relevant libraries such as Pandas, Numpy, Datetime, Regex
3. Upload the cleaned data sets to tables in a PostgreSQL database
4. Refactor the code to improve readability and add a function to clean card_numbers in dim_card_details (see below)
5. Clean PostgreSQL database: alter tables to update column types, add primary and foreign keys to create a star-based database schema

## Python files & libraries (created by the author)

#### 1. main
- This coordinates and runs the code in the other libraries. It performs no downloading, cleaning or uploading of data itself

#### 2. database_utils
- This initialises and connects to the AWS and PostgreSQL databases. It provides the necessary credentials and API keys

#### 3. data_extraction
- This extracts the data sets, which are in various formats (csv, pdf, json), from the sources mentioned above into Pandas dataframes

#### 4. data_cleaning
- This creates a DataCleaning class, and uses its methods to clean the Pandas dataframes by calling the functions in the data_transformations library (below)

#### 5. data_transformations
- This does the bulk of the heavy lifting to transform the dataframes. It contains several functions e.g. to remove NULL values, remove erroneous data, convert dates and phones numbers, check emails, and convert numbers to integers or floating point as appropriate

## Data sets

The data sets provided for analysis include:
- **dim_card_details** - over 15,000 payment card details
- **dim_date_times** - maps 120,000 date IDs to dates and times of day
- **dim_products** - catalogue of 1,800 products
- **dim_store_details** - details of 440 stores
- **dim_users** - contact details of 15,000 customers
- **orders_table** - central table with details of over 120,000 customer orders
