import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings

# information:

def table_info(df):
    print(f"\nNo. of duplicates: {df.duplicated().sum()}\n") # sums where duplicates = True (boolean)
    print(df.info())
    df

def col_info(df, col):
    print(f"{col}: \n{df[col].describe()}\n")
    df[col].unique()

def find_uppercase(df, col):
    mask = pd.Series()
    for ind in df.index:
        value = str(df.loc[ind, col])
        if value.isupper():
            mask[ind] = True
        else:
            mask[ind] = False
    return mask

def show_uppercase(df, col):
    mask = find_uppercase(df, col)
    if df[mask].empty:
        print(f"Nothing found in '{col}' in UPPERCASE")
    else:
        df[mask]

def contains_NaN(df, col):
    print(f"Number of NaN/Null values in '{col}' = {df[col].isnull().sum()}")

# transformation:

def del_rows_with_uppercase(df, col):
    mask = find_uppercase(df, col)
    if df[mask].empty:
        print(f"Nothing found in '{col}' in UPPERCASE")
    else:
        df.drop(df[mask].index, inplace=True)
        print(f"Rows with '{col}' in UPPERCASE have been deleted")
    return df

def del_rows_with_string(df, col, strng, present=True):
    mask = df[col].str.contains(strng)
    if present == True:
        df.drop(df[mask].index, inplace=True) # drop where <strng> present
        cond = 'with'
    else:
        df.drop(df[~mask].index, inplace=True) # drop where <strng> NOT present; ~ means NOT
        cond = 'without'
    print(f"Rows in '{col}' {cond} '{strng}' deleted")
    return df

def replace_string(df, col, strng1, strng2):
    df[col].replace(strng1, strng2, inplace=True)
    print(f"'{strng1}' has been replaced with '{strng2}' in '{col}'")
    return df

def clean_df(df, columns_to_clean):
    for k, v in columns_to_clean.items():
        clean_user_data(df, k, v)
    return df

def clean_user_data(df, col, output_type):

    if output_type == 'float':
        # extract numeric characters and '.':
        df[col] = df[col].str.extract(pat='(\d+\.\d+)', expand=False)
        df[col] = df[col].astype('float64')
        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'integer':
        # extract numeric characters:
        df[col] = df[col].str.extract(pat='(\d+)', expand=False)
        df[col] = df[col].astype('int64')
        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'date':
        warnings.filterwarnings('ignore')
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
        # # above code gives deprecation warning, so just in case it *is* deprecated
        # # the code below parses each format individually:
        # for ind in df.index:
        #     if '-' in df.loc[ind, col]:
        #         df.loc[ind, col] = pd.to_datetime(df.loc[ind, col], format="%Y-%m-%d")
        #     elif '/' in df.loc[ind, col]:
        #         df.loc[ind, col] = pd.to_datetime(df.loc[ind, col], format="%Y/%m/%d")
        #     elif df.loc[ind, col][:4].isnumeric():
        #         df.loc[ind, col] = datetime.strptime(df.loc[ind, col], "%Y %B %d")
        #     else:
        #         df.loc[ind, col] = datetime.strptime(df.loc[ind, col], "%B %Y %d")
        # df[col] = pd.to_datetime(df[col]) # convert the different datetimes to one
        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'end_of_month':
        warnings.filterwarnings('ignore')
        for ind in df.index:
            # find 1st of following month
            mm = int(df.loc[ind, col][:2]) + 1
            yyyy = int(df.loc[ind, col][3:]) + 2000 # convert yy to yyyy
            if mm == 13:
                mm = 1
                yyyy = yyyy + 1
            # subtract 1 day to get end of previous (i.e. desired) month:
            df.loc[ind, col] = datetime(yyyy, mm, 1) - timedelta(days=1)
        df[col] = pd.to_datetime(df[col]) # datetime.datetime to Pandas datetime
        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'category':
        df[col] = df[col].astype('category')
        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'delete_column':
        try:
            df.drop(col, axis=1, inplace=True)
            print(f"'{col}' has been deleted")
        except:
            print(f"{col} is missing")

    elif output_type == 'email':
        mask = df[col].apply(lambda x: "@" not in x and "." not in x) 
        if df[mask].empty:
            print("All emails contain '@' and '.'")
        else:
            print(f"Invalid emails:\n{df[mask]}")

    elif output_type == 'phone':
        for ind in df.index:

            if df.loc[ind, 'country_code'] == 'GB':

                for i in [' ', '(', ')', '+440', '+44']:
                    df.loc[ind, col] = df.loc[ind, col].replace(i, '')

                if df.loc[ind, col][:1] == '0':
                    df.loc[ind, col] = df.loc[ind, col][1:]

                df.loc[ind, col] = '0044' + df.loc[ind, col]

                if len(df.loc[ind, col]) != 14:
                    df.loc[ind, col] = ''

            if df.loc[ind, 'country_code'] == 'US':

                for i in [' ', '-', '.', '+1', '001']:
                    df.loc[ind, col] = df.loc[ind, col].replace(i, '')

                if 'x' in df.loc[ind, col]:
                    number = df.loc[ind, col].split('x')[0]
                    ext = df.loc[ind, col].split('x')[1]
                    df.loc[ind, col] = number
                    df.loc[ind, 'extension'] = ext

                df.loc[ind, col] = '001' + df.loc[ind, col]

                if len(df.loc[ind, col]) != 13:
                    df.loc[ind, col] = ''
                    # df.loc[ind, 'extension'] = '' # commented in case want to keep extension pending correct number?

            if df.loc[ind, 'country_code'] == 'DE':

                for i in [' ', '(', ')', '+490', '+49']:
                    df.loc[ind, col] = df.loc[ind, col].replace(i, '')

                if df.loc[ind, col][:1] == '0':
                    df.loc[ind, col] = df.loc[ind, col][1:]

                df.loc[ind, col] = '0049' + df.loc[ind, col]
                # no length check as German numbers vary by district

        print(f"'{col}' has been converted to {output_type}")

    elif output_type == 'uppercase':
        df[col] = df[col].str.upper()
        print(f"'{col}' has been converted to {output_type}")

    return df

def convert_weight(df, col):
    for ind in df.index:
        current_weight = df.loc[ind, col]
        if type(current_weight) != float:
            current_weight = current_weight.replace(' ','')
            if 'kg' in current_weight:
                amount = float(current_weight[:-2])
            elif 'x' in current_weight and 'g' in current_weight:
                amounts = current_weight.split('x')
                amount = float(amounts[0]) * float(amounts[1][:-1])/1000
            elif 'g.' in current_weight:
                amount = float(current_weight[:-2])/1000
            elif 'g' in current_weight:
                amount = float(current_weight[:-1])/1000
            elif 'ml' in current_weight:
                amount = float(current_weight[:-2])/1000
            elif 'oz' in current_weight:
                amount = float(current_weight[:-2])/35.274
            else:
                amount = np.nan # delete non-numeric
                print(f"{current_weight} deleted from '{col}'")
        else:
            amount = current_weight
        df.loc[ind, col] = round(float(amount), 3)
    df[col] = df[col].astype('float64')
    print(f"'{col}' has been converted to kg")
    return df

def combine_date(df):
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    print(f"'year', 'month', 'day' have been combined into a new 'date' column")
    return df
