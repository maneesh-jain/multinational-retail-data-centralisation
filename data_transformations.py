from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import warnings

# information functions:

def frame_info(df):
    '''Provides dataframe information including the number of duplicates.'''
    print(f"\nNo. of duplicates: {df.duplicated().sum()}\n") # sums where duplicates = True (boolean)
    print(df.info())
    print(df)

def column_info(df, col):
    '''Provides information about a column including the set of unique values.'''
    print(f"{col}: \n{df[col].describe()}\n")
    print(f"Unique values:\n{df[col].unique()}")

def show_NaN(df, col):
    '''Displays the NaN/Null values in a column to aid data analysis.'''
    print(f"Number of NaN/Null values in '{col}' = {df[col].isnull().sum()}")

def show_uppercase(df, col):
    '''
    Displays the uppercase (alphanumeric) values in a column, 
    to aid data analysis. 
    '''
    mask = find_uppercase(df, col)
    if df[mask].empty:
        print(f"Nothing found in '{col}' in UPPERCASE")
    else:
        print(df[mask])

def find_uppercase(df, col):
    '''
    Finds the uppercase (alphanumeric) values in a column. 
    Python .isupper() is used instead of (the possibly more convenient) 
    Pandas .str.isupper() as the latter excludes numbers.
    Returns a 'mask' (Boolean Pandas series) used by other functions.
    '''
    mask = pd.Series()
    for ind in df.index:
        value = str(df.loc[ind, col])
        if value.isupper():
            mask[ind] = True
        else:
            mask[ind] = False
    return mask

# transformation functions:

def delete_rows_with_uppercase(df, col):
    '''
    Deletes rows with only uppercase (alphanumeric) characters. 
    WARNING: Use only after rows have been checked with
    'show_uppercase' method.
    '''
    mask = find_uppercase(df, col)
    if df[mask].empty:
        print(f"Nothing found in '{col}' in UPPERCASE")
    else:
        df.drop(df[mask].index, inplace=True)
        print(f"Rows with '{col}' in UPPERCASE have been deleted")
    return df

def delete_rows_with_string(df, col, strng, present=True):
    '''
    Deletes rows where a column does/not contain a specified string.
    Default: present=True, deletes where string present.
    Use: present=False, to delete where string absent.
    '''
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
    '''Replaces string 1 with string 2 in specified column.'''
    df[col].replace(strng1, strng2, inplace=True)
    print(f"'{strng1}' has been replaced with '{strng2}' in '{col}'")
    return df

def convert_to_float(df, col):
    '''Extracts numeric characters and '.' from column
    (using Regex) to convert to a floating point number.'''
    df[col] = df[col].str.extract(pat='(\d+\.\d+)', expand=False)
    df[col] = df[col].astype('float64')
    print(f"'{col}' has been converted to float")
    return df

def convert_to_integer(df, col):
    '''Extracts numeric characters only from column
    (using Regex) to convert to am integer.'''
    df[col] = df[col].str.extract(pat='(\d+)', expand=False)
    df[col] = df[col].astype('int64')
    print(f"'{col}' has been converted to integer")
    return df

def convert_to_date(df, col):
    '''Converts column to Pandas Datetime format.'''
    warnings.filterwarnings('ignore')
    df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
    print(f"'{col}' has been converted to date")
    return df

def convert_to_end_of_month(df, col):
    '''Converts date format mm/yy to yyyy/mm/dd 
    where dd is the last day of the month.'''
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
    print(f"'{col}' has been converted to end of the month")
    return df

def convert_to_category(df, col):
    '''Converts column to Pandas 'category' type.'''
    df[col] = df[col].astype('category')
    print(f"'{col}' has been converted to category")
    return df

def delete_column(df, col):
    '''Deletes selected column.'''
    try:
        df.drop(col, axis=1, inplace=True)
        print(f"'{col}' has been deleted")
    except:
        print(f"{col} is missing")
    return df

def check_emails(df, col):
    '''Checks all emails have '@' and '.' present.'''
    mask = df[col].apply(lambda x: "@" not in x and "." not in x) 
    if df[mask].empty:
        print("All emails contain '@' and '.'")
    else:
        print(f"Invalid emails:\n{df[mask]}")
    return df

def clean_phone_numbers(df, col):
    '''
    Cleans phone numbers according to 'country_code' column.
    Converts them to international format, i.e.
    00 + country code + local number (without leading zero).
    '''
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
                # df.loc[ind, 'extension'] = '' # commented in case want to keep extension pending correct number

        if df.loc[ind, 'country_code'] == 'DE':

            for i in [' ', '(', ')', '+490', '+49']:
                df.loc[ind, col] = df.loc[ind, col].replace(i, '')

            if df.loc[ind, col][:1] == '0':
                df.loc[ind, col] = df.loc[ind, col][1:]

            df.loc[ind, col] = '0049' + df.loc[ind, col]
            # no length check as German numbers vary by district

    print(f"Phone numbers in '{col}' have been cleaned up")
    return df

def convert_to_uppercase(df, col):
    '''Converts column to UPPERCASE.'''
    df[col] = df[col].str.upper()
    print(f"'{col}' has been converted to UPPERCASE")
    return df

def clean_card_numbers(df, col):
    '''
    Deletes card numbers which contain '?'
    '''
    deleted_count = 0
    for ind in df.index:
        value = df.loc[ind, col]
        if type(value) == str and '?' in value:
            df.drop(ind, axis=0, inplace=True)
            deleted_count += 1
            continue

    print(f"{deleted_count} rows deleted due to incorrect '{col}'")
    df[col] = df[col].astype('int64')
    return df

def convert_weight(df, col):
    '''Converts weights to floating point kg and removes units.'''
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
    '''Combines 'year', 'month', 'day' columns into a new 'date' column.'''
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    print(f"'year', 'month', 'day' have been combined into a new 'date' column")
    return df
