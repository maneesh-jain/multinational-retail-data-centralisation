import data_transformations as dt

class DataCleaning:

    def __init__(self, df):
        self.df = df

    def clean_users(self):
        '''Cleans users details.'''
        print("CLEANING 'users'...")
        dt.delete_rows_with_string(self.df,'first_name', 'NULL', True)
        dt.delete_rows_with_uppercase(self.df, 'first_name')
        dt.convert_to_date(self.df, 'date_of_birth')
        dt.check_emails(self.df, 'email_address')
        dt.convert_to_category(self.df, 'country')
        dt.replace_string(self.df, 'country_code', 'GGB', 'GB')
        dt.convert_to_category(self.df, 'country_code')
        dt.clean_phone_numbers(self.df, 'phone_number')
        dt.convert_to_date(self.df, 'join_date')
        print("COMPLETED\n")
        return self.df

    def clean_orders_table(self):
        '''Cleans orders table.'''
        print("CLEANING 'orders_table'...")
        dt.convert_to_category(self.df, 'store_code')
        dt.convert_to_uppercase(self.df, 'product_code')
        dt.convert_to_category(self.df, 'product_code')
        dt.delete_column(self.df, '1')
        print("COMPLETED\n")
        return self.df

    def clean_store_details(self):
        '''Cleans store details.'''
        print("CLEANING 'store_details'...")
        dt.delete_rows_with_string(self.df, 'store_code', '-', False)
        dt.convert_to_float(self.df, 'longitude')
        dt.delete_column(self.df, 'lat')
        dt.convert_to_integer(self.df, 'staff_numbers')
        dt.convert_to_date(self.df, 'opening_date')
        dt.convert_to_category(self.df, 'store_type')
        dt.convert_to_float(self.df, 'latitude')
        dt.convert_to_category(self.df, 'country_code')
        dt.replace_string(self.df, 'continent', 'eeEurope', 'Europe')
        dt.replace_string(self.df, 'continent', 'eeAmerica', 'America')
        dt.convert_to_category(self.df, 'continent')
        print("COMPLETED\n")
        return self.df
    
    def clean_card_data(self):
        '''Cleans card data.'''
        print("CLEANING 'card_df'...")
        dt.delete_rows_with_string(self.df,'expiry_date', 'NULL', True)
        dt.delete_rows_with_uppercase(self.df, 'expiry_date')
        dt.convert_to_end_of_month(self.df, 'expiry_date')
        dt.convert_to_category(self.df, 'card_provider')
        dt.convert_to_date(self.df, 'date_payment_confirmed')
        dt.clean_card_numbers(self.df, 'card_number')
        print("COMPLETED\n")
        return self.df

    def convert_product_weights(self):
        '''Converts 'weight' column to floating point kg.'''
        print("CONVERTING product 'weight'...")
        dt.convert_weight(self.df, 'weight')
        return self.df

    def clean_products_data(self):
        '''Cleans products data.'''
        print("CLEANING 'product_details'...")
        dt.delete_rows_with_uppercase(self.df, 'removed')
        dt.replace_string(self.df, 'removed', 'Still_avaliable', 'Still_available')
        dt.convert_to_float(self.df, 'product_price')
        dt.convert_to_category(self.df, 'category')
        dt.convert_to_category(self.df, 'EAN')
        dt.convert_to_date(self.df, 'date_added')
        dt.convert_to_category(self.df, 'removed')
        dt.convert_to_uppercase(self.df, 'product_code')
        print("COMPLETED\n")
        return self.df

    def clean_date_details(self):
        '''Cleans date details.'''
        print("CLEANING 'date_details'...")
        dt.delete_rows_with_uppercase(self.df, 'time_period')
        dt.combine_date(self.df)
        dt.delete_column(self.df, 'month')
        dt.delete_column(self.df, 'year')
        dt.delete_column(self.df, 'day')
        dt.convert_to_category(self.df, 'time_period')
        print("COMPLETED\n")
        return self.df

if __name__ == "__main__":
    pass
