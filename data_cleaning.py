import data_transformations as dt

class DataCleaning:

    def __init__(self, df):
        self.df = df

    def clean_store_details(self):
        print("CLEANING 'store_details'...")
        dt.del_rows_with_string(self.df, 'store_code', '-', False)
        dt.replace_string(self.df, 'continent', 'eeEurope', 'Europe')
        dt.replace_string(self.df, 'continent', 'eeAmerica', 'America')
        columns_to_clean = {
                                'longitude':'float',
                                'lat':'delete_column',
                                'staff_numbers':'integer',
                                'opening_date':'date',
                                'store_type':'category',
                                'latitude':'float',
                                'country_code': 'category',
                                'continent':'category'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df
    
    def clean_users(self):
        print("CLEANING 'users'...")
        dt.del_rows_with_string(self.df,'first_name', 'NULL', True)
        dt.del_rows_with_uppercase(self.df, 'first_name')
        dt.replace_string(self.df, 'country_code', 'GGB', 'GB')
        columns_to_clean = {
                                'date_of_birth':'date',
                                'email_address':'email',
                                'country':'category',
                                'country_code':'category',
                                'join_date':'date',
                                'phone_number':'phone'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df

    def clean_orders_table(self):
        print("CLEANING 'orders_table'...")
        clean_df = dt.clean_df(self.df, {'product_code':'uppercase'})
        columns_to_clean = {
                                'store_code':'category',
                                'product_code':'category',
                                '1':'delete_column'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df

    def clean_card_data(self):
        print("CLEANING 'card_df'...")
        dt.del_rows_with_string(self.df,'expiry_date', 'NULL', True)
        dt.del_rows_with_uppercase(self.df, 'expiry_date')
        columns_to_clean = {
                                'card_provider':'category',
                                'date_payment_confirmed':'date',
                                'expiry_date':'end_of_month'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df

    def convert_product_weights(self):
        print("CONVERTING product 'weight'...")
        clean_df = dt.convert_weight(self.df, 'weight')
        return clean_df

    def clean_products_data(self):
        print("CLEANING 'product_details'...")
        dt.del_rows_with_uppercase(self.df, 'removed')
        dt.replace_string(self.df, 'removed', 'Still_avaliable', 'Still_available')
        columns_to_clean = {
                                'product_price':'float',
                                'category':'category',
                                'EAN':'category',
                                'date_added':'date',
                                'removed':'category',
                                'product_code':'uppercase'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df

    def clean_date_details(self):
        print("CLEANING 'date_details'...")
        dt.del_rows_with_uppercase(self.df, 'time_period')
        dt.combine_date(self.df)
        columns_to_clean = {
                                'month':'delete_column',
                                'year':'delete_column',
                                'day':'delete_column',
                                'time_period':'category'
                                }
        clean_df = dt.clean_df(self.df, columns_to_clean)
        print("COMPLETED\n")
        return clean_df
