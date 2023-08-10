-- Casts dim_users_table columns to correct data types

ALTER TABLE dim_users_table
    ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users_table
    ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users_table
    ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE dim_users_table
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_users_table
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

ALTER TABLE dim_users_table
    ALTER COLUMN join_date TYPE DATE;
