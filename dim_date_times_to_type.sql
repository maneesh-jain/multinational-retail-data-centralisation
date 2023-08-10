-- Casts dim_date_times columns to correct data types

ALTER TABLE dim_date_times
    ALTER COLUMN date TYPE DATE;

ALTER TABLE dim_date_times
    ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
