-- Casts dim_card_details columns to correct data types

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date TYPE DATE;

ALTER TABLE dim_card_details
    ALTER COLUMN date_payment_confirmed TYPE DATE;
