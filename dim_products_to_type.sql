-- Casts dim_products columns to correct data types

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT;

ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT;

ALTER TABLE dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products
    RENAME removed to still_available

-- Change still_available column to Boolean values before changing type
UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_Available' THEN True
ELSE False
END;

ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE BOOL;
