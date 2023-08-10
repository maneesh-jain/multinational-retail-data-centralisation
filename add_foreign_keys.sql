-- Adds foriegn keys to existing tables

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_card_number_fkey FOREIGN KEY (card_number)
    REFERENCES dim_card_details (card_number)
    ON UPDATE NO ACTION -- stops invalid updates
    ON DELETE NO ACTION -- stop invalid deletes
    NOT VALID; -- don't enforce contraints on existing records (unless updated)

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_date_uuid_fkey FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times (date_uuid)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_product_code_fkey FOREIGN KEY (product_code)
    REFERENCES dim_products (product_code)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_store_code_fkey FOREIGN KEY (store_code)
    REFERENCES dim_store_details (store_code)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_user_uuid_fkey FOREIGN KEY (user_uuid)
    REFERENCES dim_users (user_uuid)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
