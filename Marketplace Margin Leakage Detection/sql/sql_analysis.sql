create database marketplace_margin_leakage;
use marketplace_margin_leakage;

select  * from olist_customers_dataset_cleaned;
select  * from olist_geolocation_dataset_cleaned;
select  * from olist_order_items_dataset_cleaned;
select  * from olist_order_payments_dataset_cleaned;
select  * from olist_order_reviews_dataset_cleaned;
select  * from olist_orders_dataset_cleaned;
select  * from olist_products_dataset_cleaned;
select  * from olist_sellers_dataset_cleaned;
select  * from product_category_name_translation_cleaned;

CREATE OR REPLACE VIEW vw_customers AS
SELECT *
FROM olist_customers_dataset_cleaned;

CREATE OR REPLACE VIEW vw_geolocation AS
SELECT *
FROM olist_geolocation_dataset_cleaned;

CREATE OR REPLACE VIEW vw_order_items AS
SELECT *
FROM olist_order_items_dataset_cleaned;

CREATE OR REPLACE VIEW vw_order_payments AS
SELECT *
FROM olist_order_payments_dataset_cleaned;

CREATE OR REPLACE VIEW vw_order_reviews AS
SELECT *
FROM olist_order_reviews_dataset_cleaned;

CREATE OR REPLACE VIEW vw_orders AS
SELECT *
FROM olist_orders_dataset_cleaned;

CREATE OR REPLACE VIEW vw_products AS
SELECT *
FROM olist_products_dataset_cleaned;

CREATE OR REPLACE VIEW vw_sellers AS
SELECT *
FROM olist_sellers_dataset_cleaned;

CREATE OR REPLACE VIEW vw_product_category_translation AS
SELECT *
FROM product_category_name_translation_cleaned;

select * from vw_customers;
select * from vw_order_items;
select * from vw_order_payments;
select * from vw_order_reviews;
select * from vw_orders;	
select * from vw_products;
select * from vw_sellers;

select sum(price) as total_revenue from vw_order_items;