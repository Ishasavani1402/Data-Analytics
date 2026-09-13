create database sales;
use sales;
select * from sales_clean_data;

-- 1 .total dataset 
select count(*) as total_rows from sales_clean_data;

-- 2 . for each year each month total revenue
with sales as (select year , month(order_date) as order_month,
monthname(order_date) as order_month_name , 
round(sum(total_sales_amount),2) as total_sales from sales_clean_data group by year , order_month , 
order_month_name)
select * , row_number() over(partition by year order by order_month) as total_sale_rnk
from sales; 

-- 3 . for each product category which sub category generate high revenue??
WITH sale_rank as (select product_category , sub_category , round(sum(total_sales_amount),2) as total_sale , 
dense_rank() over(partition by product_category order by round(sum(total_sales_amount),2) desc) as sale_rnk
from sales_clean_data group by product_category , sub_category)
select * from sale_rank where sale_rnk = 1;

-- 4 . for each subcategory top 3 product which have higest total_purchasing_amounts??
with total_rnk as (select sub_category , product_name , round(sum(total_purchasing_amounts),2) as total_purchase_amount ,
dense_rank() over(partition by sub_category order by round(sum(total_purchasing_amounts),2) desc) as purchase_amnt_rnk
from sales_clean_data group by sub_category , product_name)
select * from total_rnk where purchase_amnt_rnk <= 3;

-- 5 . for each year montly net profit 
with netprofit as (select year , month(order_date) as order_month,
monthname(order_date) as order_month_name , 
round(sum(net_profit),2) as total_net_profit from sales_clean_data group by year , order_month , 
order_month_name)
select * , row_number() over(partition by year order by order_month) as total_profit_rnk
from netprofit; 

-- 6 . cumulative sales
with sales as (select year , month(order_date) as order_month,
monthname(order_date) as order_month_name , 
round(sum(total_sales_amount),2) as total_sales from sales_clean_data group by year , order_month , 
order_month_name)
select * , round(sum(total_sales) over(partition by year order by order_month),2) as cumulative_sum
from sales; 

-- 7 . avg profit margin distribution across sub category
select sub_category , round(avg(`profit_margin_%`),2) as avg_profit_margin 
from sales_clean_data group by sub_category order by avg_profit_margin desc;

-- 8 . in each year which day where no order placed??
SET SESSION cte_max_recursion_depth = 5000;
WITH RECURSIVE calendar AS (
    SELECT MIN(DATE(order_date)) AS cal_date
    FROM sales_clean_data
    UNION ALL
    
    SELECT cal_date + INTERVAL 1 DAY
    FROM calendar
    WHERE cal_date < (
        SELECT MAX(DATE(order_date))
        FROM sales_clean_data))
SELECT
    YEAR(c.cal_date) AS year,
    c.cal_date AS missing_order_date
FROM calendar c
LEFT JOIN sales_clean_data s
    ON c.cal_date = DATE(s.order_date)
WHERE s.order_id IS NULL
ORDER BY
    year,
    missing_order_date;

-- 9. each year which customer has higest total spent 
with total_spent as (select year , customer_id , customer_name , 
round(sum(total_sales_amount),2) as total_spent , 
dense_rank() over(partition by year order by  round(sum(total_sales_amount),2) desc) as hige_spent_rnk
from sales_clean_data group by year , customer_id ,customer_name)
select * from total_spent where hige_spent_rnk = 1;

-- 10 . year over year total sale growth
with sales as (select year , round(sum(total_sales_amount),2) as total_sale , 
lag(round(sum(total_sales_amount),2)) over(order by year) as previous_year_sale
from sales_clean_data 
group by year)
select * , round(((total_sale - previous_year_sale) / nullif(previous_year_sale , 0)) * 100.0  ,2) as growth_pct
from sales;
-- 11 . year over year net profit growth


