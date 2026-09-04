select * from clean_dataset;

-- A . KPI -- 
-- 1. total patients -- 
select count(distinct patient_name) as total_patient from clean_dataset;

-- 2 . total doctores -- 
select count(distinct doctor_name) as total_doctor from clean_dataset;

-- 3 .total hospital -- 
select count(distinct hospital_name) as total_hospital from clean_dataset;

-- 4 . avg_admit_days --
select round(avg(no_of_day_admit),2) as avg_admit_day from clean_dataset;


-- B. sql analysis -- 
-- 1 . Which medical condition has the highest average billing amount?
select medical_condition , round(avg(billing_amount),2) as avg_bill from clean_dataset
group by medical_condition order by avg_bill desc;

-- 2 . Which insurance provider brings in the most total revenue?
select insurance_provider , round(sum(billing_amount),2) as total_revenue from clean_dataset
group by insurance_provider order by total_revenue desc;

-- 3 . What's the average length of stay (no_of_day_admit) by admission_type?
select admission_type , round(avg(no_of_day_admit),2) as avg_admit_day
from clean_dataset group by admission_type order by avg_admit_day desc;

-- 4 . Which age_group has the highest share of "Abnormal" test results?
select age_group , count(case when test_results = 'Abnormal' then 1 end) as abanormal_count , 
count(*) as total , 
round(count(case when test_results = 'Abnormal' then 1 end) * 100.0 /
count(*) , 2) as pct_of_total
from clean_dataset group by age_group order by pct_of_total desc;

-- 5 . Top 5 doctors by total number of patients treated?
select doctor_name , count(distinct patient_name) as total_patient from clean_dataset
group by doctor_name order by total_patient desc limit 5;

-- 6 . Which hospital's average billing is highest, and by how much does it exceed the overall average?
with higest_bill as (select hospital_name  , round(avg(billing_amount),2) as avg_bill from clean_dataset
group by hospital_name order by avg_bill desc limit 1) , 
overall_avg as ( select  round(avg(billing_amount),2) as overall_avg_bill from clean_dataset)
select h.hospital_name , h.avg_bill as higest_avg_bill , 
o.overall_avg_bill , round(h.avg_bill - o.overall_avg_bill) as exceed_by from higest_bill h
cross join overall_avg o ;

-- 7 . For each medical_condition, which doctor treats the most patients?
with doctor_rnk as (select medical_condition , doctor_name , 
count(distinct patient_name) as total_patient , 
row_number() over(partition by medical_condition order by count(distinct patient_name) desc) as higest_treat 
from clean_dataset
group by medical_condition , doctor_name)
select * from doctor_rnk where higest_treat = 1;

-- 8 . Which month/year had the highest number of admissions — any seasonal pattern?
select year(date_of_admission) as admit_year ,
 month(date_of_admission) as admit_month , count(distinct patient_name) as total_admission 
from clean_dataset group by  year(date_of_admission) ,
month(date_of_admission) 
order by  total_admission desc;

-- 9 . Which blood type shows the highest occurrence of each medical condition — any risk pattern worth flagging?
with ranked as (select medical_condition , blood_type , count(*) as total_record , 
dense_rank() over(partition by medical_condition order by count(*) desc) as rnk
from clean_dataset group by medical_condition , blood_type)
select * from ranked where rnk = 1 order by medical_condition , blood_type;

-- 10 . Which patients are billed above the 90th percentile — who are the high-cost outliers?
with ranked as (select patient_name , PERCENT_RANK() over(order by billing_amount) as pct_rank
from clean_dataset)
select * from ranked where pct_rank > 0.9;

-- 11 . . What's the year-over-year growth in total billing revenue?
with yearly_revenue as (select year(date_of_admission) as admission_year , 
round(sum(billing_amount),2) as total_bill_revenue , 
lag(round(sum(billing_amount),2)) over(order by year(date_of_admission)) as previous_year_bill_revenue
from clean_dataset group by year(date_of_admission))
select * , round((total_bill_revenue - previous_year_bill_revenue) / nullif(previous_year_bill_revenue , 0) * 100.0 , 2)
as revenue_growth from  yearly_revenue;

-- 12 . Running (cumulative) total of billing amount over time, month by month.
with monthly_revenue as (
select month(date_of_admission) as admit_month , round(sum(billing_amount),2) as total_bill_revenue
from clean_dataset group by  month(date_of_admission))
select admit_month  , total_bill_revenue , 
round(sum(total_bill_revenue) over(order by admit_month) ,2) as cumulative_revenue
from monthly_revenue;