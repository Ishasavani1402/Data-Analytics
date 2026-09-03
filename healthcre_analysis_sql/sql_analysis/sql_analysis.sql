select * from clean_dataset;

-- A . KPI -- 
-- 1. total patients -- 
select count(distinct patient_name) as total_patient from clean_dataset;

-- 2 . total doctores -- 
select count(distinct doctor_name) as total_doctor from clean_dataset;

-- 3 .total hospital -- 
select count(distinct hospital_name) as total_hospital from clean_dataset;


-- B. sql analysis -- 
-- 1 . age group wise medical_condition distribution --
select medical_condition , age_group , count(distinct patient_name) as total_patient from clean_dataset
group by medical_condition , age_group order  by total_patient desc; 





