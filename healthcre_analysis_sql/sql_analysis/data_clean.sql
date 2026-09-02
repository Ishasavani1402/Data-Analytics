select * from healthcare_dataset;
SET SQL_SAFE_UPDATES = 0;

-- 1. total record --
select count(*) as total_record from healthcare_dataset;

-- 2 . show data types --
describe healthcare_dataset;

-- 3. change column name --
alter table healthcare_dataset rename column name to patient_name;
alter table healthcare_dataset rename column hospital to hospital_name;
alter table healthcare_dataset rename column doctor to doctor_name;

-- 4 . check for null values -- 
SELECT
SUM(patient_name IS NULL) patient_name,
SUM(age IS NULL) age,
SUM(gender IS NULL) gender,
SUM(blood_type IS NULL) blood_type,
SUM(medical_condition IS NULL) medical_condition,
sum(date_of_admission is null) date_of_admission , 
SUM(doctor_name IS NULL) doctor_name,
SUM(hospital_name IS NULL) hospital_name,
SUM(insurance_provider IS NULL) insurance_provider,
SUM(billing_amount IS NULL) billing_amount,
SUM(room_number IS NULL) room_numer,
SUM(admission_type IS NULL) admission_type,
SUM(discharge_date IS NULL) discharge_date,
SUM(medication IS NULL) medication,
SUM(test_results IS NULL) test_results
FROM healthcare_dataset;
-- ans : no null value found -- 

-- 5 . check blank value -- 
select * from healthcare_dataset
where trim(patient_name) = ''
or trim(medical_condition) = ''
or trim(doctor_name) = ''
or trim(hospital_name) = ''
or trim(insurance_provider) = ''
or trim(admission_type) = ''
or trim(medication) = ''
or trim(test_results) = '';
-- ans : no blank value found

-- 6 : remove white spaces --
UPDATE healthcare_dataset
SET
patient_name = TRIM(patient_name),
gender = TRIM(gender),
medical_condition = TRIM(medical_condition),
doctor_name = TRIM(doctor_name),
hospital_name = TRIM(hospital_name),
insurance_provider = TRIM(insurance_provider),
admission_type = trim(admission_type) , 
medication = trim(medication) , 
test_results = TRIM(test_results);

-- 7 . check unique values -- 
SELECT 'patient_name' AS column_name, COUNT(DISTINCT patient_name) AS unique_count,
GROUP_CONCAT(DISTINCT patient_name ORDER BY patient_name SEPARATOR ', ') AS unique_values
FROM healthcare_dataset
UNION ALL

SELECT 'gender', COUNT(DISTINCT gender),GROUP_CONCAT(DISTINCT gender ORDER BY gender SEPARATOR ', ')
FROM healthcare_dataset

UNION ALL

SELECT
    'blood_type',
    COUNT(DISTINCT blood_type),
    GROUP_CONCAT(DISTINCT blood_type ORDER BY blood_type SEPARATOR ', ')
FROM healthcare_dataset

UNION ALL

SELECT
    'medical_condition',
    COUNT(DISTINCT medical_condition),
    GROUP_CONCAT(DISTINCT medical_condition ORDER BY medical_condition SEPARATOR ', ')
FROM healthcare_dataset

UNION ALL

SELECT
    'doctor_name',
    COUNT(DISTINCT doctor_name),
    GROUP_CONCAT(DISTINCT doctor_name ORDER BY doctor_name SEPARATOR ', ')
FROM healthcare_dataset

UNION ALL

SELECT
    'hospital_name',
    COUNT(DISTINCT hospital_name),
    GROUP_CONCAT(DISTINCT hospital_name ORDER BY hospital_name SEPARATOR ', ')
FROM healthcare_dataset

UNION ALL

SELECT
    'insurance_provider',
    COUNT(DISTINCT insurance_provider),
    GROUP_CONCAT(DISTINCT insurance_provider ORDER BY insurance_provider SEPARATOR ', ')
FROM healthcare_dataset

union all 
select 'admission_type' , 
count(distinct admission_type),
group_concat(distinct admission_type order by admission_type separator ', ')
from healthcare_dataset

union all
select 'medication' , 
count(distinct medication),
group_concat(distinct medication order by medication separator ', ')
from healthcare_dataset

union all
select 'test_results',
count(distinct test_results) , 
group_concat(distinct test_results order by test_results separator ', ')
from healthcare_dataset;

-- 8 . check duplicate data -- 
with a as (select patient_name,age,gender,blood_type,medical_condition,
date_of_admission,doctor_name,hospital_name,
insurance_provider,
billing_amount,room_number,admission_type,
discharge_date,medication,test_results ,  count(*) as total from healthcare_dataset
group by patient_name,age,gender,blood_type,medical_condition,
date_of_admission,doctor_name,hospital_name,
insurance_provider,
billing_amount,room_number,admission_type,
discharge_date,medication,test_results having count(*) > 1)
select count(*) as duplicate from a;
