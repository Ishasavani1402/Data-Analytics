# Employee Attrition Analysis

An end-to-end data analytics project that explores **why employees leave a company** — analyzing demographic, compensation, and workplace factors to identify which employee segments carry the highest attrition risk.

---

## Table of Contents

- [Objective](#objective)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Methodology](#methodology)
- [Key Insights](#key-insights)
- [How to Access This Project](#how-to-access-this-project)
- [Future Work](#future-work)
- [Author](#author)

---

## Objective

The objective is to identify patterns and key factors associated with employee attrition and understand which employee segments are more likely to leave. The analysis examines the impact of salary, overtime, work-life balance, job role, department, job level, and demographic characteristics on employee churn.

---

## Dataset

- **Source:** Kaggle (HR employee dataset, 2026)
- **Raw file:** `employee_attrition_hr_2026.csv` — 5,000 records, 25 columns
- **Cleaned file:** `clean_dataset.csv` — 4,902 records, 21 columns (post data-cleaning)

**Key fields include:**

| Category | Columns |
|---|---|
| Demographics | age, gender, marital_status, education_level, age_group |
| Job info | department, job_role, job_level, work_mode |
| Compensation | monthly_income, salary_hike_pct |
| Tenure | years_at_company, total_working_years |
| Wellbeing | burnout_score, engagement_score, work_life_balance_score, overtime_hours_per_week |
| Tech adoption | uses_ai_tools_at_work, perceived_ai_job_risk |
| Target | attrition (1 = left, 0 = retained) |

---

## Project Structure

```
employee_attrition/
│
├── dataset/
│   ├── employee_attrition_hr_2026.csv   # Raw dataset
│   └── clean_dataset.csv                # Cleaned dataset (output of data_clean.ipynb)
│
├── notebooks/
│   ├── data_clean.ipynb                 # Data cleaning & preprocessing
│   └── EDA.ipynb                        # Exploratory data analysis & visualizations
│
├── requirements.txt                     # Python dependencies
└── README.md
```

---

## Tech Stack

- **Language:** Python
- **Libraries:** pandas, numpy, matplotlib, seaborn
- **Environment:** Jupyter Notebook

---

## Methodology

**1. Data Cleaning (`data_clean.ipynb`)**
- Checked for nulls, duplicates, and data shape
- Stripped whitespace from text columns and standardized categorical values
- Encoded `attrition` (Yes/No → 1/0)
- Binned `age` into age groups and mapped numeric `job_level` codes to readable labels
- Exported the cleaned dataset for analysis

**2. Exploratory Data Analysis (`EDA.ipynb`)**
- Summary statistics, correlation heatmap, distribution and outlier checks
- Company-wide KPIs: headcount, average income, attrition rate, retention rate
- Attrition breakdown by gender, department, marital status, job level, and work mode
- Burnout and overtime patterns across departments
- Engagement score trends across age groups and job levels
- Compensation patterns (salary hike %) across job roles and job levels
- Impact of AI tool adoption on salary hikes and attrition

---

## Key Insights

- **Overall attrition rate:** 17.91% (878 of 4,902 employees), against a retention rate of 82.09%
- **Department:** Customer Support has the highest attrition (23.9%), followed by Sales (20%); Engineering and R&D are the most stable (~14.5%)
- **Job level:** Entry-level employees show the highest attrition risk, while Management has the lowest — and the highest average income
- **Work mode:** Onsite employees churn the most (21%), followed by Hybrid (17%); Remote employees have the lowest attrition (15%)
- **Gender & marital status:** Attrition rates are nearly identical across both — neither is a meaningful driver in this dataset
- **Burnout & overtime:** Sales and Customer Support show the highest burnout and overtime hours, aligning with their higher attrition
- **AI tool usage:** Employees using AI tools at work earn a marginally higher salary hike, but attrition rate is unaffected (18% either way)

---

## How to Access This Project

**1. Clone the repository**
```bash
git clone https://github.com/Ishasavani1402/employee-attrition-analysis.git
cd employee-attrition-analysis
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Run the notebooks in order**
```bash
jupyter notebook notebooks/data_clean.ipynb   # Step 1: Clean the raw data
jupyter notebook notebooks/EDA.ipynb          # Step 2: Explore & visualize
```

> Note: Update the file paths in the notebooks (`pd.read_csv(...)`) to match your local `dataset/` folder path before running.

---

## Author

**Isha Savani**
Aspiring Data Analyst | Python · SQL · Power BI
GitHub: [Ishasavani1402](https://github.com/Ishasavani1402)
