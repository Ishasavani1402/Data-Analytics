# 📊 Tech Layoffs & Hiring Trend Analysis

End-to-end analytics project on global tech industry workforce trends — covering layoffs, hiring activity, AI adoption, and employee sentiment. Built with **Python → MySQL → Power BI**, taking raw data through cleaning, EDA, SQL analysis, and an interactive dashboard.

![Python](https://img.shields.io/badge/Python-3.x-blue)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## 🎯 Objective

Identify layoff patterns and hiring trends across the global tech industry, and understand how they relate to AI adoption, market conditions, and employee sentiment — using a full analytics pipeline from raw data to a decision-ready dashboard.

---

## 🗂️ Dataset

| | |
|---|---|
| **Records** | 12,000 rows |
| **Companies** | 20 (Microsoft, Google, Meta, Amazon, Apple, Databricks, Anthropic, Stripe, and others) |
| **Industries** | 7 — AI, Cloud, Cybersecurity, E-Commerce, FinTech, Gaming, Social Media |
| **Countries** | 6 — USA, UK, India, Canada, Germany, Singapore |
| **Time range** | 2024 – 2026 |
| **Key fields** | layoffs_count, layoff_percentage, hiring_trend, open_roles, ai_adoption_level, ai_replacement_risk, employee_sentiment, job_security_score, market_condition |

---

## 🛠️ Tech Stack

- **Python** — pandas, numpy, matplotlib, seaborn (cleaning + EDA)
- **MySQL 8.0** — via `mysql-connector-python` + `python-dotenv` (storage + SQL analysis)
- **SQL** — window functions (`DENSE_RANK`, partitioned aggregates) for ranking and share-of-total analysis
- **Power BI** — 2-page interactive dashboard with drill-through navigation and slicers

---

## 📁 Project Structure

```
tech_layoffs_hiring_trends/
│
├── datasets/
│   ├── tech_layoffs_hiring_trends.csv     # raw data
│   └── clean_dataset.csv                  # cleaned data (output of data_clean.ipynb)
│
├── notebooks/
│   ├── data_clean.ipynb                   # null/duplicate/whitespace/negative/range checks
│   └── EDA.ipynb                          # correlation, outliers, KPIs, trend analysis
│
├── sql/
│   └── sql_analysis.sql                   # ranking & distribution queries (window functions)
│
├── power Bi/
│   ├── tech_layoff_hiring_trend.pbix
│   ├── tech_layoff_hiring_trend_1.png
│   └── tech_layoff_hiring_trend_2.png
│
├── csv_to_mysql.py                        # ETL script: CSV → MySQL (schema-aware, batch insert)
├── requirements.txt
└── README.md
```

---

## 🔄 Workflow

**1. Data Cleaning** (`data_clean.ipynb`)
- Standardized column names, stripped whitespace from text fields
- Verified `record_id` as a unique primary key (12,000/12,000)
- Checked nulls, duplicates, and out-of-range values (percentages bound to 0–100, scores bound to 1–10)
- Validated categorical fields for spelling/casing inconsistencies

**2. Exploratory Data Analysis** (`EDA.ipynb`)
- Correlation heatmap across all numeric features
- IQR-based outlier detection with boxplot visualization
- KPI summary, yearly trend, and breakdowns by country / industry / company / company size / market condition
- AI adoption vs. replacement risk, and sentiment/job-security trend over time

**3. SQL Analysis** (`sql_analysis.sql`)
- Loaded into MySQL via `csv_to_mysql.py` (dynamic schema creation, `DECIMAL` typing for percentage/score fields, chunked inserts, null-safe handling)
- Window-function queries: top layoff company per year, most common layoff reason per industry, country with highest layoffs per year, top hiring role per company, and each company's share of moderate/aggressive hiring activity per year

**4. Power BI Dashboard**
- 2-page interactive report with drill-through (`next` / `back`) navigation
- Slicers: hiring role, year, company name, company size, industry
- KPI cards, geographic and categorical breakdowns, and a dedicated insights panel

---

## 📌 Key Insights

- **60.1M** total layoffs and **34.6M** open roles recorded across 20 companies (2024–2026), at an average layoff rate of **12.78%**
- **Social Media** is the leading industry by total layoffs; **UK** leads by country
- **2024** recorded the highest yearly layoffs; **2026** saw the highest number of open roles — signaling a hiring recovery
- **ML Engineer** is the most in-demand hiring role (8M+ open roles), and **AI Automation** is the most cited layoff reason
- AI adoption level shows a strong positive correlation with AI replacement risk, while layoff percentage is strongly negatively correlated with job security score

---

## ⚠️ Known Limitations

- `hiring_trend` and `company_size` labels don't always causally align with `open_roles` / `layoffs_count` (e.g., some "Hiring Freeze" records still show high open role counts) — these fields should be read as independent simulated signals, not strictly causal ones.
- `stock_growth_percent`, `revenue_growth_percent`, and `salary_budget_change` were excluded from the cleaned dataset and are not reflected in the EDA, SQL, or dashboard.

---

## ▶️ How to Reproduce

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run data cleaning
jupyter notebook notebooks/data_clean.ipynb

# 3. Run EDA
jupyter notebook notebooks/EDA.ipynb

# 4. Load cleaned data into MySQL (set DB credentials in a .env file)
python csv_to_mysql.py datasets/clean_dataset.csv

# 5. Run sql/sql_analysis.sql in your MySQL client

# 6. Open power Bi/tech_layoff_hiring_trend.pbix in Power BI Desktop
```

---

## 📷 Dashboard Preview

**Page 1 — Overview**
![Dashboard Part 1](power%20Bi/tech_layoff_hiring_trend_1.png)

**Page 2 — Deep Dive**
![Dashboard Part 2](power%20Bi/tech_layoff_hiring_trend_2.png)

---

## 👤 Author

**Isha Savani**
GitHub: [@Ishasavani1402](https://github.com/Ishasavani1402)
