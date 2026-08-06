# 🛒 E-Commerce Customer Churn Analysis

An end-to-end data analytics project that identifies **why customers churn** on an e-commerce platform and **which segments are highest-risk**, using Python for cleaning/EDA, MySQL for business-question SQL analysis, and Power BI for an executive dashboard.

---

## 📌 Table of Contents

- [Business Problem](#-business-problem)
- [Objective](#-objective)
- [Dataset](#-dataset)
- [Tech Stack](#-tech-stack)
- [Project Workflow](#-project-workflow)
- [Folder Structure](#-folder-structure)
- [Data Cleaning](#-data-cleaning)
- [SQL Business Analysis](#-sql-business-analysis)
- [Key Insights](#-key-insights)
- [Power BI Dashboard](#-power-bi-dashboard)
- [How to Run This Project](#-how-to-run-this-project)
- [Future Improvements](#-future-improvements)
- [Author](#-author)

---

## 🎯 Business Problem

Customer churn directly erodes revenue and increases acquisition costs. This project analyzes an e-commerce platform's customer base to answer the questions a Product, Marketing, or Customer Success leader would actually ask:

- Why are customers churning?
- Which customer segments carry the highest churn risk?
- Which behavioral and demographic factors influence retention most?
- Where should retention budget be spent first?

## 🎯 Objective

Build a complete, portfolio-grade analytics pipeline — from raw CSV to a business-ready dashboard — covering data cleaning, exploratory analysis, SQL-driven business analytics, and interactive visualization, following the same workflow used by data analysts at product-based companies.

## 🗂 Dataset

- **Source:** [Kaggle – E-Commerce Customer Churn Dataset](https://www.kaggle.com/)
- **Size:** 5,630 customers × 20 columns
- **Target variable:** `Churn` (1 = churned, 0 = retained)

| Column | Description |
|---|---|
| CustomerID | Unique customer identifier |
| Churn | Whether the customer churned |
| Tenure | Months the customer has stayed with the company |
| PreferredLoginDevice | Device used to log in |
| CityTier | Tier of the customer's city |
| WarehouseToHome | Distance from warehouse to customer |
| PreferredPaymentMode | Preferred payment method |
| Gender | Customer gender |
| HourSpendOnApp | Avg. hours spent on the app |
| NumberOfDeviceRegistered | Devices registered by the customer |
| PreferedOrderCat | Preferred order category (last month) |
| SatisfactionScore | Customer satisfaction score |
| MaritalStatus | Marital status |
| NumberOfAddress | Number of registered addresses |
| Complain | Whether a complaint was raised (last month) |
| OrderAmountHikeFromlastYear | % increase in order amount vs. last year |
| CouponUsed | Coupons used (last month) |
| OrderCount | Orders placed (last month) |
| DaySinceLastOrder | Days since last order |
| CashbackAmount | Average cashback received |

## 🛠 Tech Stack

| Layer | Tools |
|---|---|
| Data Storage / Loading | MySQL, SQLAlchemy, `mysql-connector-python` |
| Data Cleaning & EDA | Python, Pandas, NumPy |
| Visualization (Python) | Matplotlib, Seaborn |
| Business Analysis | MySQL (window functions, CTEs) |
| Dashboarding | Power BI |
| Environment | Jupyter Notebook, `python-dotenv` |

## 🔄 Project Workflow

```
Raw CSV
   │
   ▼
[1] Load CSV → MySQL          (csv_to_mysql.py)
   │
   ▼
[2] Data Cleaning              (notebooks/data_clean.ipynb)
   │   • standardize column names
   │   • handle nulls, whitespace, duplicate categories
   │   • push clean table back to MySQL (clean_dataset)
   ▼
[3] EDA + SQL Business Analysis  (notebooks/EDA.ipynb)
   │   • univariate distributions, correlation heatmap, outlier check
   │   • 23 business questions solved directly in MySQL
   ▼
[4] Power BI Dashboard          (power Bi/*.pbix)
   │   • KPIs, segment breakdowns, filters
   ▼
Business Insights & Recommendations
```

## 📁 Folder Structure

```
E-com customer churn analysis/
│
├── dataset/
│   └── ecommerce_churn.csv              # Raw source data
│
├── notebooks/
│   ├── data_clean.ipynb                 # Cleaning & preprocessing
│   └── EDA.ipynb                        # EDA + 23 SQL business questions
│
├── power Bi/
│   ├── ecommerce_customer_churn_analysis.pbix
│   └── ecommerce_customer_churn_analysis_dashboard.png
│
├── csv_to_mysql.py                      # Loads CSV → MySQL with schema mapping
├── requirements.txt                     # Python dependencies
└── README.md
```

## 🧹 Data Cleaning

Performed in `notebooks/data_clean.ipynb`:

- Loaded raw data from MySQL, standardized column names to `snake_case`
- Null checks and duplicate-row checks
- Missing numeric values (Tenure, WarehouseToHome, HourSpendOnApp, CouponUsed, OrderCount, DaySinceLastOrder) imputed with the column median
- Removed leading/trailing whitespace from all text columns
- Consolidated inconsistent categories (`CC` → `Credit Card`, `COD` → `Cash on Delivery`)
- Verified the cleaned table (`clean_dataset`) matches the source row count before use in analysis

## 🧠 SQL Business Analysis

23 business questions were designed and solved directly in MySQL, progressing from descriptive to advanced analytical thinking:

| Level | Count | Examples |
|---|---|---|
| Basic | 6 | Overall churn rate · Churn by city tier · Churn by payment mode |
| Intermediate | 9 | Complaint impact on churn · Coupon usage vs. churn · Engagement (app hours) vs. churn |
| Advanced | 8 | RFM-style segmentation · Churn-weighted category risk score · High-value at-risk customers · VIP-at-risk ranking (window functions) |

Techniques used: CTEs, `NTILE()`, `ROW_NUMBER()`, `DENSE_RANK()`, conditional aggregation, and cohort bucketing.
Full query set with business justification for each: [`notebooks/EDA.ipynb`](notebooks/EDA.ipynb).

## 💡 Key Insights

- **Overall churn rate: 16.84%** (948 churned / 5,630 customers) — retain rate 83.16%
- **Complaints are the single strongest churn driver found:** customers who complained churned at **31.67%** vs. **10.93%** for those who didn't — nearly 3× higher
- **Single customers churn far more than married customers:** 26.73% vs. 11.52%
- **Mobile Phone category has by far the highest churn rate** among order categories (27.40%), vs. as low as 4.88% for Grocery
- **Cash on Delivery users churn the most** among payment modes (24.90%), Credit Card users the least (14.21%)
- **Churn risk drops sharply with city tier:** Tier 1 cities churn at 14.51% vs. 21.37% in Tier 3
- **Churned customers have much shorter tenure** on average (3.86 months) than retained customers (11.40 months) — churn is concentrated early in the customer lifecycle
- **Counterintuitive finding:** average satisfaction score is *higher* for churned customers (3.39) than retained ones (3.00) — satisfaction alone is not a reliable retention signal here and warrants deeper investigation

## 📊 Power BI Dashboard

![Dashboard Preview](power%20Bi/ecommerce_customer_churn_analysis_dashboard.png)

**Includes:**
- KPI cards — total customers, churned, retained, churn rate, retention rate
- Churn rate breakdowns by gender, marital status, city tier, complaint status
- Coupon usage and cashback comparison between churned and retained customers
- Interactive slicers — gender, order category, marital status, churn status

## ⚙️ How to Run This Project

**1. Clone the repository**
```bash
git clone https://github.com/Ishasavani1402/Data-Analytics/tree/main/E-com%20customer%20churn%20analysis
cd "E-com customer churn analysis"
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Set up MySQL connection**
Create a `.env` file in the project root (this is not committed file):
```
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=your_database_name
CSV_FILE=your_csvfile_path
```

**4. Load the data into MySQL**
```bash
python csv_to_mysql.py
```

**5. Run the notebooks in order**
```
notebooks/data_clean.ipynb   → produces the clean_dataset table
notebooks/EDA.ipynb          → EDA + all 23 SQL business questions
```

**6. Open the dashboard**
Open `power Bi/ecommerce_customer_churn_analysis.pbix` in Power BI Desktop and point the data source to your local `clean_dataset` table.


## 👤 Author

**Isha Savani**
Aspiring Data Analyst | Python · SQL · Power BI
GitHub: [@Ishasavani1402](https://github.com/Ishasavani1402)

---

⭐ If you found this project useful, consider giving it a star on GitHub!
