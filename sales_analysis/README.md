# 📊 Sales Analysis — End-to-End Data Analytics Project

A complete sales performance analysis pipeline: raw data → Python cleaning → SQL business analysis → interactive Power BI dashboard. Built as a portfolio project covering the full analytics workflow a Data Analyst role expects.

![Dashboard preview](powerBi/screenshots/dashboard_page2.png)

---

## 📌 Overview

This project analyzes a 1,000+ row retail sales dataset (orders, products, customers, shipping, and profit data) to answer real business questions: which regions and product categories drive the most revenue, how profit margins vary by category, which customers are most valuable each year, and how sales/profit trends move year-over-year.

The project deliberately mirrors a real analyst's workflow — the raw data had genuine quality problems (a completely broken `discount` column reporting 0% on every order despite real discounts being applied, missing values, duplicate rows, mismatched year labels) that needed to be diagnosed and fixed before any analysis could be trusted.

## 🧰 Tech Stack

| Layer | Tools |
|---|---|
| Data cleaning & EDA | Python, pandas, NumPy, Matplotlib, Seaborn |
| Database | MySQL, SQLAlchemy |
| Business analysis | SQL (window functions, recursive CTEs) |
| Dashboard | Power BI (DAX measures, dynamic insight text) |
| Environment config | python-dotenv |

## 📁 Project Structure

```
sales_analysis/
├── dataset/
│   ├── Sales_Data.xlsx           # Raw source data (1,014 rows)
│   └── sales_clean_data.xlsx     # Cleaned output (1,006 rows, 0 nulls, 0 duplicates)
├── notebook/
│   ├── data_clean.ipynb          # Null handling, duplicate removal, data quality fixes
│   └── EDA.ipynb                 # Correlation analysis, outlier detection, KPI breakdowns
├── sql/
│   └── sql_analysis.sql          # Business queries: rankings, YoY growth, cumulative sales
├── powerBi/
│   ├── sales analysis.pbix       # Interactive 4-page dashboard
│   ├── sales analysis.pdf        # Static export
│   └── logo.png
└── xls_to_muysql.py              # Loads cleaned data into MySQL
```

## 🧹 Data Cleaning Highlights

Started at 1,014 rows / 24 columns with nulls scattered across 9 columns and 7 duplicate rows. Key fixes:

- **Removed** one fully-blank row (an export artifact) and 7 exact duplicate rows.
- **Filled missing values using relationships already present in the data** rather than blind imputation — e.g. `sales_price` derived from `total_sales_amount ÷ quantity`, `product_category` mapped from `sub_category` (1:1 relationship), and `ship_mode`/`order_date` pulled from other line items of the same `order_id`.
- **Found and fixed a real data integrity bug**: the `discount` column reported 0% for every single row, but back-calculating from `total_sales_amount` showed real discounts of 10–30% had actually been applied on ~40% of orders. Reverse-engineered the true discount rate: `1 − (total_sales_amount / (sales_price × quantity))`.
- **Corrected a `year` vs `order_date` mismatch** on 26 rows by trusting the actual order date.
- Verified `net_profit = total_sales_amount − total_purchasing_amounts` holds across the full dataset.

Result: **1,006 clean rows, 0 nulls, 0 duplicates**, ready for analysis.

## 📈 EDA Highlights

- Correlation heatmap across all numeric fields (price, cost, sales, profit).
- IQR-based outlier detection on price and cost columns.
- KPI breakdowns by year, region, state, product category, sub-category, client segment, and shipping mode.

## 🗃️ SQL Analysis Highlights

11 business queries covering:
- Monthly revenue and profit per year, ranked
- Top-grossing sub-category within each product category
- Top 3 products by purchase cost within each sub-category
- **A recursive CTE calendar** to find every date with zero orders placed
- Highest-spending customer per year (ranked by total spend, not order count)
- Year-over-year sales and profit growth using `LAG()`

## 📊 Power BI Dashboard

4 pages: landing/navigation, sales & profit analysis, order analysis, and an advanced YoY growth page with a dynamic DAX-generated insights summary.

| Sales & Profit Analysis | Order Analysis |
|---|---|
| ![Sales and profit](powerBi/screenshots/dashboard_page2.png) | ![Order analysis](powerBi/screenshots/dashboard_page3.png) |

| Advanced Analysis (YoY growth + dynamic insights) |
|---|
| ![Advanced analysis](powerBi/screenshots/dashboard_page4.png) |

**Key measures:** `total_sales`, `total_net_profit`, `profit_margin`, YoY growth %, and a dynamic "Dashboard Insights" text measure that auto-generates a plain-English summary (top state/region/category by sales, profit, and orders) as filters change.

## 🔑 Key Insights

- Overall profit margin: **35.2%**, with Technology leading (36.9%) ahead of Office Supplies (35.6%) and Furniture (32.6%).
- **California** is the top state by both sales (₹170K) and profit.
- Every order in the dataset is profitable — there are no loss-making orders — so margin *distribution* (not profit/loss split) is the meaningful lens for this data.
- Order count and order *value* tell different stories: ranking customers by number of orders produces frequent ties, but total spend gives a clean, distinct ranking every year.
- 488 unique orders span 1,006 line items — most orders contain multiple products.

## ▶️ How to Access & Run

**1. Clone the repo**
```bash
git clone <your-repo-url>
cd sales_analysis
```

**2. Set up the Python environment**
```bash
pip install pandas numpy matplotlib seaborn sqlalchemy mysql-connector-python python-dotenv openpyxl xlrd jupyter
```

**3. Run the data cleaning notebook**
Open `notebook/data_clean.ipynb` in Jupyter and run all cells top to bottom. Update the file paths at the top to match your local `dataset/` folder. This produces `sales_clean_data.xlsx`.

**4. Run the EDA notebook**
Open `notebook/EDA.ipynb` and run all cells to reproduce the charts and KPI breakdowns.

**5. Load into MySQL**
Create a `.env` file next to `xls_to_muysql.py`:
```
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=sales
XLS_FILE=dataset/sales_clean_data.xlsx
```
Then run:
```bash
python xls_to_muysql.py
```

**6. Run the SQL analysis**
Open `sql/sql_analysis.sql` in MySQL Workbench (or your client of choice) against the `sales` database and run the queries.

**7. Open the dashboard**
Open `powerBi/sales analysis.pbix` in Power BI Desktop. Point the data source to your MySQL `sales_clean_data` table (or the cleaned Excel file) and refresh.

## 👤 Author

**Isha** — Aspiring Data Analyst
GitHub: [Ishasavani1402](https://github.com/Ishasavani1402)
