# 🛒 Walmart Sales Analysis

End-to-end sales analysis project built on Walmart transaction data — from raw CSV load in MySQL through data cleaning, SQL-based business analysis, and an interactive Power BI dashboard.

![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## 📌 Project Overview

This project analyzes 1,000 Walmart transaction records to answer core retail business questions: which cities, branches, and product lines drive the most revenue, when customers shop, how profitable each segment is, and how customer type affects spending. The pipeline covers the full analytics workflow — raw data ingestion, cleaning and validation, SQL analysis, and dashboard visualization.

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Data Source | CSV (raw transaction export) |
| Database | MySQL 8.0 |
| Analysis | SQL (window functions, CTEs, aggregates) |
| Visualization | Power BI |

## 📂 Project Structure

```
walmart_sql/
├── dataset/
│   ├── Walmart_Sales.csv        # raw source data
│   └── clean_sales.csv          # cleaned, analysis-ready data
├── sql analysis/
│   ├── prepare_dataset.sql      # table creation + LOAD DATA import
│   ├── data_clean.sql           # validation, cleaning, feature engineering
│   └── sql_analysis.sql         # KPIs + business analysis queries
└── powerBi/
    └── walmart sales analysis.pbix   # interactive dashboard
```

## 🔄 Workflow

1. **Load** — Created the `sales` table in MySQL 8.0 and imported the raw CSV via `LOAD DATA LOCAL INFILE`, converting the source date format with `STR_TO_DATE`.
2. **Clean** — Validated and prepared the data, then saved the result as a separate `clean_sales` table (raw table preserved as an audit trail).
3. **Analyze** — Ran SQL queries answering specific business questions (KPIs, rankings, trends, profit).
4. **Visualize** — Built a Power BI dashboard on top of `clean_sales` with interactive slicers.

## 🧹 Data Cleaning Steps

- Checked for duplicate `invoice_id` values (primary key)
- Checked for NULLs and blank strings across all columns
- Trimmed leading/trailing whitespace on text fields
- Validated for zero/negative values in numeric fields (`quantity`, `total`, `unit_price`, `rating`)
- Verified calculated-field consistency (`total ≈ unit_price × quantity + vat`)
- Verified categorical fields against expected distinct values (branch, city, payment, product_line, etc.)
- Engineered new columns for analysis: `time_of_day` (Morning/Afternoon/Evening), `day_name`, `month_no`

## 📊 Business Questions Answered

- Total sales, quantity sold, orders, average rating, and profit/margin (headline KPIs)
- Revenue by product category, product line, individual product, and city
- Gender split by customer type
- Best-selling products by revenue vs. by quantity
- Sales and transaction volume by payment method
- Monthly sales trend and month-over-month growth %
- Sales by time of day and by day of week
- Top-rated products (with a minimum transaction threshold to avoid small-sample bias)
- Best-selling product within each product line
- Top-performing branch within each city
- VAT/GST contribution by product line
- Average basket size (spend per transaction) by customer type

## 📈 Key Insights

- **₹322.97K** in total sales across **1,000 orders** (~6,000 units sold)
- Overall profit of **₹15.38K** at a **4.76%** margin
- **Mumbai** is the top-performing city; **Electronic accessories** is the top-performing product line
- **Saturday** is the strongest sales day of the week, **Monday** the weakest
- **Coca-Cola** is the single best-selling product by revenue
- Sales are fairly evenly spread across product categories — no single category dominates the mix

## 🖥️ Dashboard

The Power BI dashboard (`powerBi/walmart sales analysis.pbix`) includes:
- KPI cards: total orders, total quantity, total sales, total profit, profit margin
- Sales breakdown by product line, product, city, month, and weekday
- Sales by customer type and gender
- Slicers for city, month, product line, product name, customer type, and weekday

## 🚀 Reproducing This Project

1. Run `sql analysis/prepare_dataset.sql` to create the database, table, and load the raw CSV.
2. Run `sql analysis/data_clean.sql` to validate, clean, and generate `clean_sales`.
3. Run `sql analysis/sql_analysis.sql` for the full set of business-analysis queries.
4. Open `powerBi/walmart sales analysis.pbix` in Power BI Desktop and refresh the data source to point at your local `clean_sales` table.

## 👤 Author

**Isha Savani**
Aspiring Data Analyst | SQL · Power BI · Python
GitHub: [@Ishasavani1402](https://github.com/Ishasavani1402)
