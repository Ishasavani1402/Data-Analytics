<div align="center">

# 🛍️ Myntra Sales Analysis

### End-to-End Data Analytics Project | Python • MySQL • SQL • Power BI

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-Database-4479A1?style=flat&logo=mysql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Cleaning-150458?style=flat&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-D71F00?style=flat)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat&logo=powerbi&logoColor=black)
![Status](https://img.shields.io/badge/Status-Complete-success)

</div>

---

## 📖 1. Project Overview

This project analyzes **8,000 Myntra e-commerce orders (2023–2025)** to answer real business questions — which categories drive revenue, which customers are worth retaining, whether discounts actually move units, and where delivery/returns are hurting margins.

It covers the **full analytics pipeline**:

`CSV → Python ETL → MySQL → Data Cleaning → SQL-driven EDA → Power BI Dashboard`

Rather than generic descriptive stats, the analysis is framed as **22 business questions** an e-commerce analyst would actually be asked — revenue drivers, discount effectiveness, customer retention, delivery bottlenecks, and return/cancellation leakage.

---

## 🎯 2. Business Objective

| Goal | Why it matters |
|---|---|
| Identify top revenue categories, brands & channels | Focus marketing & inventory spend |
| Test if discounts actually drive sales | Stop wasting margin on ineffective discounting |
| Measure repeat vs one-time customer value | Justify retention spend over acquisition |
| Spot delivery & return bottlenecks | Reduce revenue leakage from cancellations/returns |
| Segment customers by age, gender, membership | Sharper targeting for campaigns |

---

## 🧰 3. Tech Stack

| Layer | Tools Used |
|---|---|
| **Data Storage** | MySQL |
| **ETL / Data Loading** | Python, `mysql-connector`, dynamic schema mapping |
| **Data Cleaning** | Python, Pandas, SQLAlchemy |
| **Analysis** | SQL (CTEs, window functions, `NTILE`, aggregations) |
| **Visualization (Python)** | Matplotlib, Seaborn |
| **Dashboard** | Power BI |
| **Environment Config** | `python-dotenv` |

---

## 📂 4. Project Structure

```
Mytra sales analysis/
│
├── csv_mysql.py                        # Python ETL: CSV → MySQL (auto schema + chunked insert)
├── requirements.txt                    # Project dependencies
│
├── dataset/
│   └── Myntra_Sales_Dataset.csv        # Raw dataset (8,000 rows × 29 columns)
│
├── notebooks/
│   ├── data_clean.ipynb                # Data cleaning & preprocessing
│   └── EDA.ipynb                       # 22 SQL business questions + visual EDA
│
└── power Bi/
    ├── mytra sales analysis dashboard.pbix
    ├── revenue analysis.png
    ├── order analysis.png
    ├── customer and discount analysis.png
    └── mytra logo.png
```

---

## 🗃️ 5. Dataset

- **Source file:** `Myntra_Sales_Dataset.csv`
- **Size:** 8,000 orders | 29 columns | 2023 – 2025
- **Grain:** One row = one order line

**Key fields:** `Order_ID`, `Order_Date`, `Customer_ID`, `Age`, `Gender`, `City`, `State`, `Membership_Tier`, `Product_Name`, `Category`, `Sub_Category`, `Brand`, `MRP`, `Discount_Percent`, `Selling_Price`, `Quantity`, `Total_Amount`, `Payment_Mode`, `Order_Channel`, `Order_Status`, `Delivery_Days`, `Rating`

---

## 🔄 6. Project Workflow

```
1️⃣  Raw CSV loaded into MySQL          → csv_mysql.py (dynamic type mapping + chunked insert)
2️⃣  Data cleaning in MySQL              → data_clean.ipynb
3️⃣  Clean table pushed back to MySQL    → clean_dataset table + clean_dataset.csv
4️⃣  Business questions answered in SQL  → EDA.ipynb (22 queries + charts)
5️⃣  Insights visualized                 → Power BI (3-page interactive dashboard)
```

### 🧹 Data Cleaning Steps (`data_clean.ipynb`)
- Loaded raw table from MySQL via SQLAlchemy
- Standardized column names (lowercase, stripped, underscored)
- Dropped `customer_name` (PII, not needed for analysis)
- Checked & handled nulls (`rating` → filled with median)
- Checked for duplicates
- Stripped leading/trailing whitespace across all text columns
- Verified unique category values for consistency
- Wrote the cleaned table back to MySQL (`clean_dataset`) and exported to CSV

### 🔍 SQL Business Questions (`EDA.ipynb`)
22 SQL-driven questions using joins, CTEs, window functions, and `NTILE` banding, including:
- Revenue & AOV by category, brand, membership tier, payment mode, channel, state/city
- Discount-band effectiveness (does more discount = more quantity sold?)
- Underperforming products despite heavy discounting (dead stock detection)
- High-volume products with rating < 3 (quality risk on popular SKUs)
- Delivery time by category/sub-category
- Return & cancellation rate by category and brand
- Customer segmentation by age group and gender
- Seasonal monthly revenue trend (2023–2025)
- Repeat vs one-time customer revenue contribution
- Top 3 revenue products per category
- Cumulative revenue trend (window functions)
- Net revenue after returns/cancellations vs gross revenue
- Delivery speed vs rating correlation
- Discount-to-revenue lift by order channel

---

## 💡 7. Key Insights

| Insight | Finding |
|---|---|
| 👗 **Top category** | **Women** leads revenue (₹70.9L), followed by Men (₹66.9L) and Kids (₹33.5L) |
| 🏷️ **Top brand** | **H&M** drives the largest share of revenue (8.1%), ahead of Roadster (7.8%) and Puma (5.6%) |
| 📱 **Best channel** | **Mobile App** generates ~66% of total revenue (₹1.33Cr) — far ahead of Website and Mobile Web |
| 💸 **Discounts aren't working as intended** | Average quantity per order stays flat (~1.24–1.25) across ALL discount bands — heavier discounting isn't pulling more units |
| 🔁 **Retention is the real revenue engine** | Repeat customers are just **84.3%** of the customer base but drive **95.3%** of total revenue |
| 🚚 **Delivery bottleneck** | Home & Living → Furnishing has the slowest average delivery (5.84 days) |
| ↩️ **Returns eat into revenue** | Women's and Men's categories lose the most absolute revenue to returns/cancellations |
| ⭐ **Membership tier ≠ biggest spender base** | "Insider" (not "Insider Gold") contributes the highest total revenue — Gold tier has far fewer members |
| 🏙️ **Order volume leaders** | Patna, Chandigarh, and Kochi top the list for total number of orders |

---

## 📊 8. Power BI Dashboard

A 3-page interactive dashboard built on the cleaned dataset, with slicers for Brand, Category, Order Status, Gender, Payment Mode, Membership Tier, Age Group, Order Channel, State, City, and Product.

**Page 1 — Revenue Analysis**
![Revenue Analysis](power%20Bi/revenue%20analysis.png)

**Page 2 — Customer & Discount Analysis**
![Customer and Discount Analysis](power%20Bi/customer%20and%20discount%20analysis.png)

**Page 3 — Order Analysis**
![Order Analysis](power%20Bi/order%20analysis.png)

**Headline KPIs:** ₹21.26M total revenue • 8K orders • 2,481 customers • ₹2,658 AOV • 2,092 repeat customers

---

## ⚙️ 9. How to Run This Project

### Prerequisites
- Python 3.10+
- A running MySQL server

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/Ishasavani1402/myntra-sales-analysis.git
cd myntra-sales-analysis
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Set up environment variables**
Create a `.env` file in the project root:
```env
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=myntra_sales
CSV_FILE=dataset/Myntra_Sales_Dataset.csv
```
> ⚠️ Never commit `.env` — add it to `.gitignore`.

**4. Load the raw CSV into MySQL**
```bash
python csv_mysql.py
```

**5. Run the notebooks in order**
```
notebooks/data_clean.ipynb   →  cleans data, creates `clean_dataset` table
notebooks/EDA.ipynb          →  runs the 22 SQL business questions + charts
```

**6. Explore the dashboard**
Open `power Bi/mytra sales analysis dashboard.pbix` in Power BI Desktop (point it at the `clean_dataset` table or `clean_dataset.csv`).

---

## 🚀 10. Future Scope

- Add a churn-prediction model using repeat-purchase behavior
- Automate the ETL → clean → dashboard-refresh pipeline with a scheduler
- Build a customer lifetime value (CLV) model by membership tier
- Add cohort analysis for retention by signup month

---

## 👩‍💻 11. Author

**Isha Savani**
Aspiring Data Analyst | Python • SQL • Power BI
🔗 [GitHub — Ishasavani1402](https://github.com/Ishasavani1402)

---

<div align="center">

⭐ If you found this project useful, consider giving it a star!

</div>
