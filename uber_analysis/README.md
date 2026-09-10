# 🚕 Uber Trip Analysis

End-to-end data analytics project on 100K+ Uber trip records — from raw Excel files to a live Power BI dashboard, covering ETL, MySQL data modeling, data cleaning, SQL business analysis, and dashboard storytelling.

---

## 📌 Project Overview

This project analyzes Uber trip data to answer core business questions: which vehicle types and payment methods drive the most revenue, when demand peaks, which pickup zones matter most, and how trip distance relates to trip duration.

The pipeline goes: **raw Excel → MySQL (via Python ETL) → SQL cleaning & analysis → Power BI dashboard.**

**Key questions answered:**
- Which vehicle type and payment method generate the most revenue — and which is costliest per ride?
- When does trip demand peak (hour, day, weekday vs weekend)?
- Which cities and pickup zones drive the most trips and revenue?
- How does trip distance relate to trip duration?
- What share of revenue comes from surge pricing?

---

## 📊 Dataset

| File | Rows | Description |
|---|---|---|
| `Uber Trip Details.xlsx` | 103,728 | Trip-level data: pickup/dropoff time, distance, fare, surge fee, vehicle, payment type |
| `Location Table.xlsx` | 265 | Pickup/dropoff zone reference: location ID, zone name, city |

The two tables are linked via `PULocationID` / `DOLocationID` → `LocationID` (foreign key relationship, enforced in MySQL).

---

## 🛠️ Tech Stack

- **Python** — pandas, SQLAlchemy, python-dotenv (ETL: Excel → MySQL)
- **MySQL 8.0** — schema design, data cleaning, window functions, business analysis queries
- **Power BI** — data modeling, DAX measures, interactive dashboard

---

## 📂 Repository Structure

```
uber_analysis/
├── datasets/
│   ├── Location Table.xlsx
│   └── Uber Trip Details.xlsx
├── xls_to_mysql.py              # ETL script: loads Excel files into MySQL
├── analysis/
│   ├── datasetup.sql            # Database & table creation (schema + FK relationships)
│   └── sql_analysis.sql         # Data quality checks + full SQL business analysis
├── data_clean/
│   └── data_clean.ipynb         # Python data cleaning & feature engineering
└── powerBi/
    ├── uber analysis.pbix       # Power BI dashboard file
    └── uber analysis dashboard.png
```

---

## 🔄 Project Workflow

**1. Database setup** (`analysis/datasetup.sql`)
Created a MySQL database `uber` with two tables — `location` and `trip_details` — linked by foreign keys on pickup/dropoff location IDs.

**2. ETL — Excel to MySQL** (`xls_to_mysql.py`)
A reusable Python script reads both `.xlsx` files, standardizes column names, and loads them into MySQL via SQLAlchemy. Credentials and file paths are kept out of the code using a `.env` file.

**3. Data cleaning** (`data_clean/data_clean.ipynb`)
- Investigated nulls in `location`/`city` — confirmed these are known NYC TLC placeholder zones (location IDs 264 "NV" and 265 blank), not data errors, and labeled them accordingly rather than dropping trips.
- Checked and removed invalid records (e.g. dropoff time earlier than pickup time).
- Checked for negative values across numeric columns.
- Engineered new columns: `total_booking_amount`, `pickup_date`, `pickup_hour`, `trip_duration_min`, `trip_distance_bucket`, `pickup_day_name`, `day_type` (weekday/weekend).
- Saved the cleaned output back to MySQL as `clean_location` and `clean_trip_detail`, with row-count verification against the source dataframes.

**4. SQL analysis** (`analysis/sql_analysis.sql`)
14 queries covering data quality checks, core KPIs, vehicle/payment/time-based breakdowns, city and pickup-zone analysis (with joins), weekday vs weekend comparison, surge fee contribution, and a window-function query ranking the top pickup location within each city.

**5. Power BI dashboard** (`powerBi/uber analysis.pbix`)
Interactive dashboard built on `clean_trip_detail` and `clean_location`, with slicers for city, vehicle, day type, payment type, pickup day, and date range. Includes a DAX-generated natural-language insight card that auto-summarizes the current filter view.

---

## 💡 Key Insights

- **103,726 trips** analyzed, generating **₹1,553,650** in total booking revenue.
- Average trip: **3.36 miles**, **15.9 minutes**, **₹13** fare.
- **UberX** leads by revenue (₹583.87K); **Uber Pay** is the most-used payment method (₹1,099.02K in revenue).
- Trips over 50 miles ("Extreme Long") average **246 minutes** — by far the longest duration bucket.
- Demand peaked on **26 June 2024** (4,947 trips); **Monday** is the busiest weekday overall, and weekdays see higher average daily volume than weekends.
- **Manhattan's Penn Station/Madison Sq West** is the single highest-volume pickup zone (4,475 trips).

---

## 🚀 How to Access / Run This Project

### View the dashboard only
Open `powerBi/uber analysis.pbix` directly in [Power BI Desktop](https://www.microsoft.com/en-us/power-platform/products/power-bi/downloads) (free) — no database connection needed to browse the existing visuals.

### Reproduce the full pipeline

**1. Clone the repo**
```bash
git clone https://github.com/Ishasavani1402/uber_analysis.git
cd uber_analysis
```

**2. Install dependencies**
```bash
pip install pandas sqlalchemy mysql-connector-python openpyxl python-dotenv
```

**3. Set up MySQL**
```bash
mysql -u root -p < analysis/datasetup.sql
```

**4. Configure environment variables**
Create a `.env` file in the project root:
```
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=uber
XLS_LOCATION=datasets/Location Table.xlsx
XLS_TRIP_DETAILS=datasets/Uber Trip Details.xlsx
```

**5. Run the ETL script**
```bash
python xls_to_mysql.py
```

**6. Clean the data**
Run `data_clean/data_clean.ipynb` in Jupyter to clean the raw tables and generate `clean_location` / `clean_trip_detail`.

**7. Run the analysis**
Execute the queries in `analysis/sql_analysis.sql` against your MySQL instance.

**8. Explore the dashboard**
Open `powerBi/uber analysis.pbix` in Power BI Desktop and point it at your local MySQL instance to refresh with your own data.

---

## 👤 Author

**Isha Savani**
Aspiring Data Analyst | Python · SQL · Power BI
GitHub: [@Ishasavani1402](https://github.com/Ishasavani1402)
