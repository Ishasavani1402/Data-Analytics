# 🍕 Domino's Sales Analysis — End-to-End Data Analytics Project

## 📌 Project Overview

This project is an **end-to-end Data Analytics solution for Domino's pizza sales**, built to transform raw transactional data into actionable business insights.

The project follows a practical analytics workflow:

**Raw CSV Data → MySQL → Python Data Cleaning → SQL Analysis → Python EDA → Power BI Dashboard**

The analysis focuses on understanding **revenue, order behavior, pizza performance, customer ordering patterns, time-based trends, category contribution, pizza size performance, and operational opportunities**.

The project is designed to demonstrate how a Data Analyst can move from raw business data to a structured analytical solution and an interactive dashboard.

---

## 🎯 Business Problem

A pizza business generates a large volume of transactional data, but raw order records alone do not answer important business questions.

The key objective of this project is to analyze sales data and answer questions such as:

- 💰 How much total revenue is being generated?
- 🍕 Which pizza categories generate the most revenue?
- 📏 Which pizza sizes contribute the most revenue?
- 🏆 Which pizzas are the best sellers?
- 📅 Which days generate the most orders and revenue?
- ⏰ What are the peak ordering hours?
- 📈 How does revenue change month over month?
- 🧾 What is the Average Order Value (AOV)?
- 📊 How does AOV differ between weekdays and weekends?
- 🍕 Which pizzas underperform in both revenue and quantity?
- 📦 Which orders contain unusually high item quantities?
- 🏅 Which pizza is the top seller in each month?
- 🥇 Which pizza performs best within each category?
- 📆 Are there any calendar dates with zero orders?

The goal is not only to calculate metrics, but to convert them into **business-oriented insights that can support sales, menu, marketing, and operational decisions**.

---

## 📂 Project Structure

```text
Dominozz_sale_analysis/
│
├── datasets/
│   ├── orders.csv
│   ├── order details.csv
│   ├── pizzas.csv
│   └── pizza types.csv
│
├── notebooks/
│   ├── data_clean.ipynb
│   └── EDA.ipynb
│
├── PowerrBi/
│   ├── dominos sales analysis.pbix
│   ├── dominos sales analysis dashboard.png
│   └── dominos-logo4.png
│
├── csv_to_mysql.py
├── requirements.txt
└── README.md
```

### 📁 Folder & File Description

| File / Folder | Purpose |
|---|---|
| `datasets/` | Contains the raw CSV datasets |
| `notebooks/data_clean.ipynb` | Data inspection, cleaning, validation, and loading of cleaned tables |
| `notebooks/EDA.ipynb` | SQL-based business analysis and Python visualizations |
| `PowerrBi/` | Power BI dashboard, PBIX file, and dashboard assets |
| `csv_to_mysql.py` | Python script for loading CSV files into MySQL |
| `requirements.txt` | Project dependency file |
| `README.md` | Project documentation |

---

# 🗃️ Dataset Overview

The project contains four related datasets.

### 1. `orders.csv`

Contains order-level information.

| Column | Description |
|---|---|
| `Order Id` | Unique order identifier |
| `Order Date` | Date on which the order was placed |
| `Order Time` | Time at which the order was placed |

**Records:** 21,350  
**Columns:** 3

---

### 2. `order details.csv`

Contains individual pizza items associated with each order.

| Column | Description |
|---|---|
| `Order Details Id` | Unique order-detail identifier |
| `Order Id` | Associated order identifier |
| `Pizza Id` | Associated pizza identifier |
| `Quantity` | Quantity of the pizza ordered |

**Records:** 48,620  
**Columns:** 4

> `order_id` is intentionally not required to be unique in this table because a single order can contain multiple pizza items.

---

### 3. `pizzas.csv`

Contains pizza SKU/size/price information.

| Column | Description |
|---|---|
| `Pizza Id` | Unique pizza identifier |
| `Pizza Type Id` | Associated pizza type |
| `Size` | Pizza size |
| `Price` | Pizza price |

**Records:** 96  
**Columns:** 4

---

### 4. `pizza types.csv`

Contains pizza-level descriptive information.

| Column | Description |
|---|---|
| `Pizza Type Id` | Unique pizza type identifier |
| `Name` | Pizza name |
| `Category` | Pizza category |
| `Ingredients` | Ingredients used |

**Records:** 32  
**Columns:** 4

---

# 🔄 Data Analytics Workflow

## 1️⃣ Data Loading

The raw CSV files are first loaded into **MySQL**.

The project includes:

```text
csv_to_mysql.py
```

This script:

- Connects to MySQL using environment variables
- Reads the CSV files with Pandas
- Detects Pandas data types
- Maps Pandas data types to MySQL data types
- Creates MySQL tables
- Handles NULL values
- Inserts records in chunks
- Loads all four datasets into MySQL

The script uses explicit table mappings for:

```text
orders
order_details
pizza_types
pizzas
```

---

# 🧹 Data Cleaning & Validation

Data cleaning is performed in:

```text
notebooks/data_clean.ipynb
```

### Cleaning steps include:

### 🔹 Data Inspection

For every table, the project checks:

- Dataset shape
- Data types
- Missing values
- Duplicate rows
- Sample records

### 🔹 Column Standardization

Column names are standardized by:

- Removing leading/trailing spaces
- Converting names to lowercase
- Replacing spaces with underscores

Example:

```text
Order Id
```

becomes:

```text
order_id
```

### 🔹 Categorical Data Cleaning

Leading and trailing whitespace is removed from object/string columns.

The project also performs a validation check to ensure that unwanted whitespace does not remain.

### 🔹 Relationship Validation

The project validates relationships between tables by checking:

- `order_details.order_id` against `orders.order_id`
- `order_details.pizza_id` against `pizzas.pizza_id`
- `pizzas.pizza_type_id` against `pizza_types.pizza_type_id`

### 🔹 Duplicate Validation

Uniqueness is checked for:

- `orders.order_id`
- `pizzas.pizza_id`
- `pizza_types.pizza_type_id`

The order-detail table is treated differently because one order can contain multiple pizza records.

### 🔹 Date & Time Conversion

`order_date` and `order_time` are converted using Pandas datetime functionality:

```python
pd.to_datetime(..., errors='coerce')
```

### 🔹 Clean Tables

The cleaned datasets are written back to MySQL as:

```text
orders_clean
order_details_clean
pizzas_clean
pizza_types_clean
```

---

# 🗄️ MySQL Data Model

The project uses four main logical entities:

```text
orders
   │
   │ order_id
   ▼
order_details
   │
   │ pizza_id
   ▼
pizzas
   │
   │ pizza_type_id
   ▼
pizza_types
```

This relational structure allows order-level information to be combined with pizza, size, price, and category information.

---

# 🔎 SQL Business Analysis

SQL analysis is performed in:

```text
notebooks/EDA.ipynb
```

The project answers **18 business questions**.

### 💰 Revenue & Sales Performance

1. Total revenue generated
2. Revenue by pizza category
3. Revenue by pizza size
4. Top 5 best-selling pizzas by revenue
5. Average Order Value (AOV)
6. Monthly revenue trend
7. Orders and revenue by day of week
8. Peak order hours
9. Month-over-month revenue growth
10. Revenue/order behavior across pizza sizes

### 📊 Customer & Operational Patterns

11. Category performance across weekdays and weekends
12. Underperforming pizzas based on revenue and quantity
13. Revenue contribution percentage by category
14. Identification of bulk orders
15. Weekday vs weekend AOV
16. Monthly #1 best-selling pizza
17. Top pizza within each category
18. Calendar dates with zero orders

---

# 🐍 Python Exploratory Data Analysis

Python is used for exploratory analysis and visualization.

### Libraries Used

- **Pandas** — Data manipulation and analysis
- **NumPy** — Numerical operations
- **Matplotlib** — Data visualization
- **Seaborn** — Statistical visualization
- **SQLAlchemy** — MySQL database connectivity

### Visualizations include:

- 📊 Revenue by pizza category
- 📏 Revenue by pizza size
- 🏆 Top 5 pizzas by revenue
- 📈 Monthly revenue trend
- 📅 Orders by day of week
- 💰 Revenue by day of week
- ⏰ Hourly order trend
- 📦 Orders vs average order value by pizza size
- 🥧 Revenue contribution by category

---

# 📈 Power BI Dashboard

The final interactive dashboard is built using **Microsoft Power BI**.

### Dashboard File

```text
PowerrBi/dominos sales analysis.pbix
```

The dashboard provides an interactive business view of Domino's sales performance.

### Dashboard capabilities include:

- 💰 Revenue KPIs
- 🧾 Order KPIs
- 🍕 Pizza performance analysis
- 📊 Category-level analysis
- 📏 Pizza size analysis
- 📅 Time-based sales trends
- ⏰ Order-hour analysis
- 🏆 Best-performing pizzas
- 🔎 Interactive filtering and exploration

### Dashboard Preview

![Domino's Sales Analysis Dashboard](PowerrBi/dominos%20sales%20analysis%20dashboard.png)

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | Data cleaning, analysis & visualization |
| **Pandas** | Data manipulation |
| **NumPy** | Numerical analysis |
| **Matplotlib** | Visualization |
| **Seaborn** | Visualization |
| **MySQL** | Data storage & SQL analysis |
| **SQLAlchemy** | Python–MySQL connectivity |
| **Power BI** | Interactive dashboard & reporting |
| **DAX** | Power BI calculations |
| **Power Query** | Data transformation |
| **Jupyter Notebook** | Analysis environment |
| **Git & GitHub** | Version control & project sharing |

---

# 🚀 How to Access & Run the Project

## 1. Clone the Repository

```bash
git clone https://github.com/Ishasavani1402/powerBi.git
```

Navigate to the project:

```bash
cd powerBi/Dominozz_sale_analysis
```

---

## 2. Install Python Dependencies

Create/activate your Python environment and install the required libraries.

```bash
pip install pandas numpy matplotlib seaborn sqlalchemy mysql-connector-python python-dotenv jupyter
```

> The project also includes `requirements.txt`; use it when the dependency list is populated for your environment.

---

## 3. Prepare MySQL

Create a MySQL database for the project.

For example:

```sql
CREATE DATABASE dominos;
```

Make sure your MySQL server is running and that your user has permission to create and replace tables.

---

## 4. Configure Environment Variables

The project uses environment variables for database credentials rather than hard-coding credentials in the Python scripts.

Create a `.env` file in the project directory and configure values similar to:

```env
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=dominos

CSV_ORDERS=path/to/orders.csv
CSV_ORDER_DETAILS=path/to/order details.csv
CSV_PIZZA_TYPES=path/to/pizza types.csv
CSV_PIZZAS=path/to/pizzas.csv
```

### ⚠️ Security

Do **not** commit your `.env` file or database password to GitHub.

Add this to `.gitignore`:

```gitignore
.env
__pycache__/
```

---

## 5. Load CSV Data into MySQL

Run:

```bash
python csv_to_mysql.py
```

The script creates and loads the following tables:

```text
orders
order_details
pizza_types
pizzas
```

---

## 6. Run Data Cleaning Notebook

Open:

```text
notebooks/data_clean.ipynb
```

Run the notebook to:

- Inspect the raw tables
- Validate relationships
- Check duplicates and missing values
- Standardize column names
- Remove unwanted whitespace
- Convert date/time fields
- Create cleaned MySQL tables

The cleaned tables are:

```text
orders_clean
order_details_clean
pizzas_clean
pizza_types_clean
```

---

## 7. Run EDA & SQL Analysis

Open:

```text
notebooks/EDA.ipynb
```

This notebook connects to MySQL and performs the project's SQL analysis and Python visualizations.

Make sure your environment variables are configured before running the notebook.

---

## 8. Open the Power BI Dashboard

Open:

```text
PowerrBi/dominos sales analysis.pbix
```

Use **Power BI Desktop** to explore the interactive dashboard.

If your local database connection differs from the original setup, update the data source/credentials in Power BI before refreshing the report.

---

# 📊 Key Analytical Areas

The project focuses on several important business dimensions:

### 🍕 Product Performance
- Best-selling pizzas
- Underperforming pizzas
- Category performance
- Pizza size performance

### 💰 Revenue Analysis
- Total revenue
- Category revenue
- Size-level revenue
- Monthly revenue
- Revenue contribution
- Revenue growth

### 🧾 Order Behavior
- Total orders
- Average Order Value
- Bulk orders
- Orders by pizza size

### ⏰ Time Analysis
- Monthly trends
- Day-of-week patterns
- Weekday vs weekend
- Peak ordering hours
- Zero-order dates

### 🏆 Ranking Analysis
- Top 5 pizzas
- Monthly top seller
- Top pizza within each category

---

# 💡 Business Value

The analysis can help a pizza business make better decisions around:

### 📌 Menu Optimization
Identify high-performing and underperforming pizzas and categories.

### 📌 Inventory Planning
Understand which products and sizes generate the highest demand.

### 📌 Marketing Strategy
Identify high-performing days, hours, categories, and products for targeted promotions.

### 📌 Revenue Optimization
Track AOV, monthly revenue, category contribution, and growth patterns.

### 📌 Operational Planning
Use peak-hour and peak-day demand patterns to better plan staffing and inventory.

---

# 🧠 Key Skills Demonstrated

This project demonstrates practical experience in:

- End-to-end Data Analytics
- Data Cleaning & Validation
- Relational Data Understanding
- Python & Pandas
- MySQL & Advanced SQL
- CTEs
- Window Functions
- Aggregations
- Ranking
- Time-based Analysis
- Business KPI Analysis
- Exploratory Data Analysis
- Data Visualization
- Power BI Dashboard Development
- Data Storytelling
- Business Problem Solving

---

# 📌 Project Outcome

The project converts four raw transactional datasets into a structured analytical workflow:

```text
                 RAW DATA
                     │
                     ▼
              CSV DATASETS
                     │
                     ▼
                  MySQL
                     │
                     ▼
            DATA VALIDATION
                     │
                     ▼
            PYTHON CLEANING
                     │
                     ▼
             CLEAN MYSQL TABLES
                     │
             ┌───────┴────────┐
             ▼                ▼
        SQL ANALYSIS      PYTHON EDA
             │                │
             └───────┬────────┘
                     ▼
              POWER BI DASHBOARD
                     │
                     ▼
            BUSINESS INSIGHTS
```

This structure reflects a practical **Data Analyst workflow**, from raw data ingestion through data preparation, analytical querying, visualization, and final business reporting.

---

# 👩‍💻 Author

**Isha Savani**

MCA Student | Data Analyst | Power BI & SQL Enthusiast

### Skills

`Python` `SQL` `MySQL` `Power BI` `DAX` `Pandas` `Data Analytics` `Data Visualization`

---

## ⭐ If You Found This Project Useful

Feel free to explore the repository, review the notebooks, and check out the Power BI dashboard.

If you find the project useful, consider giving the repository a ⭐.

**GitHub:**  
https://github.com/Ishasavani1402/powerBi
