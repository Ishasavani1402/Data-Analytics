# 🍎 Apple Product Price Analysis (2020–2026)

## 📌 Project Overview

This project analyzes Apple product pricing data collected from **Amazon** and **Flipkart** between **2020 and 2026**. The objective is to understand pricing trends, discount strategies, product performance, stock availability, and platform-wise price differences through SQL, Python, and Power BI.

The project follows a complete data analytics workflow—from data cleaning and exploration to business analysis and interactive dashboard creation.

---

## 🎯 Problem Statement

Online marketplaces frequently change product prices based on demand, sales events, competition, and inventory. Without proper analysis, it becomes difficult to identify:

- Pricing trends over time
- Best discount opportunities
- Platform-wise price differences
- Product categories receiving maximum discounts
- Customer value retention
- Impact of sale events on pricing
- Overall market behavior

This project aims to solve these challenges by converting raw pricing data into meaningful business insights.

---

# 📂 Dataset Information

- **Domain:** E-Commerce
- **Industry:** Consumer Electronics
- **Products:** Apple Devices
- **Time Period:** 2020–2026
- **Records:** 80,000+
- **Platforms:**
  - Amazon
  - Flipkart

### Dataset Features

- Date
- Platform
- Product Category
- Model Name
- Product Condition
- Launch Price (USD & INR)
- Current Price (USD & INR)
- Discount Percentage
- Sale Event
- Product Rating
- Stock Status

---

# 🛠 Tech Stack

| Tool | Purpose |
|------|----------|
| Python | Data Cleaning & Analysis |
| Pandas | Data Manipulation |
| Matplotlib | Data Visualization |
| Seaborn | Statistical Visualization |
| MySQL | Business Query Analysis |
| SQLAlchemy | Database Connection |
| Jupyter Notebook | Development Environment |
| Power BI | Interactive Dashboard |

---

# 📊 Project Workflow

## 1. Data Collection

- Imported pricing dataset
- Loaded into MySQL
- Connected Python using SQLAlchemy

---

## 2. Data Cleaning

Performed several preprocessing steps including:

- Removing duplicate records
- Handling missing values
- Correcting data types
- Cleaning categorical values
- Formatting dates
- Standardizing column names
- Data validation

---

## 3. SQL Business Analysis

Performed extensive SQL analysis to answer business questions such as:

- Platform-wise pricing comparison
- Product category distribution
- Average discount by category
- Sale event analysis
- Price stability over years
- Value retention analysis
- Best pricing opportunities
- Highest discounted products
- Inventory availability
- Product rating trends
- Market activity comparison

---

## 4. Python Exploratory Data Analysis (EDA)

Created visualizations to identify patterns and trends including:

- Line Charts
- Bar Charts
- Box Plots
- Heatmaps
- Pie Charts
- Histogram
- Scatter Plots
- Distribution Analysis

---

## 5. Power BI Dashboard

Built an interactive dashboard featuring:

- KPI Cards
- Filters & Slicers
- Platform Comparison
- Category Analysis
- Discount Analysis
- Year-wise Trends
- Product Performance
- Price Distribution
- Stock Availability
- Interactive Visualizations

---

# 📈 Key Business Insights

- Compared Apple product pricing across Amazon and Flipkart.
- Identified categories receiving the highest average discounts.
- Analyzed pricing trends from 2020–2026.
- Evaluated the effect of sale events on product pricing.
- Measured product value retention using launch and current prices.
- Discovered pricing patterns by product condition.
- Examined inventory availability alongside pricing.
- Identified premium and budget product segments.

---

# 📊 Dashboard Preview

> *refere from above apple_products_price_analysis dashboard.png*

```
Dashboard/
│
├── Overview Page
├── Platform Analysis
├── Discount Analysis
├── Product Category Analysis
└── Pricing Trend Analysis
```

---

# 📁 Project Structure

```
Apple-Product-Price-Analysis/
│
├── Dataset/
│   └── apple_product_price.csv
│
├── SQL/
│   ├── Data Cleaning.sql
│   ├── Business Queries.sql
│
├── Python/
│   ├── Data Cleaning.ipynb
│   ├── EDA.ipynb
│
├── Power BI/
│   └── Apple_Product_Price_Analysis.pbix
│
├── Images/
│   └── Dashboard Screenshots
│
├── README.md

```

---

# 🚀 How to Run This Project

## 1. Clone the Repository

```bash
git clone https://github.com/Ishasavani1402/Data-Analytics/tree/main/apple_products_price
```

---

## 2. Install Required Libraries

```bash
pip install pandas matplotlib seaborn sqlalchemy pymysql
```

---

## 3. Import Dataset

- Import the dataset into MySQL.
- Update your database credentials in the notebook.

Example:

```python
engine = create_engine("mysql+pymysql://username:password@localhost/database_name")
```

---

## 4. Run SQL Queries

Execute the SQL scripts to perform business analysis.

---

## 5. Run Python Notebook

Open Jupyter Notebook and execute:

- Data Cleaning
- Exploratory Data Analysis

---

## 6. Open Power BI Dashboard

Open the `.pbix` file using Power BI Desktop.

---

# 📌 Skills Demonstrated

- Data Cleaning
- SQL Query Writing
- Exploratory Data Analysis
- Data Visualization
- Business Analytics
- Dashboard Development
- Pricing Analysis
- Statistical Analysis
- Data Storytelling

---

# 🎯 Business Value

This project demonstrates how data analytics can help businesses:

- Optimize pricing strategies
- Monitor market trends
- Compare competitor pricing
- Improve inventory planning
- Understand discount effectiveness
- Support data-driven pricing decisions

---

# 📷 Sample Visualizations

Include screenshots such as:

- SQL Query Results
- Python Charts
- Power BI Dashboard
- KPI Overview
- Pricing Trend Analysis

---

# 👩‍💻 Author

**Isha Savani**

Data Analyst | SQL | Python | Power BI | Data Visualization


GitHub: *https://github.com/Ishasavani1402*

---

## ⭐ If you found this project useful, consider giving it a star!