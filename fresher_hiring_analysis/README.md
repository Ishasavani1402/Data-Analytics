# 🎯 Fresher Hiring Intelligence: Decoding India's Entry-Level Job Market

> An end-to-end data analytics project analyzing 5,000 fresher job applications across 20+ Indian hiring platforms — uncovering what actually drives offers, where candidates drop off, and which platforms deliver real ROI.

---

## 📌 Project Overview

Every year, lakhs of fresh graduates apply across dozens of platforms — LinkedIn, Naukri, Internshala, Unstop, Apna, campus placements, and more — with almost no visibility into what actually works. Which platform converts best? What candidate profile gets hired? Where exactly does the hiring funnel break down?

This project answers those questions using a **complete analytics pipeline**: raw CSV → MySQL → Python (cleaning + EDA) → SQL-driven analysis → Power BI dashboard.

---

## ❓ Problem Statements

This project is built around **3 core business questions**:

1. **📊 Platform Effectiveness** — Which job platforms actually deliver the best shortlist rate, offer rate, response time, and salary outcomes for freshers?
2. **🕳️ Funnel Leakage** — At which stage of the hiring journey (Applied → Shortlisted → Interview → Offer) do most candidates drop off, and does this vary by platform or sector?
3. **🔑 Offer Predictors** — What candidate attributes (CGPA, internships, projects, skills, referrals, profile strength) most strongly influence whether someone gets hired?

---

## 🗂️ Dataset

| Detail | Description |
|---|---|
| **Source** | `fresher_hiring_india_dataset.csv` |
| **Size** | ~5,000 candidate records × 30 columns |
| **Coverage** | 20+ platforms (LinkedIn, Naukri, Indeed, Internshala, Unstop, Apna, Cutshort, Campus Placement, Referral, etc.) |
| **Key fields** | candidate demographics, education (degree/branch/CGPA), platform & application details, hiring stage, interview rounds, response time, offered salary, LinkedIn profile metrics |

---

## 🛠️ Tech Stack

| Layer | Tools Used |
|---|---|
| **Database** | MySQL (via SQLAlchemy + mysql-connector) |
| **Data Cleaning** | Python — pandas, numpy |
| **Analysis** | SQL (window functions, CTEs, CASE logic) + pandas |
| **Statistics** | scipy (chi-square test of independence) |
| **Visualization (Python)** | seaborn, matplotlib |
| **Dashboard** | Power BI |
| **Config Management** | python-dotenv (`.env` for DB credentials) |

---

## 🔄 Project Workflow

```
fresher_hiring_india_dataset.csv
        │
        ▼
┌──────────────────────┐
│   csv_mysql.py        │  → Loads raw CSV into MySQL (auto schema detection)
└──────────────────────┘
        │
        ▼
┌──────────────────────┐
│   data_clean.ipynb    │  → Cleaning, null handling, type fixes,
│                        │     feature engineering, writes clean_dataset
└──────────────────────┘
        │
        ▼
┌──────────────────────┐
│   EDA.ipynb           │  → 40+ SQL-driven business questions
│                        │     across 8 analytical phases
└──────────────────────┘
        │
        ▼
┌──────────────────────┐
│   Power BI Dashboard  │  → Interactive visual reporting layer
└──────────────────────┘
```

---

## 🧹 Data Cleaning Summary (`data_clean.ipynb`)

- ✅ Renamed ambiguous columns (`hiring_stage` → `current_hiring_stage`, `interview_rounds` → `interview_rounds_completed`) for clarity
- ✅ Handled nulls contextually — `interview_rounds_completed` and `offered_salary_inr` filled with `0` (candidates who haven't reached those stages yet)
- ✅ Stripped whitespace from all categorical/text columns
- ✅ Standardized `linkedin_premium` — converted blank values (78% of data) to `"Unknown"` instead of dropping or defaulting (avoids introducing bias)
- ✅ Parsed `application_date` into `application_year`, `application_month`, `application_day` for time-based analysis
- ✅ Verified row integrity (5,000 rows preserved end-to-end) and pushed clean data back to MySQL (`clean_dataset` table)

---

## 📊 Analysis Phases (`EDA.ipynb`)

The analysis is structured into **8 progressive phases**, moving from descriptive → diagnostic → predictive:

### Phase 1 — 📋 Foundational Descriptive
Platform-wise application volume, Tier-1 vs Tier-2/3 college distribution, work-type breakdown, candidate demographics, CGPA distribution, sector/role/salary landscape.

### Phase 2 — 🔻 Hiring Funnel & Conversion Analysis
Overall funnel breakdown (Applied → Offer), stage-wise distribution by platform, application-to-offer conversion rates, shortlist-to-offer conversion, referral impact on conversions.

### Phase 3 — ⏱️ Time & Responsiveness Analysis
Average response time by platform and sector, fastest/slowest responding platforms, response time vs. hiring outcome, metro vs. non-metro response patterns.

### Phase 4 — 💰 Compensation Analysis
Salary distribution by sector/role/work-type, CGPA-band salary trends, impact of internships/projects on offered salary, platform-wise salary benchmarking, gender pay gap analysis.

### Phase 5 — 🎯 Candidate Quality & Profile Impact
CGPA vs. advancement to later stages (with chi-square significance testing), top skills among offer recipients, LinkedIn profile completion vs. shortlist rate, LinkedIn Premium impact, projects/certifications vs. outcomes.

### Phase 6 — 🌍 Demographic & Regional Patterns
Gender-wise funnel drop-off comparison, city-wise offer rates (most/least competitive locations), graduation-year hiring trends, top colleges by hire volume and conversion quality.

### Phase 7 — 🏆 Platform Effectiveness & Benchmarking
Full platform scorecard (applications, shortlist rate, offer rate, response time, avg salary), niche vs. general platform comparison by role, sector-wise platform suitability, platforms favoring lower-CGPA candidates.

### Phase 8 — 🧩 Cohort, Segmentation & Intermediate Analysis
Candidate persona segmentation (academic strength × experience) and offer-rate comparison, graduation-year cohort trends, common rejection points, side-by-side funnel comparison across top 5 platforms.

---

## 📈 Power BI Dashboard

An interactive dashboard built on the cleaned MySQL dataset, consolidating:
- Platform performance scorecards
- Hiring funnel visualizations
- Compensation breakdowns by sector/role
- Demographic and regional hiring patterns

📁 File: `fresher_hiring_analysis.pbix`
🖼️ Preview: `fresher_hiring_analysis.dashboard.png`

---

## 📂 Repository Structure

```
fresher_hiring_analysis/
│
├── fresher_hiring_india_dataset.csv     # Raw dataset
├── clean_dataset.csv                     # Cleaned dataset (output)
├── csv_mysql.py                          # Loads CSV → MySQL with auto schema detection
├── data_clean.ipynb                      # Data cleaning & feature engineering
├── EDA.ipynb                             # 40+ phase-wise SQL/Python analyses
├── fresher_hiring_analysis.pbix          # Power BI dashboard file
├── fresher_hiring_analysis.dashboard.png # Dashboard preview image
├── .env.example                          # Sample env config (DB credentials)
└── README.md
```

---

## ⚙️ Setup & How to Run

1. **Clone the repo**
   ```bash
   git clone <your-repo-url>
   cd fresher_hiring_analysis
   ```

2. **Install dependencies**
   ```bash
   pip install pandas numpy seaborn matplotlib scipy sqlalchemy mysql-connector-python python-dotenv
   ```

3. **Configure environment variables**
   Copy `.env.example` → `.env` and fill in your MySQL credentials:
   ```
   DB_HOST=localhost
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_NAME=fresher_hiring
   CSV_FILE=./fresher_hiring_india_dataset.csv
   ```

4. **Load raw data into MySQL**
   ```bash
   python csv_mysql.py
   ```

5. **Run the notebooks in order**
   - `data_clean.ipynb` → cleans data & writes `clean_dataset` table
   - `EDA.ipynb` → runs all phase-wise analyses

6. **Open the dashboard**
   Open `fresher_hiring_analysis.pbix` in Power BI Desktop (ensure MySQL connection is configured under Data Source Settings).

---

## 🔍 Key Insights (Sample Highlights)

- 📌 **LinkedIn and Naukri dominate application volume**, but niche platforms like Internshala show competitive shortlist-to-offer conversion for specific roles.
- 📌 The steepest funnel drop-off occurs between **Applied → Shortlisted**, consistent across most platforms.
- 📌 **Referral applications show only a marginal conversion advantage** over non-referral applications — not a major differentiator in this dataset.
- 📌 CGPA alone shows **no statistically significant relationship** (chi-square test, p > 0.05) with reaching advanced interview stages — profile strength and experience matter more.
- 📌 **Profile completion % and projects/certifications** show measurable, if modest, positive association with shortlist and offer rates.

---

## 🚀 Future Enhancements

- 🔮 Build a predictive ML model (Logistic Regression / Random Forest) to estimate offer probability based on candidate profile
- 🧪 Add proportion/significance testing (z-test, chi-square) across all comparative metrics for statistical rigor
- 💵 Normalize stipend vs. CTC salary fields to a common annualized unit for more accurate compensation benchmarking
- 🌐 Automate the CSV → MySQL → Dashboard refresh pipeline

---

## 👤 Author

**Neha** — Data Analyst | SQL · Python · Power BI
📫 Open to feedback, suggestions, and collaboration!

---

⭐ If you found this project useful or insightful, consider giving it a star!
