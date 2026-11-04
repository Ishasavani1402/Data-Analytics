# 🏥 Hospital Capacity & Appointment Bottleneck Analysis

![Python](https://img.shields.io/badge/Python-pandas%20%7C%20seaborn%20%7C%20scipy-3776AB?logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-DAX%20%7C%20Dashboard-F2C811?logo=powerbi&logoColor=black)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebooks-F37626?logo=jupyter&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-success)

> An end-to-end analytics project that finds **where appointment bottlenecks occur**, **how much capacity is lost to no-shows**, and **which levers (reminders, rebooking, scheduling) can win it back** for a multi-specialty private clinic chain.

---

## 📌 Table of Contents
- [Business Problem](#-business-problem)
- [Key Results at a Glance](#-key-results-at-a-glance)
- [Dashboard Preview](#-dashboard-preview)
- [Dataset](#-dataset)
- [Tech Stack](#-tech-stack)
- [Project Workflow](#-project-workflow)
- [Key Findings](#-key-findings)
- [Business Recommendations](#-business-recommendations)
- [Power BI Data Model](#-power-bi-data-model)
- [Repository Structure](#-repository-structure)
- [How to Run](#-how-to-run)
- [Limitations & Data Notes](#-limitations--data-notes)
- [About the Author](#-about-the-author)

---

## 🎯 Business Problem

A multi-specialty private hospital chain is seeing a **widening gap between appointment demand and capacity**. Patients report longer waits, and the Clinical Operations Director needs answers to three questions:

| # | Question | Where it is answered |
|---|----------|----------------------|
| A | **Where is the bottleneck?** (wait times by clinic, time, patient group) | EDA Q1–Q4 |
| B | **How is demand spread against capacity?** (volume by clinic, day, hour) | EDA Q5–Q7 |
| C | **How much capacity is lost to waste?** (no-shows, rebooking, reminders) | EDA Q8–Q10 |

---

## 📊 Key Results at a Glance

| KPI | Value |
|-----|-------|
| Total appointments analysed | **10,000** |
| Unique patients | **2,890** |
| Average appointment wait | **13.5 days** (vs a 7-day clinical target) |
| Appointments waiting over 7 days / over 14 days | **71.4% / 46.7%** |
| No-show rate | **21.4%** (2,143 appointments) |
| No-show slots successfully rebooked | **41.5%** (890 slots) |
| No-show slots left empty | **58.5%** (1,253 slots) |
| Estimated revenue lost to no-shows | **£221K** (`estimated_revenue_loss_gbp`) |

---

## 🖼️ Dashboard Preview

![Dashboard](power%20Bi/Hospital%20Capacity%20and%20Appointment%20Bottleneck%20Analysis%20dashboard.png)

**Auto-generated insight panel.** A single DAX measure writes the dashboard's key takeaways in plain English, and it updates with every slicer and filter.

![Dashboard Key Insights](power%20Bi/dashboard%20insights.png)

The dashboard has three slicers (`year_month`, `reminder_response`, `clinic_type`), five KPI cards and five visuals:

1. Column chart: average appointment wait days by clinic type
2. Line chart: average appointment wait days by month
3. Column chart: no-show rate by reminder response
4. Donut chart: no-show rate by clinic type
5. Matrix: average daily appointments by weekday for each clinic type

---

## 🗂️ Dataset

| Property | Detail |
|----------|--------|
| File | `uk_private_clinic_no_show_2026.csv` (raw) → `clean_dataset.csv` (cleaned) |
| Size | 10,000 rows × 30 columns |
| Appointment period | 02 Jan 2026 – 26 Jan 2027 (bookings made 01 Jan – 31 Dec 2026) |
| Clinics | 5 sites (`C1`–`C5`) across 4 clinic types: GP, Dental, Dermatology, Physiotherapy |
| Patients | 2,890 unique patients across 10 UK postcode areas |

**Column groups**

- **Patient:** `age`, `gender`, `postcode_area`, `distance_to_clinic_km`, `has_chronic_condition_flag`
- **Appointment:** `clinic_id`, `clinic_type`, `appointment_type`, `consultation_fee`, `appo_book_date`, `appointment_date`, `appointment_day_of_week`, `appointment_hour`, `appo_wait_days`
- **Reminders:** `sms_reminder_sent`, `reminder_hours_before`, `reminder_response`, `number_of_reminders_sent`
- **Outcome / cost:** `no_show`, `slot_rebooked_flag`, `estimated_revenue_loss_gbp`, `staff_idle_cost_gbp`

---

## 🛠️ Tech Stack

| Layer | Tools |
|-------|-------|
| Data cleaning & EDA | Python, pandas, NumPy |
| Visualisation (EDA) | matplotlib, seaborn |
| Statistical testing | SciPy (one-way ANOVA across patient groups) |
| BI & dashboard | Power BI Desktop, DAX |
| Environment | Jupyter Notebook |

---

## 🔄 Project Workflow

```
Raw CSV ──► data_clean.ipynb ──► clean_dataset.csv ──► EDA.ipynb ──► Power BI dashboard
            (quality checks)      (analysis-ready)     (10 business     (KPIs, DAX measures,
                                                         questions)       auto-insights)
```

### 1️⃣ Data Cleaning: `notebooks/data_clean.ipynb`
- Standardised column names (lowercase, underscores) and renamed fields for clarity (`days_in_advance` → `appo_wait_days`, `scheduled_date` → `appo_book_date`, and so on)
- Checked for nulls and duplicates across all 30 columns
- **Business-rule validations (all passed with 0 invalid records):**
  - Booking date is never after the appointment date
  - Consultation fee is always greater than £0
  - `appo_wait_days` equals `appointment_date − appo_book_date` for every row
- Trimmed whitespace in text columns and checked unique categories (4 clinic types, 7 weekdays, 4 reminder responses)
- Exported the final `clean_dataset.csv`

### 2️⃣ Exploratory Analysis: `notebooks/EDA.ipynb`
- Correlation heatmap, which flagged leakage features (see [Limitations](#-limitations--data-notes))
- IQR-based outlier detection on numeric columns
- KPI calculation, then **10 business questions** grouped into three themes (A: bottlenecks, B: demand vs capacity, C: hidden capacity loss)

### 3️⃣ Dashboard: `power Bi/*.pbix`
- Built a dedicated `_Measures` table with all DAX measures
- Added calculated columns for time-based analysis (`year_month`, `day_of_week`, `week_start`, `is_full_week`)
- Wrote dynamic text measures so the dashboard narrates its own insights

---

## 🔍 Key Findings

### A. Where is the bottleneck?
- **Waits are system-wide, not a one-clinic problem.** The average wait is 13.5 days against a 7-day target, and 71.4% of appointments wait longer than 7 days. Clinic averages differ by only 0.23 days: Dental 13.68, Physiotherapy 13.59, Dermatology 13.50, GP 13.45.
- **Nearly half of appointments exceed 14 days.** Dental is highest at 48.2% and the range across clinics is 45.9% to 48.2%.
- **No patient group waits meaningfully longer.** New vs follow-up, chronic vs not, every age band and gender all sit between 13.2 and 13.7 days.
- **One exception: patients living 20+ km away wait 15.9 days**, about 2.3 days above average. This is a small group of 106 appointments, so treat it as a signal and not a conclusion.

### B. Demand vs capacity
- **GP carries about 45% of weekly volume**, around 12 appointments a day. That is more than Dental and Physiotherapy combined.
- **Dermatology is the lightest** at about 4 appointments a day (about 15% of volume), making it the most likely candidate to absorb extra demand.
- **Load is evenly spread across days and hours.** Friday is busiest (1,470 appointments) and Sunday quietest (1,383). Peak hour is 9 AM (1,150) and the quietest is 2 PM (1,053).

### C. Hidden capacity lost to waste
- **21.4% of appointments are no-shows.** Highest-risk slots are **5 PM (23.9%)** and **Tuesday (22.9%)**. Clinic rates are similar, from Dermatology at 20.1% to Physiotherapy at 21.8%.
- **58.5% of no-show slots (1,253) are never refilled.** Only 41.5% get rebooked.
- **Reminders work.** Patients who got an SMS had a 20.2% no-show rate, against 26.6% for those with no reminder.

| Reminder response | Appointments | No-show rate |
|-------------------|-------------:|-------------:|
| Confirmed | 5,146 | **16.6%** |
| Ignored | 2,050 | 25.3% |
| No Reminder | 1,969 | 26.6% |
| Cancelled | 835 | 29.7% |

- **Timing matters a little:** a reminder sent 72 hours before the appointment has the lowest no-show rate (20.0%, vs 22.0% at 24 hours and 22.3% at 48 hours). Sending a second reminder adds almost nothing (21.1% vs 21.6%).

---

## 💡 Business Recommendations

1. **Treat the 13.5-day wait as a system-wide capacity and scheduling problem.** Every clinic type is about 6.5 days over target, so fixing one department will not close the gap.
2. **Send an SMS reminder with every booking.** Almost 1 in 5 appointments gets no reminder, and those patients no-show about 6 points more often. Prefer 72 hours before the appointment.
3. **Refill empty slots.** A waitlist or same-day recall process could recover part of the 1,253 slots that stay empty after a no-show.
4. **Target high-risk slots** such as 5 PM and Tuesdays with extra confirmation calls or controlled overbooking.
5. **Test shifting some demand towards Dermatology,** which has the lowest volume share and a similar wait. This needs real capacity data first (see below).
6. **Offer remote or telehealth follow-ups** to patients living 20+ km away, who wait the longest.

---

## 🧮 Power BI Data Model

**Source table:** `clean_dataset`  |  **Measures table:** `_Measures`

### Core KPI measures
| Measure | Logic |
|---------|-------|
| `Average Appointment Wait Days` | `AVERAGE(clean_dataset[appo_wait_days])` |
| `Total Appointment` | `DISTINCTCOUNT(clean_dataset[appointment_id])` |
| `Total Patient` | `DISTINCTCOUNT(clean_dataset[patient_id])` |
| `No Show Rate` | `DIVIDE(SUM(no_show), COUNTROWS(clean_dataset))` |
| `Slot Rebook Rate` | `DIVIDE(SUM(slot_rebooked_flag), SUM(no_show))`, since only no-shows can be rebooked |

### Throughput measures (complete weeks only)
| Measure | Purpose |
|---------|---------|
| `Avg Daily Appointments` | Mean daily volume per clinic, excluding partial first and last weeks |
| `Avg Weekly Appointments` | Mean weekly volume per clinic |
| `Weekly Share %` | Each clinic's share of total weekly volume |

### Dynamic insight measures
`Dashboard Headline`, `Insight 1 Wait by Clinic`, `Insight 2 Wait Trend`, `Insight 3 No Show by Reminder`, `Insight 4 No Show by Clinic`, `Insight 5 Weekday Volume`. These are combined into one measure, **`Dashboard Key Insights`**, which feeds the text card on the dashboard.

### Calculated columns
`year_month` (YYYY-MM text, sorts chronologically) · `day_of_week` (sorted Mon→Sun via `weekday_no`) · `week_start` · `is_full_week`

---

## 📁 Repository Structure

```
Hospital Capacity and Appointment Bottleneck Analysis/
│
├── datasets/
│   ├── uk_private_clinic_no_show_2026.csv   # raw data
│   └── clean_dataset.csv                    # cleaned, analysis-ready data
│
├── notebooks/
│   ├── data_clean.ipynb                     # cleaning + data quality checks
│   └── EDA.ipynb                            # 10 business questions
│
├── power Bi/
│   ├── Hospital Capacity and Appointment Bottleneck Analysis dashboard.pbix
│   ├── Hospital Capacity and Appointment Bottleneck Analysis dashboard.png
│   └── dashboard insights.png
│
└── README.md
```

---

## ▶️ How to Run

**1. Python notebooks**
```bash
pip install pandas numpy seaborn matplotlib scipy jupyter
jupyter notebook
```
Run `notebooks/data_clean.ipynb` first, then `notebooks/EDA.ipynb`.

> The notebooks use Windows-style paths (`..\datasets\...`). On macOS or Linux, change them to `../datasets/...`.

**2. Power BI dashboard**
Open the `.pbix` file in **Power BI Desktop**. If prompted, update the data source to point at `datasets/clean_dataset.csv`.

---

## ⚠️ Limitations & Data Notes

- **No true capacity field.** The dataset has no slots-available or occupancy column, so "which clinic can absorb more demand" is inferred from volume and wait times only. Real capacity data would sharpen this.
- **Edge months in the trend are artefacts.** Bookings only run from 1 Jan to 31 Dec 2026. January 2026 looks short (9.6 days) because no long-lead bookings could have been made before the data starts. January 2027 looks high (18.3 days) because it only contains appointments booked far in advance. The stable trend is roughly 13–14 days for Feb–Dec 2026.
- **Leakage features.** `estimated_revenue_loss_gbp` and `staff_idle_cost_gbp` correlate about 0.94 with `no_show`, and `slot_rebooked_flag` about 0.60. They are consequences of a no-show, so exclude them if building a predictive model.
- **"Cancelled" has the highest no-show rate (29.7%).** A cancellation is not normally a no-show, so check how this field is recorded at source before drawing conclusions from it.
- **Associations, not proof of cause.** Patients who confirm reminders may simply be more reliable, so the effect of reminders themselves is likely smaller than the gap shown.

---

## 👩‍💻 About the Author

**Isha** is an aspiring Data Analyst building an end-to-end portfolio across Python, SQL and Power BI.

🔗 GitHub: [Ishasavani1402](https://github.com/Ishasavani1402)

⭐ If you found this project useful, please consider starring the repository.
