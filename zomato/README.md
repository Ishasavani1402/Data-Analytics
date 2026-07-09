# 🍔 Zomato Delivery Time Analytics

End-to-end Python data analytics project on Zomato's food delivery dataset — from raw, messy data to cleaned data to business insights through EDA.

---

## 📌 Project Overview

This project analyzes **45,584 food delivery orders** across Indian cities to understand what actually drives delivery time — traffic, weather, rider profile, city type, festivals, and delivery stacking — and where the operational bottlenecks are.

**Goal:** Go beyond surface-level charts and find insights that would actually matter to delivery operations (staffing, rider allocation, SLA planning).

---

## 🗂️ Dataset

| | |
|---|---|
| Raw records | 45,584 orders |
| Cleaned records | 43,012 orders |
| Columns (cleaned) | 17 |
| Date range | Feb 11 – Apr 6, 2022 |
| Cities covered | Metropolitan, Urban, Semi-Urban |

**Key fields:** delivery person age & rating, order/pickup time, weather, road traffic density, vehicle type & condition, order type, multiple deliveries (stacking), festival flag, city type, time taken.

---

## 🛠️ Tech Stack

- **Python** — pandas, numpy
- **Visualization** — matplotlib, seaborn
- **Environment** — Jupyter Notebook

---

## 📁 Project Structure

```
zomato/
├── Zomato Dataset.csv      # Raw dataset
├── data_clean.ipynb        # Data cleaning & preprocessing
├── clean_dataset.csv       # Cleaned dataset (output of above)
├── EDA.ipynb               # Exploratory analysis + insights
└── README.md
```

---

## 🧹 Data Cleaning (`data_clean.ipynb`)

Real-world data had several issues that needed handling before any analysis:

- **Dropped identifier/geo columns** not needed for analysis (`ID`, `Delivery_person_ID`, lat/long columns)
- **Null handling:** median imputation for numeric fields (age, ratings), mode imputation for categorical fields (weather, traffic, city), `"No"` for missing festival flags
- **Fixed Excel time-fraction bug** — some `Time_Orderd` values were stored as Excel decimal fractions (e.g. `0.913`) instead of `HH:MM`, converted properly back to time
- **Fixed overnight order bug** — orders where pickup time appeared earlier than order time (crossing midnight) were corrected by rolling the date forward by one day
- **Engineered features:**
  - `Preparation_Time_Min` = pickup time − order time
  - `order_month`, `orderd_hour` extracted from date/time
  - `Vehicle_condition` mapped from numeric codes (0/1/2) to readable labels (Poor/Average/Good)
- Removed rows with irrecoverable nulls in time columns, stripped whitespace from all text columns, removed duplicates

**Result:** 43,012 clean records saved to `clean_dataset.csv`.

---

## 📊 Exploratory Analysis & Key Insights (`EDA.ipynb`)

### Delivery Stacking
Delivery time increases sharply with the number of stacked orders a rider is carrying:

| Multiple deliveries | Avg time |
|---|---|
| 0 | 22.92 min |
| 1 | 26.80 min |
| 2 | 40.43 min |
| 3 | 47.82 min |

→ Stacking 3 orders more than **doubles** delivery time vs. a single order.

### Road Traffic Density
| Traffic | Avg time |
|---|---|
| Jam | 31.15 min |
| High | 27.21 min |
| Medium | 26.72 min |
| Low | 21.21 min |

→ Jam conditions add ~10 minutes vs low traffic — the single biggest controllable delay factor.

### Weather Conditions
| Weather | Avg time |
|---|---|
| Cloudy | 29.04 min |
| Fog | 28.98 min |
| Windy | 26.21 min |
| Sandstorms | 26.00 min |
| Stormy | 25.98 min |
| Sunny | 21.81 min |

→ Cloudy/foggy conditions consistently slow deliveries by ~7 minutes vs sunny.

### City Type — Delivery Speed
| City | Avg time |
|---|---|
| Semi-Urban | **49.74 min** |
| Metropolitan | 27.21 min |
| Urban | 23.06 min |

→ Semi-Urban deliveries take almost **2x longer** than Metro/Urban, despite carrying the least volume (156 orders) — pointing to a rider-coverage gap rather than a demand problem.

### Festival Days
| Festival | Orders | Avg time |
|---|---|---|
| No | 42,162 | 25.98 min |
| Yes | 850 | **45.53 min** |

→ Festivals make up only ~2% of orders but delivery time jumps **~75%** — a strong case for temporary surge staffing during festival periods.

### Vehicle Condition & Type
- Vehicles in **Poor** condition take ~30.16 min vs ~24.5 min for Average/Good — maintenance directly affects delivery speed.
- **Scooters/electric scooters** are ~3 min faster on average than motorcycles.

### Delivery Partner Age
| Age group | Avg time | Avg rating |
|---|---|---|
| 20-24 | 23.06 min | 4.68 |
| 25-29 | 23.05 min | 4.68 |
| 30-34 | 29.60 min | 4.59 |
| 35-39 | 29.59 min | 4.60 |

→ Younger delivery partners (20-29) are both faster and rated higher than older partners — experience doesn't translate into speed here.

### Order & Time Patterns
- Order volume peaks in the **evening (7 PM–11 PM)**, consistent with dinner-time demand.
- Monthly order counts aren't directly comparable in this dataset — the date range covers only 8 days of February and 6 days of April vs a full 30 days of March, so raw monthly totals reflect data coverage, not real demand trends.
- Order type (Snack/Meal/Drinks/Buffet) shows almost no difference in delivery time (26.3–26.5 min) — order type itself isn't a delivery time driver.

---

## 💡 Key Business Takeaways

1. **Delivery stacking and traffic jams are the two biggest levers** for reducing delivery time — both are operationally controllable (stacking limits, route/traffic-aware dispatch).
2. **Semi-Urban areas need more riders**, not more marketing — the delay isn't demand-driven.
3. **Festival periods need temporary surge staffing** — low volume, high strain.
4. **Vehicle maintenance matters** — poor condition vehicles measurably slow deliveries.

---

## 🚀 How to Run

```bash
git clone <https://github.com/Ishasavani1402/Data-Analytics/tree/main/zomato>
cd zomato
pip install pandas numpy matplotlib seaborn
jupyter notebook data_clean.ipynb   # run first to generate clean_dataset.csv
jupyter notebook EDA.ipynb          # then run the analysis
```

---

## 👤 Author

**Neha**
📎 [GitHub](https://github.com/Ishasavani1402)

---

*Part of an ongoing data analytics portfolio focused on real-world, end-to-end analysis projects.*
