# RetailIQ: Global Sales Intelligence & RFM Analytics Platform


##  What Is This Project?

**RetailIQ** is a production-grade, end-to-end Big Data pipeline that processes over half a million raw e-commerce transactions using distributed cloud computing and surfaces executive-level business intelligence through an enterprise Power BI dashboard.

This project bridges **Data Engineering** and **Business Analytics** — ingesting messy raw data at scale using PySpark on Databricks, modeling it with advanced SQL, and delivering a 2-page dark-mode SaaS-style dashboard with RFM customer segmentation.

> **Business Problem:** E-commerce companies generate millions of transactions daily but struggle to identify which customers are high-value, which are about to churn, and which markets are driving growth — without a scalable data pipeline.
>
> **Solution:** RetailIQ processes 541,910 raw transactions, cleans and validates them at scale using Spark, runs 8 advanced SQL analytics queries, and delivers actionable intelligence — including £1.4M+ in at-risk revenue identified before churn occurs.

---

##  Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    RAW DATA SOURCE                      │
│         UCI Online Retail II Dataset                    │
│         541,910 rows · 8 columns · 2010–2011            │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│         CLOUD INGESTION — Azure ADLS + Databricks       │
│  • Uploaded CSV to Azure Data Lake Storage Volume       │
│  • Read natively using spark.read.csv()                 │
│  • inferSchema=True for automatic type detection        │
│  • 541,910 rows loaded into Spark DataFrame             │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│          DATA PROFILING — 4-Pillar X-Ray                │
│  • Null X-Ray    : 135,080 missing Customer IDs         │
│  • Numeric X-Ray : Negative quantities (-80,995 min)    │
│  • Text X-Ray    : Hidden spaces, mixed case            │
│  • Duplicate X-Ray : 5,192 duplicate rows found         │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│          PYSPARK ETL — Distributed Cleaning             │
│  • dropna()       : Remove 135,080 null Customer IDs    │
│  • filter()       : Remove negative/zero quantities     │
│  • trim() + upper(): Standardise product descriptions   │
│  • dropDuplicates(): Remove 5,192 duplicate records     │
│  • to_timestamp() : Parse InvoiceDate string → timestamp│
│  • Output         : 392,693 clean validated transactions│
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│           DELTA TABLE — Unity Catalog                   │
│  • Saved as Delta format to workspace.default           │
│  • Persistent cloud storage — survives session end      │
│  • Queryable directly from Databricks SQL Editor        │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│           ADVANCED SQL — 8 Business Queries             │
│  • Macro KPIs, Country Revenue, Monthly Trend           │
│  • Top Products, Revenue % CTE, Running Total           │
│  • RFM Segmentation, MoM Growth LAG Function            │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│          POWER BI — 2-Page Enterprise Dashboard         │
│  • Page 1: SQL Intelligence — Pre-computed insights     │
│  • Page 2: Global Market View — Raw data exploration    │
└─────────────────────────────────────────────────────────┘
```

---

##  Dashboard Preview

| Page | Title | Key Visuals |
|---|---|---|
| Page 1 | SQL Intelligence | KPI cards, revenue trend, donut chart, product leaderboard, MoM growth, scatter plot |
| Page 2 | Global Market View | World map, country bar chart, gauges, scatter plot |

>  Screenshots in `/screenshots/` folder

---

##  Data Quality Results

| Problem Found | Raw Count | After Cleaning |
|---|---|---|
| Total raw rows | 541,910 | — |
| Null Customer IDs | 135,080 | 0 ✅ |
| Negative Quantity rows | 10,624 | 0 ✅ |
| Negative Price rows | 2 | 0 ✅ |
| Duplicate records | 5,192 | 0 ✅ |
| Hidden spaces in Description | 10,000+ | 0 ✅ |
| InvoiceDate as string | All rows | Timestamp ✅ |
| **Final clean rows** | — | **392,693** |

---

##  Advanced SQL Queries (8 Queries)

### Query 1 — Macro KPIs
```sql
-- Single row summary of business health
-- Total Revenue, Orders, Customers, Products, Avg Order Value
-- Technique: COUNT DISTINCT, SUM, AVG
```
**Finding:** £8.89M total revenue · 18,532 orders · 4,338 customers

---

### Query 2 — Revenue by Country
```sql
-- Country-level revenue breakdown with customer and order counts
-- Technique: GROUP BY, ORDER BY, LIMIT
```
**Finding:** United Kingdom dominates at 81.97% of total revenue

---

### Query 3 — Monthly Revenue Trend
```sql
-- Revenue aggregated by Year and Month
-- Technique: YEAR(), MONTH(), DATE_FORMAT(), GROUP BY
```
**Finding:** Revenue peaked in November 2011 before December drop-off

---

### Query 4 — Top 10 Products
```sql
-- Best performing products by revenue and units sold
-- Technique: GROUP BY Description, ORDER BY Revenue DESC
```
**Finding:** PAPER CRAFT LITTLE BIRDIE generated £168,469 from a single order

---

### Query 5 — Revenue Contribution % (CTE)
```sql
-- Country revenue as percentage of total using CTE
-- Technique: CTE + CROSS JOIN for grand total calculation
```
**Finding:** Top 3 countries (UK, Netherlands, EIRE) = 88.26% of revenue

---

### Query 6 — Cumulative Running Total (Window)
```sql
-- Month-by-month running total showing business growth
-- Technique: SUM() OVER (ORDER BY) Window Function
```
**Finding:** Business reached £8.32M cumulative revenue by end of 2011

---

### Query 7 — Month-over-Month Growth (LAG)
```sql
-- Calculate % revenue change between consecutive months
-- Technique: LAG() Window Function + percentage formula
```
**Finding:** June 2011 showed highest growth spike at +41.2% MoM

---

### Query 8 — RFM Customer Segmentation (CTE + CASE WHEN)
```sql
-- Classify every customer into value segments
-- Technique: CTE + DATEDIFF + RANK() + CASE WHEN
-- Segments: Active / Warm / At Risk / Churned
```
**Finding:** At-Risk customers represent £1.4M+ in recoverable revenue

---

##  Data Model (Power BI)

```
query5_rfm (1) ──────────────────── (*) cleaned_online_retail
Customer_ID                              Customer ID
(one row per customer)                   (many transactions)

Standalone tables (pre-aggregated — no joins needed):
├── query1_macro_kpis
├── query2_country_revenue
├── query3_monthly_trend
├── query4_top_products
├── query6_running_total
└── query7_mom_growth
```

**Key Design Decision:** Pre-aggregated SQL query outputs are loaded as standalone tables — no joins required in Power BI. This keeps the BI layer fast and lightweight, with all heavy computation handled upstream in Databricks SQL.

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Python 3.x | Data pipeline scripting |
| PySpark | Distributed data processing at scale |
| Databricks Community Edition | Cloud Spark cluster |
| Azure Data Lake Storage | Cloud file storage (ADLS Volume) |
| Databricks SQL | Advanced analytics queries |
| Delta Lake | Persistent cloud table storage |
| Power BI Desktop | Interactive enterprise dashboard |

---

## Key Engineering Decisions

**Why PySpark instead of Pandas?**
541,910 rows is at the boundary where Pandas struggles on low-memory machines. PySpark reads natively in parallel across the cluster — same code scales to 100M rows without modification.

**Why native spark.read.csv() instead of pandas → convert?**
Loading via Pandas first then converting defeats distributed processing. Native Spark read keeps data in cluster memory throughout the entire pipeline.

**Why Delta table format?**
Delta provides ACID transactions, time travel, and persistent storage in Unity Catalog. Parquet files disappear when the cluster terminates — Delta tables survive indefinitely.

**Why pre-aggregate in SQL instead of Power BI DAX?**
Pushing computation upstream to Databricks SQL means Power BI only renders results — not compute them. Dashboards load instantly regardless of data size.

**Why regexp_replace before to_timestamp()?**
The InvoiceDate column contained mixed formats (`01-12-2010` and `1/12/2010`). Replacing hyphens with slashes first unified the format before parsing — preventing CANNOT_PARSE_TIMESTAMP errors.

---

##  Results Summary

```
541,910 raw transactions ingested into Spark
         ↓
392,693 clean validated records (27.6% data quality issues fixed)
         ↓
8 advanced SQL queries executed in Databricks
         ↓
2-page enterprise Power BI dashboard
         ↓
£8.89M total revenue tracked
£1.4M+ at-risk revenue identified via RFM
81.97% UK revenue concentration flagged
```

---

##  Author

**Abhishek S**
MCA — Sapthagiri NPS University, Bengaluru
📧 abhisheks86609@gmail.com

---

*Built as part of a data engineering portfolio demonstrating cloud-scale ETL, advanced SQL analytics, and enterprise business intelligence.*
