# Vendor-Performance-and-Inventory-Optimization-Analytics
Project demonstrates: 
- End-to-end data pipeline development
- SQL and Python for data analysis
- Actionable business insights 
## Table of Contents
- <a href="#overview">Overview</a>
- <a href="#business-problem">Business Problem</a>
- <a href="#dataset">Dataset & Tools</a>
- <a href="#eda">EDA & Data Quality</a>
- <a href="#keyfindings">Key Findings</a>
- <a href="#recommendations">Recommendations</a>

---

<h2><a class="anchor" id="overview"></a>Overview</h2>
This project implements an automated analytical pipeline to evaluate the inventory efficiency of <b>10,000+ brands</b>. By integrating data from disparate sources (VendorInvoice,PurchasePrices,Purchases,Sales, BeginInventory and EndInventory), the analysis quantifies how effectively the business converts inventory into cash. 

---

<h2><a class="anchor" id="business-problem"></a>Business Problem</h2>
Effective inventory management is considered the backbone of retail profitability. Challenges were faced by the business regarding:
1.  <b>"Cash Traps":</b> Capital was tied up in slow-moving inventory (High DIO).
2.  <b>"Profit Leaks":</b> High-volume items were sold at margins that did not justify the holding costs (Low GMROI).
3.  <b>Granularity Mismatch:</b> Data was aggregated incorrectly in previous reports, which led to inaccurate COGS calculations.

The goal was to build a data-driven framework so that the question could be answered: <i>"Which vendors are funding growth, and which are draining cash?"</i>

---

<h2><a class="anchor" id="dataset"></a>Dataset & Tools</h2>

**Tech Stack:** - **SQL (PostgreSQL):** Advanced CTEs, Window Functions, and Data Cleaning (`COALESCE`, `NULLIF`) were utilized.
- **Python:** Pandas was used for statistical analysis, and Matplotlib was used for visualization.

**Data Sources:**
- `Purchases`: Historical procurement data (Cost, Quantity, Vendor Terms) was accessed.
- `Sales`: Transactional sales data (Revenue, Quantity) was analyzed.
- `Begin_Inventory` & `End_Inventory`: Periodic snapshots of stock levels were compared.
- `Vendor_Invoice`: Payment dates were used to calculate DPO (Days Payable Outstanding).

---

<h2><a class="anchor" id="eda"></a>EDA & Data Quality</h2>

**1. The "Granularity Mismatch" Solved:**
A logic error was revealed during initial exploration where item-level costs were applied to brand-level sales. A **Weighted Average Cost** model was implemented in SQL, allowing the true cost of goods sold to be dynamically calculated based on purchase history.

**2. Outlier Detection (IQR Method):**
Extreme skewness was revealed by the statistical analysis of **DIO (Days Inventory Outstanding)**.
- **Lower Bound:** -156.3 days (Data errors/Negative stock)
- **Upper Bound:** 373.9 days
- **Result:** **1,139 brands (10.9%)** were detected as outliers. These were segmented into a separate "Dead Stock" report to prevent the core business metrics from being skewed.

---

<h2><a class="anchor" id="keyfindings"></a>Key Findings</h2>

**1. The Efficiency Gap:**
A massive performance disparity was observed between the core business and the "Long Tail" of inefficient brands:

| Metric | Clean Portfolio | Outlier Group (Bottom 10%) |
| :--- | :--- | :--- |
| **Inventory Turnover** | **28.96x** (Fast) | **3.69x** (Stagnant) |
| **Avg GMROI** | **10.13** | **1.88** |
| **DIO (Days on Shelf)** | **91.5 Days** | **497.2 Days** |

**2. Cash Conversion Cycle (CCC):**
- A **Negative CCC** was achieved by top performing brands, meaning the vendor's payment terms (DPO) were longer than the time taken to sell the goods (DIO). Effectively, business operations are funded by these vendors.
- A CCC exceeding **365 days** was observed for the bottom 10% of brands, representing significant trapped capital.

---

<h2><a class="anchor" id="recommendations"></a>Recommendations</h2>

Based on the quantitative analysis, the following strategic actions are recommended:

1.  **Immediate Liquidation:** The **1,139 outlier brands** (Avg DIO > 497 days) should be marked for clearance. It is recommended that the released capital be reinvested in high-GMROI items.
2.  **Negotiate Terms:** For brands where **High Turnover but Positive CCC** is observed, longer payment terms (DPO) should be negotiated with vendors to shift the CCC to negative.
3.  **Stop-Buy Orders:** An automated freeze on Purchase Orders should be implemented for any brand with a **GMROI < 1.0**, as money is effectively lost on these items when holding costs are factored in.


