#  Data Engineering Project: Accurate Data Warehouse (DWH)

![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![dbt](https://img.shields.io/badge/dbt-Transformation-orange)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-DataWarehouse-blue)
![Power BI](https://img.shields.io/badge/PowerBI-Visualization-yellow)

## 🚀 Overview

This project builds a modern **Data Warehouse (DWH)** from **Accurate**, an accounting and transaction recording application, using a robust **ELT pipeline architecture**.

The goal is to transform raw transactional data into **analytics-ready datasets** to support better decision-making and reporting.

---

## 1. Background & Problem Statement

### What is Accurate?
Accurate is an accounting and transaction recording system used to manage business operations such as sales, inventory, and financial transactions.

<img width="1000" height="552" alt="image" src="https://github.com/user-attachments/assets/d85c39fa-0342-4d6d-8d0e-fa82064276c6" />


### Problem
While Accurate is powerful for operational use, it has limitations:
- ❌ Limited flexibility for advanced analytics
- ❌ Difficult to perform complex queries
- ❌ Reporting is not customizable for deeper insights
- ❌ Not optimized for analytical workloads

### Why Data Warehouse?
To overcome these limitations, a **Data Warehouse** is needed:
- Centralized analytical data storage
- Optimized for querying and reporting
- Enables historical and trend analysis
- Supports BI tools like Power BI

### Solution
This project implements a **modern ELT pipeline**:
- Extract data from Accurate API
- Load raw data into PostgreSQL
- Transform using dbt
- Serve analytics-ready data for visualization in Power BI

---

## 2. Specifications, Architecture & Pipeline

### 🔧 Tech Stack

| Tool            | Role |
|-----------------|------|
| **Apache Airflow** | Workflow orchestration & scheduling |
| **PostgreSQL**     | Data Warehouse storage |
| **dbt**            | Data transformation & modeling |
| **Docker**         | Containerization & environment setup |
| **Power BI**       | Data visualization & reporting |

---

### Architecture Diagram

<img width="1616" height="640" alt="Accurate ELT Architecture" src="https://github.com/user-attachments/assets/54c813e6-9f63-4083-bb78-13d32ba7e9ae" />


---

### 🔄 Pipeline Flow (ELT)

1. **Extract**
   - Data is pulled from Accurate API

2. **Load (EL)**
   - Raw data is stored in PostgreSQL (`raw` schema)

3. **Transform (T)**
   - dbt transforms data into structured layers:
     - Staging
     - Data Marts

4. **Serve**
   - Power BI connects to Data Marts for analytics

---

### 🧱 Data Layers

#### 🥉 Raw Layer (Bronze)
- Source-aligned data from Accurate API
- Minimal transformation
- Stored in `raw` schema

#### 🥈 Staging Layer (Silver)
- Cleaned and standardized data
- Renamed columns, type casting, filtering
- Prepared for modeling

#### 🥇 Data Mart Layer (Gold)
- Business-ready datasets
- Optimized for analytics
- Implements **Star Schema**

---

### Data Modeling (Star Schema)

<img width="1609" height="1014" alt="DW Star Accurate Sales Detail Grain" src="https://github.com/user-attachments/assets/9e040157-a89b-419e-8230-7f9bf78b0c24" />


#### Fact Table:
- `fact_sales`

#### Dimension Tables:
- `dim_item`
- `dim_customer`
- `dim_date`
- `dim_branch`
- `dim_warehouse`

This structure enables:
- Fast querying
- Easy aggregation
- Clear business logic

---

### Data Visualization

-  Customer Deep Down
<img width="1124" height="634" alt="Dasborad" src="https://github.com/user-attachments/assets/cb554849-497a-4b9d-b104-d27bf3bde94a" />


## 3. How to Run (Step-by-Step)

### 1. Clone Repository
```bash
git clone <your-repo-url>
cd <your-project-folder>
```
### 2. Setup OAuth for Accurate
```bash
python ingestion/oauth/oauth_bootstrap.py
```
fill in `.env`
- `ACCESS_TOKEN`
- `REFRESH_TOKEN`

### 3. Start Docker
```bash
docker-compose up -d
```
This will:
- Initilaze PostgreSQL
- Create `raw` Schema
- Create `Variables` & `Connection` in Airflow

### 4. Configure Airflow Variables
1. Open Airflow UI
2. Go to **Admin** -> **Variables**
3. Replace:
    - `refresh_token`
    - `access_token`

### 5. Backfill Historical Data
```bash
python ingestion/backfill/backfill_dwh.py
```

### 6. Run Pipeline
- Trigger DAG Manually in Airflow **OR**
- Let the scheduler run automatically

### 7. Connect to Power BI
- Connect to PostgreSQL database
- Use data marts schema
- Build dashboards and reports


