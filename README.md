# Databricks_Lakehouse_Pipeline_Project (Medallion Architecture)


This project demonstrates an end-to-end Medallion pipeline developed on Databricks Lakehouse Platform.
The pipeline extracts raw operational records from separate CRM and ERP source systems, cleans and normalizes them through progressive Delta layers, and structures the final data into an analytics-ready Star Schema for downstream business intelligence..

## 🏗️ Pipeline Design & Orchestration

Pipeline execution and task dependencies are managed using **Databricks Workflows** deployed on serverless compute.

![Databricks Workflow DAG](images/pipeline_dag.png)

### Execution Mechanics:
* **Bronze (Configuration Ingestion):** The pipeline kicks off with a single metadata-driven notebook. It processes an ingestion array to load all six source CSV files into their respective Bronze Delta tables in one task. This eliminates notebook sprawl and keeps the ingestion baseline centralized.
* **Silver (Concurrent Transformations):** Once the raw data lands, the workflow fans out to trigger six dedicated cleaning notebooks simultaneously. Running these tasks in parallel avoids a single-file line bottleneck and significantly drops pipeline runtime.
* **Gold (Analytical Convergence):** The separate data streams converge to build the analytical Star Schema. The pipeline processes both dimension tables concurrently. The final step triggers the sales fact notebook only after the lookup dimensions are fully loaded to protect referential integrity.

---

## 🛠️ Technology Stack
* **Core Platform:** Databricks Lakehouse
* **Storage Layer:** Delta Lake (ACID Transactions, Schema Enforcement)
* **Compute Infrastructure:** Serverless Apache Spark Pools
* **Development Languages:** PySpark, Spark SQL, Python
* **Orchestration & Scheduling:** Databricks Jobs (DAG Architecture)

---

## 💾 Data Layer Specifications

### 1. Bronze Layer (Raw Ingest)
Acts as the historical landing zone for the system. Data is pulled directly from cloud storage volumes mapped to two distinct operational source domains:
* **CRM Source:** Extracts raw files containing customer profiles, product specifications, and transactional sales details.
* **ERP Source:** Extracts backend operational files managing customer master data, localized geographic regions, and global product categories.


### 2. Silver Layer (Cleaned & Standardized)
The Silver layer processes the raw Bronze tables into a standardized, query-ready format. Each data stream passes through a consistent data-cleaning pipeline to ensure downstream reliability:
* **Data Type Enforcement:** Converts inconsistent raw source columns into strict, structured data types (such as formatting string timestamps into proper date types).
* **Data Quality Normalization:** Resolves data layout issues, replaces missing or invalid values with standard defaults, and fixes inconsistent text casing.
* **Record Deduplication:** Identifies and removes duplicate rows using unique business keys to maintain strict data integrity across tables.


### 3. Gold Layer (Star Schema Analytics)
The Gold layer transforms the clean Silver data into a Star Schema model designed for business reporting. It structures the data into two dimension tables and one central fact table:
* **`dim_customers`**: Combines customer profiles from both the CRM and ERP systems into a single master customer table.
* **`dim_products`**: Matches active operational products with their correct category descriptions while filtering out old, inactive items.
* **`fact_sales`**: Stores core business metrics like sales amounts, quantities, and prices, connecting them directly to the customer and product tables using lookup keys.

---

## 🚀 Engineering Design Decisions & Optimizations

### Network Shuffle Reduction
Fact table compilation typically incurs high cloud network costs due to data shuffling during multi-table joins. Because the lookup dimension tables (`dim_products` and `dim_customers`) are small relative to the high-volume transactional sales records, the execution plan uses **Broadcast Hints**. This instructs Spark to copy the dimensions directly to all active worker nodes, executing a localized map-side join and eliminating network-heavy shuffles.

### Pipeline Idempotency
To guarantee data reliability, every processing tier applies data overwrites combined with schema overwrite options. If a scheduled pipeline run fails mid-way due to external cloud infrastructure or network timeouts, the workflow can be safely re-triggered from the beginning without risking duplicate records or schema mutations.

### Granular Fault Recovery
Each task within the Databricks Workflow DAG is configured with isolated retry thresholds. If an individual notebook experiences a transient storage timeout, Databricks automatically retries only that specific node, keeping the remaining unaffected parallel execution tasks running uninterrupted.

---

## 📂 Repository Structure

```text
├── code/
│   ├── Bronze_Layer/
│   │   └── Ingest_Raw_Sources.py      # Configuration-driven ingestion loop
│   ├── Silver_Layer/
│   │   ├── Silver_crm_cust_info.py
│   │   ├── Silver_crm_prd_info.py
│   │   ├── Silver_crm_sales_details.py
│   │   ├── Silver_erp_cust_az12.py
│   │   ├── Silver_erp_loc_a101.py
│   │   └── Silver_erp_px_cat_g1v2.py
│   └── Gold_Layer/
│       ├── dim_customers.py           # Customer dimension pipeline
│       ├── dim_products.py            # Product dimension pipeline
│       └── fact_sales.py              # Optimized sales fact pipeline
├── images/
│   └── pipeline_dag.png               # Databricks workflow graph screenshot
└── README.md
