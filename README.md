# Modern Data Warehouse with Medallion Architecture

## 📌 Project Overview
This project demonstrates the design and implementation of a **Modern Data Warehouse** using the **Medallion Architecture (Bronze, Silver, Gold layers)**. It showcases end-to-end data engineering capabilities—from raw data ingestion to analytics-ready datasets—enabling scalable, reliable, and high-performance analytical workloads.

The solution emphasizes data quality, performance optimization, and analytics enablement, providing a strong foundation for business intelligence and data-driven decision-making.

---

## 🏗️ Architecture

### Medallion Architecture
The data platform is structured into three logical layers to ensure progressive data refinement and governance.

#### 🟤 Bronze Layer (Raw Data)
- Ingests raw data from source systems with minimal transformation  
- Preserves data fidelity for traceability and auditing  
- Acts as the system of record  

#### ⚪ Silver Layer (Refined Data)
- Applies data cleansing, validation, and standardization  
- Enforces business rules and schema consistency  
- Produces reliable, high-quality datasets  

#### 🟡 Gold Layer (Analytics-Ready Data)
- Curated datasets optimized for reporting and analytics  
- Implements business logic and aggregations  
- Serves dashboards, KPIs, and ad-hoc analytical queries  

<img width="900" alt="data_architecture" src="https://github.com/user-attachments/assets/ae41c534-bac7-4d53-9b28-3e61f5954781" />

---

## 🔄 ETL Pipelines
- Designed and implemented robust ETL pipelines to extract data from multiple source systems  
- Applied transformations to ensure data accuracy, consistency, and usability  
- Optimized data loading for performance and scalability  
- Incorporated error handling and data validation checks to improve reliability  

---

## 📐 Data Modeling
- Developed dimensional data models using industry best practices  
- Designed fact and dimension tables (star/snowflake schemas)  
- Optimized for analytical query performance and BI consumption  
- Enabled intuitive exploration of business metrics  
