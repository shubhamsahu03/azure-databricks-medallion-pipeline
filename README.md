# 📘 Industry-Grade Report: Azure Databricks Data Engineering Pipeline (2025)

## 🚀 Project Overview

This project demonstrates a **complete, production-ready data engineering pipeline** built with **Azure Databricks**, applying the **medallion architecture** (bronze → silver → gold). It integrates modern features such as **PySpark with OOP**, **Delta Live Tables**, and **Unity Catalog**, while ensuring data governance, scalability, and analytical readiness.

---

## 🧱 Architecture Summary

The pipeline is built using the **Medallion Architecture**, a layered approach widely adopted in modern lakehouse implementations.

- **Bronze Layer**: Incremental ingestion of raw data using **Spark Structured Streaming** and **Autoloader**
- **Silver Layer**: Cleansed and enriched data using **modular PySpark transformations**
- **Gold Layer**: Analytics-ready **star schema** warehouse using **Delta Live Tables** with **SCD Type 1 & 2**

📌 **Architecture Diagram**:  
![Architecture Diagram](./docs/architecture.png)

---

## 🔧 Technology Stack

| Component             | Tool / Service                          |
|-----------------------|------------------------------------------|
| Cloud Platform        | Azure                                    |
| Data Lake             | Azure Data Lake Storage Gen2 (ADLS)      |
| Processing Framework  | Apache Spark (Azure Databricks)          |
| Language              | Python (PySpark)                         |
| Storage Format        | Parquet, Delta Lake                      |
| ETL Orchestration     | Databricks Jobs                          |
| Data Governance       | Unity Catalog                            |
| Visualization         | Power BI, Databricks SQL Warehouse       |

---

## 🔄 Data Pipeline Breakdown

### 🔹 Bronze Layer – Ingestion
- **Autoloader** ingests raw files from ADLS incrementally
- **Structured Streaming** ensures exactly-once semantics
- **Checkpointing** and **schema evolution** manage state and drift
- Data is stored in **Parquet format**

### 🔸 Silver Layer – Transformation
- Modular transformation logic using **Python OOP in PySpark**
- Includes:
  - Surrogate key generation
  - Date normalization
  - Data deduplication
- Functions/classes are reusable and testable
- Data is stored in **Delta format**

### ⭐ Gold Layer – Star Schema
- Star schema includes:
  - **Fact tables**: `fact_sales`, `fact_transactions`
  - **Dimension tables**: `dim_customer`, `dim_product`, `dim_date`, `dim_branch`
- Built using **Delta Live Tables** with declarative transformations
- Implements:
  - **SCD Type 1** (overwrite on update)
  - **SCD Type 2** (preserve history)
- Data quality checks via `@dlt.expect`

---

## 🐍 PySpark OOP Design

To enhance **code maintainability and reusability**, the project applies key **object-oriented programming (OOP)** principles within PySpark scripts:

| OOP Concept   | Application                                |
|---------------|---------------------------------------------|
| Encapsulation | Wrapped transformation logic into classes  |
| Inheritance   | Shared logic extracted to base classes      |
| Modularity    | Separated logic into reusable components    |
| Reusability   | Used the same class for multiple datasets   |
| Parameterization | Configuration passed into constructors  |

✅ Result: Cleaner notebooks, reduced duplication, and testable logic.

---

## 🛡️ Unity Catalog & Governance

- Managed catalogs, schemas, and tables with **Unity Catalog**
- Defined **external locations** for data lake access
- Applied **role-based access control (RBAC)** at table/function level
- Registered reusable **Python and SQL functions** for team-wide use

---

## ⛓️ Workflow Orchestration

- Used **Databricks Jobs** to orchestrate multi-layer pipelines
- Parameterized notebooks using `dbutils.widgets`
- Enabled:
  - Retry policies
  - Cluster reuse
  - Sequential and parallel execution

---

## 📊 Analytics Integration

- Gold layer exposed via **Databricks SQL Warehouse**
- Connected to **Power BI** using Partner Connect
- Dashboards include:
  - Time-series sales trends
  - Product-wise performance
  - Region-wise revenue reports

---

## 🚧 Challenges & Mitigation Strategies

| Challenge                        | Solution |
|----------------------------------|----------|
| Schema evolution in streaming   | Autoloader with schema inference + evolution |
| Repetitive transformation logic | Reusable Python OOP modules |
| Complex SCD logic               | Simplified via Delta Live Tables merge support |
| Governance setup across teams  | Unity Catalog with managed locations and RBAC |
| Cost optimization               | Used job clusters and auto-termination |

---

## 💼 Business Impact

- Enabled **real-time, reliable ingestion** using Spark Autoloader
- Created a **centralized, governed, and reusable** transformation layer
- Built an **analytics-ready star schema** with versioned history
- Ensured **clean architecture** through OOP and cataloged functions
- Allowed **scalable insights** through Power BI integration

---

## 🔮 Future Enhancements

- Add **unit tests** using `pytest` and `chispa` for PySpark classes
- Integrate **data observability** tools like Great Expectations
- Expand to handle **Change Data Capture (CDC)** with Event Hubs/Kafka
- Automate deployments via **Terraform** and REST APIs
- Build **CI/CD workflows** for notebook lifecycle management

---

## 👨‍💻 Author

**Your Name**  
📧 Email: your.email@example.com  
🔗 [LinkedIn](https://linkedin.com/in/your-profile)

---

## 📄 License

This project is licensed under the [MIT License](./LICENSE).
