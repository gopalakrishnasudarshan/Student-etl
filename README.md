# Student Analytics ETL & Dashboard Project

This project is an end-to-end data engineering and analytics pipeline built using Java, Spring Boot, PostgreSQL, Elasticsearch, and Kibana.

The goal of the project is to ingest raw student data, transform it into meaningful analytics, and visualize the results in an interactive dashboard.

The final output of the project is a Kibana dashboard showing insights about student performance.

---

## Project Overview

The project follows a clear pipeline:

1. ETL (Extract, Transform, Load)
    - Read raw student data from a CSV file
    - Validate and clean the data
    - Store structured data in PostgreSQL

2. Analytics Layer
    - Create analytics-focused SQL views on top of the raw data
    - Each view answers a specific analytical question

3. Analytics Export
    - Read analytics views from PostgreSQL
    - Export each view into its own Elasticsearch index

4. Visualization
    - Build a Kibana dashboard using Elasticsearch indices
    - Visualize all analytics in one place

---

## Technology Stack

Backend / ETL
- Java
- Spring Boot
- Spring Data JPA
- Flyway

Database
- PostgreSQL

Analytics & Visualization
- Elasticsearch
- Kibana

Infrastructure
- Docker
- Docker Compose

---


## Dataset

- Source: UCI Machine Learning Repository
- Dataset: Student Performance Dataset
- Context: Student performance in secondary education (high school)
- Subjects available: Mathematics (MAT), Portuguese (POR)
- Subject used in this project: **MAT only**

--- 

### Data contains
- Student demographic information
- Family and social background
- Study habits and lifestyle attributes
- Academic scores:
    - G1 – First period grade
    - G2 – Second period grade
    - G3 – Final grade

---

## Kibana Dashboard

A single Kibana dashboard has been created that brings together all analytics views.

- All charts are based on Elasticsearch indices
- Axes, aggregations, and labels are clearly defined
- Dashboard is saved and reusable

Screenshots of the complete dashboard are available in: docs/screenshots/

---

## How to Run the Project

### 1. Start Elasticsearch and Kibana

From the `student-etl-infra` directory:
docker compose up -d
Verify:

Elasticsearch: http://localhost:9200
Kibana: http://localhost:5601

### 2. Run ETL only (CSV → PostgreSQL)

From the project root:

./gradlew bootRun --args="--etl.enabled=true"

### 3. Run Analytics Export only (PostgreSQL → Elasticsearch)
   ./gradlew bootRun --args="--export.enabled=true"

### 4. Run Full Pipeline (ETL + Export)
   ./gradlew bootRun --args="--etl.enabled=true --export.enabled=true"

This command:  Loads CSV data into PostgreSQL,  Generates analytics views, Exports analytics into Elasticsearch , Updates the Kibana dashboard data

---
