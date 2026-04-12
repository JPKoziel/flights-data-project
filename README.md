# ✈️ US Flight Delays - Data Engineering Project

## Problem Statement

This project analyzes US domestic flight delays and cancellations (2019-2023)
to identify which airlines and routes have the worst on-time performance.
The goal is to build a reliable data pipeline using the Medallion Architecture
(Bronze → Silver → Gold) that transforms raw flight data into actionable analytics.

**Key analytical question:**
*Which airlines and routes have the highest average arrival delays
and cancellation rates?*

---

## Architecture

The project follows the **Medallion Architecture** orchestrated by Apache Airflow
and processed by PySpark: