# Customer RFM Segmentation Pipeline

## Overview

This project computes daily RFM (Recency, Frequency, Monetary) metrics for customers based on orders data and stores the results in the table ecom.customer_rfm_daily. It processes transactional data, computes RFM scores, and segments customers to enable targeted marketing startegies, personalization, and business insights.


## Table of Contents

- [What This Pipeline Does](#what-this-pipeline-does)
- [Data Access](#data-access)
- [Prerequisites](#prerequisites)
- [How to run the script](#running-the-project)
- [Scheduling the task](#scheduling-automation)
- [Testing](#testing)


## What this pipeline does:

1. Extracts data from ecom.orders table (excluding cancelled orders)
2. Calculates RFM metric for each customer:
   Recency: days since last completed order (latest order date)
   Frequency: number of orders in the last 90 days
   Monetary: total spend in the last 90 days (sum of order total)
3. Assigns scores (1-5) for RFM.
4. Segments customers based on RFM scores.
5. Runs incrementally (daily) using upsert logic (no duplicates)

## Data Access:

1. A separate .env file is created to store the database credentials in the project root directory

 ```env
DB_HOST=localhost
DB_NAME=postgres
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432
```
2. Import packages to load the environment variables:

     from dotenv import load_dotenv
     load_dotenv()

4. The python script reads the credentials using:

 ```python
import os

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
```

## Prerequisites:

Python 3.x
PostgreSQL
Jupyter Notebook

## How to run the script :

Install dependencies
```bash
pip install -r requirements.txt
```
**Option 1: Using Jupyter Notebook:**

```bash
 jupyter notebook rfm_pipeline.ipynb 
 ```
 Run all the cells.

 **Option 2: Using Python Script (Recommended for production)**:

 1. Convert notebook to sript: rfm_pipeline.py
 2. Run:

```bash
 python rfm_pipeline.py 
 ```

## Scheduling the task:

Use cron jobs to run daily.

1. Open crontab:

```bash
 crontab -e
 ```
2. Run daily at 11 am
   
```bash
 0 11 * * * /usr/bin/python3 /path/to/rfm_pipeline.py
  ```

## Testing:

1. Validate RFM calculations with sample data.
2. Check edge cases:
- Customers with no recent orders
3. Ensure:
- No duplicate rows
- Correct upsert behavior


   


