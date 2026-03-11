# RFM-pipeline
Daily RFM pipeline

**Overview**

This project computes daily RFM (Recency, Frequency, Monetary) metrics for customers based on orders data and stores the results in the table ecom.customer_rfm_daily.

**Description of the pipeline:**

1. Extracts data from ecom.orders table (excluding cancelled orders)
2. Calculate RFM metric:
   Recency: days since last completed order (latest order date)
   Frequency: number of orders in the last 90 days
   Monetary: total spend in the last 90 days (sum of order total) - all three ranging form 1-5
3. RFM scores are created and then segmented accordingly to identify the different set of customers and design targeted marketing strategies, personalized offers etc.
4. Upserts results into the destination table so the job can run daily without duplicates.

**Creation of environment variables**:

1. A separate .env file is created to store the database credentials in the project root directory

  DB_HOST=localhost
  DB_NAME=postgres
  DB_USER=your_username
  DB_PASSWORD=your_password
  DB_PORT=5432

2. Import packages to load the env variables:

     from dotenv import load_dotenv
     load_dotenv()
     import os

4. The python script reads the credentials using:

  DB_HOST = os.getenv("DB_HOST")
  DB_NAME = os.getenv("DB_NAME")
  DB_USER = os.getenv("DB_USER")
  DB_PASSWORD = os.getenv("DB_PASSWORD")
  DB_PORT = os.getenv("DB_PORT", "5432")

**How to run the script** :

1. Install the required packages
   
    pip install -r requirements.txt

3. Run the pipeline using "jupyter notebook rfm_pipeline.ipynb" and run all the cells.

**To schedule the task using cron:**

1. Need to create a python script for this, let's say: rfm_pipeline.py
2. Open crontab using: crontab -e
3. To run the script daily at 11am suppose:
   
   0 11 * * * /usr/bin/python3 /path/to/rfm_pipeline.py

   


