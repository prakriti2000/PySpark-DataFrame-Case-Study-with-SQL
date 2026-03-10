# PySpark DataFrame Case Study with SQL

## Project Overview

This project demonstrates how to analyze **e-commerce transaction data** using **PySpark DataFrame operations and SQL queries**.

The goal is to understand customer purchase behavior, total spending, and product preferences using common PySpark transformations.

---

## Dataset Description

The dataset contains information about e-commerce transactions.

| Column | Description |
|------|------|
| transaction_id | Unique ID for each transaction |
| customer_id | Unique ID for each customer |
| product_id | Unique ID for each product |
| product_name | Name of the product |
| category | Product category |
| price | Price of the product |
| quantity | Quantity purchased |

---

## Sample Data

| transaction_id | customer_id | product_id | product_name | category | price | quantity |
|---|---|---|---|---|---|---|
| 1 | 101 | 5001 | Laptop | Electronics | 1000.0 | 1 |
| 2 | 102 | 5002 | Headphones | Electronics | 50.0 | 2 |
| 3 | 101 | 5003 | Book | Books | 20.0 | 3 |
| 4 | 103 | 5004 | Laptop | Electronics | 1000.0 | 1 |
| 5 | 102 | 5005 | Chair | Furniture | 150.0 | 1 |

---

## PySpark Operations Performed

This project demonstrates several important PySpark transformations and actions.

### 1. Loading Data
Create a Spark session and load the sample dataset into a DataFrame.

### 2. Filtering Data
Filter transactions where the **quantity purchased is greater than 1**.

### 3. Handling Null Values
Replace missing values in the **price column** with the **average price**.

### 4. Removing Duplicates
Remove duplicate transactions based on **customer_id and product_id**.

### 5. Selecting Specific Columns
Extract only useful columns such as **customer_id, product_name, and price**.

### 6. Aggregating Data
Calculate **total spending per customer** using `groupBy` and aggregation.

### 7. Joining DataFrames
Join the transaction DataFrame with a **customer details DataFrame**.

### 8. Union of DataFrames
Combine two DataFrames with the same schema.

### 9. Running SQL Queries
Create a **temporary view** and run SQL queries using `spark.sql()`.

Example query:

SELECT customer_id, SUM(price * quantity) AS total_spent
FROM transactions
GROUP BY customer_id


---

## Technologies Used

- Python
- PySpark
- Apache Spark
- Spark SQL

---

## How to Run the Project

Run the PySpark script using:


spark-submit DataFramewithsql.py


---

## Key Concepts Demonstrated

- DataFrame transformations
- Filtering data
- Handling null values
- Removing duplicates
- Aggregation and grouping
- DataFrame joins
- Union operations
- Running SQL queries on Spark DataFrames

---
