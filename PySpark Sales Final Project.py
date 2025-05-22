# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Sales Project
# MAGIC ### 1. Total number of customers
# MAGIC ### 2. Total number of orders
# MAGIC ### 3. Total number of sales as of now
# MAGIC ### 4. Total profit
# MAGIC ### 5. Top sales by country
# MAGIC ### 6. Most profitable region and country
# MAGIC ### 7. Top sales category products
# MAGIC ### 8. Top 10 sales sub-category products
# MAGIC ### 9. Most ordered quantity product
# MAGIC ### 10. Top customer based on sales and city

# COMMAND ----------

# 1. Total number of customers
df.selectExpr("count(distinct Customer_ID) as Total_Customers").show()

# 2. Total number of orders
df.selectExpr("count(distinct Order_ID) as Total_Orders").show()

# 3. Total number of sales as of now
df.selectExpr("sum(Sales) as Total_Sales").show()

# 4. Total profit
df.selectExpr("sum(Profit) as Total_Profit").show()

# 5. Top sales by country
df.groupBy("Country").agg({"Sales": "sum"}).orderBy("sum(Sales)", ascending=False).show()

# 6. Most profitable region and country
df.groupBy("Region", "Country").agg({"Profit": "sum"}).orderBy("sum(Profit)", ascending=False).show()

# 7. Top sales category products
df.groupBy("Category").agg({"Sales": "sum"}).orderBy("sum(Sales)", ascending=False).show()

# 8. Top 10 sales sub-category products
df.groupBy("Sub_Category").agg({"Sales": "sum"}).orderBy("sum(Sales)", ascending=False).limit(10).show()

# 9. Most ordered quantity product
df.groupBy("Product_Name").agg({"Quantity": "sum"}).orderBy("sum(Quantity)", ascending=False).limit(1).show()

# 10. Top customer based on sales and city
df.groupBy("Customer_ID", "City").agg({"Sales": "sum"}).orderBy("sum(Sales)", ascending=False).limit(1).show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

 /FileStore/tables/superstore.csv

# COMMAND ----------

dbutils.widgets.dropdown("time_period",'Weekly',['Weekly','Monthly'])

# COMMAND ----------

from datetime import date,timedelta,datetime
from pyspark.sql.functions import * 

time_period= dbutils.widgets.get("time_period")
print(time_period)
today = date.today()

if time_period=='Weekly':
    start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
    end_date=start_date+timedelta(days=6)

else:
    first=today.replace(day=1)
    end_date=first-timedelta(days=1)
    start_date=first-timedelta(days=end_date.day)

print(start_date,end_date)

# COMMAND ----------

display(spark.sql(f""" select count(distinct customer_id) from sample where order_date between '{start_date}' and '{end_date}' """))

# COMMAND ----------

    display(spark.sql(f"""
    SELECT COUNT(DISTINCT customer_id)
    FROM sample
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
"""))


# COMMAND ----------

display(spark.sql(f""" select count(distinct customer_id) from sample 
                  where order_date between '{start_date}' and '{end_date}' """))

# COMMAND ----------



# COMMAND ----------

df = spark.read.csv("/FileStore/tables/superstore.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df.createOrReplaceTempView('sample')

# COMMAND ----------

# DBTITLE 1,total number customer
# MAGIC %sql
# MAGIC select count(distinct customer_id) from sample;

# COMMAND ----------

spark.sql(f"""select count(distinct (customer_id)) from sample where order_date between '{start_date}' and 
          '{end_date}'""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct order_id) from sample;

# COMMAND ----------

spark.sql(f"""select count(distinct (order_id)) from sample where order_date between '{start_date}' and 
          '{end_date}'""").display()

# COMMAND ----------

# DBTITLE 1,total sum and total profit
# MAGIC %sql
# MAGIC select sum(sales),sum(profit) from sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(sales),country from sample group by country;

# COMMAND ----------

# DBTITLE 1,total profitable region/country
# MAGIC  %sql
# MAGIC  select sum(sales) as total_sales,country,region from sample
# MAGIC  group by country, region
# MAGIC  order by total_sales desc

# COMMAND ----------

# DBTITLE 1,top sales category  product
# MAGIC %sql
# MAGIC  select sum(sales) as total_sales , category from sample group by category order by total_sales;

# COMMAND ----------

# DBTITLE 1,top 10 most selling sub_category  product
# MAGIC %sql
# MAGIC  select sum(sales) as total_sales , sub_category from sample group by sub_category order by  total_sales desc limit 10;

# COMMAND ----------

# DBTITLE 1,most order quantity product
# MAGIC %sql
# MAGIC
# MAGIC select sum(quantity) as total_quantity, product_name from sample 
# MAGIC group by product_name 
# MAGIC order by total_quantity desc ;

# COMMAND ----------

# DBTITLE 1,top customer by city who made top sales
# MAGIC %sql
# MAGIC select sum(sales) as total_sales, customer_name,city from sample 
# MAGIC group by customer_name ,city
# MAGIC order by total_sales desc ;

# COMMAND ----------

