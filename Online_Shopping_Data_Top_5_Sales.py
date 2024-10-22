# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format
import matplotlib.pyplot as plt
# Create a Spark session
spark = SparkSession.builder.appName("OnlineShoppingDataAnalysis").getOrCreate()

# Load the dataset
file_path = "/FileStore/tables/online_shopping_data.csv"  # Update this with your file path
shopping_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the first few rows of the dataset
shopping_df.show()

# Print schema to check data types
shopping_df.printSchema()

# COMMAND ----------

# Clean the order_date format
cleaned_shopping_df = shopping_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Verify the changes
cleaned_shopping_df.show()
cleaned_shopping_df.printSchema()

# COMMAND ----------

# Handle null values
cleaned_shopping_df = cleaned_shopping_df.na.drop(subset=["product_name", "total_price"])
cleaned_shopping_df = cleaned_shopping_df.na.fill({"product_name": "Unknown", "total_price": 0})
cleaned_shopping_df.show(50)

# COMMAND ----------

# Calculate total sales per product
total_sales_per_product = cleaned_shopping_df.groupBy("product_name") \
    .agg({"total_price": "sum"}) \
    .withColumnRenamed("sum(total_price)", "total_sales") \
    .orderBy(col("total_sales").desc())

# Get the top 5 products based on total sales
print("Total sale per product")
total_sales_per_product.show(total_sales_per_product.count())
top_5_sales_per_product = total_sales_per_product.limit(5)
print("Top 5 sales per product")
top_5_sales_per_product.show()

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame for visualization
top_5_sales_pd = top_5_sales_per_product.toPandas()

# Create a pie chart using Matplotlib
plt.figure(figsize=(10, 8))
plt.pie(top_5_sales_pd['total_sales'], labels=top_5_sales_pd['product_name'], autopct='%1.1f%%', startangle=140)
plt.title('Top 5 Products by Total Sales')
plt.axis('equal')  # Equal aspect ratio ensures that pie chart is circular

# Display the chart
plt.show()

# Save the top 5 sales data to a CSV file in DBFS
csv_output_path = "/FileStore/tables/top_5_sales.csv"
top_5_sales_per_product.write.csv(csv_output_path, header=True)

# Optional: Print paths for confirmation
#print(f"Pie chart saved to: {output_file_path}")
print(f"CSV file saved to: {csv_output_path}")

# COMMAND ----------

df_output = spark.read.csv("/FileStore/tables/top_5_sales.csv")
df_output.show()
