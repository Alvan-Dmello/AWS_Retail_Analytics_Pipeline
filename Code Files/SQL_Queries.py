from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("Data Preprocessing Online Store UK") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

s3_path = f"s3a://project-data-alvan/processed_data/part-00000-6bf34c5f-b73c-436f-981d-48fa9f3965c1-c000.csv"

try:
    df = spark.read.csv(s3_path, header=True, inferSchema=True)
    print("Dataset loaded successfully!")
except Exception as e:
    print(f"Error loading dataset: {e}")
    spark.stop()
    exit()

# Create temporary view
df.createOrReplaceTempView("sales_data")

print("------------------------------------------------------------------")

#1. Total Revenue by Country
print("Total Revenue by Country:")
query = """
SELECT 
    Country,
    SUM(Revenue) AS TotalRevenue
FROM 
    sales_data
GROUP BY 
    Country
ORDER BY 
    TotalRevenue DESC
"""
result = spark.sql(query)
result.show()

print("------------------------------------------------------------------")

#2. Top 5 Customers by Revenue
print("Top 5 Customers by Revenue")
query = """
SELECT 
    CustomerID,
    SUM(Revenue) AS TotalRevenue
FROM 
    sales_data
GROUP BY 
    CustomerID
ORDER BY 
    TotalRevenue DESC
LIMIT 5
"""
result = spark.sql(query)
result.show()

print("------------------------------------------------------------------")

from pyspark.sql.functions import when

df = df.withColumn(
    "MonthOrder",
    when(col("Month") == "January", 1)
    .when(col("Month") == "February", 2)
    .when(col("Month") == "March", 3)
    .when(col("Month") == "April", 4)
    .when(col("Month") == "May", 5)
    .when(col("Month") == "June", 6)
    .when(col("Month") == "July", 7)
    .when(col("Month") == "August", 8)
    .when(col("Month") == "September", 9)
    .when(col("Month") == "October", 10)
    .when(col("Month") == "November", 11)
    .when(col("Month") == "December", 12)
)
df.createOrReplaceTempView("sales_data")

#3. Monthly Revenue Trends
print("Monthly Revenue Trends")
query = """
SELECT 
    Year,
    Month,
    SUM(Revenue) AS MonthlyRevenue
FROM 
    sales_data
GROUP BY 
    Year, Month, MonthOrder
ORDER BY 
    Year, MonthOrder
"""
result = spark.sql(query)
result.show()


print("------------------------------------------------------------------")

#4. Average Transaction Value by Month
print("Average Transaction Value by Month")
query = """
SELECT 
    Month,
    SUM(Revenue) / COUNT(DISTINCT InvoiceNo) AS AvgTransactionValue
FROM 
    sales_data
GROUP BY 
    Month, MonthOrder
ORDER BY 
    MonthOrder
"""
result = spark.sql(query)
result.show()


print("------------------------------------------------------------------")

#5. Top 5 Products by Quantity Sold
print("Top 5 Products by Quantity Sold")
query = """
SELECT 
    StockCode,
    Description,
    SUM(Quantity) AS TotalQuantitySold
FROM 
    sales_data
GROUP BY 
    StockCode, Description
ORDER BY 
    TotalQuantitySold DESC
LIMIT 5
"""
result = spark.sql(query)
result.show()
