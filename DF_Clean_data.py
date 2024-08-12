from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit, split, regexp_replace

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Example1") \
    .master("local") \
    .getOrCreate()

column_names = [
    "order id", "customer id", "customer name", "product id", 
    "product name", "product category", "payment method", 
    "quantity", "unit price", "order date", "country", 
    "city", "store", "payment transaction id", "payment success", 
    "failure reason", "hour", "minute", "second"
]

# Read the CSV file into a DataFrame
df = spark.read.csv("hdfs:///jerry/data_team_4.csv", header=False, inferSchema=False)

df = df.toDF(*column_names)

# Remove commas from countries with commas
df = df.withColumn("country", 
                   when(col("country") == "Bonaire, Sint Eustatius, and Saba", 
                        lit("Bonaire Sint Eustatius and Saba"))
                   .when(col("country") == "Korea, South", 
                         lit("South Korea"))
                   .when(col("country") == "Korea, North", 
                         lit("North Korea"))
                   .otherwise(col("country")))

# Combine product sub-categories into one column
df = df.withColumn("combined_category", 
                   when(col("category_column1").isNotNull(), 
                        col("category_column1") + col("category_column2")))

# Filter out rogue rows with missing/false data
df = df.filter((col("column9") != '00000000000') & 
               (col("column1").isNotNull() & (col("column1") != "")) & 
               (col("column2").isNotNull() & (col("column2") != "")) & 
               # Add checks for other necessary columns...
               (col("column15").isNotNull() & (col("column15") != "")))

# Split dates and create new columns for time
df = df.withColumn("date", split(col("column9"), " ")[0]) \
       .withColumn("time", split(col("column9"), " ")[1]) \
       .withColumn("hour", split(col("time"), ":")[0]) \
       .withColumn("minute", split(col("time"), ":")[1]) \
       .withColumn("second", split(col("time"), ":")[2])

# Select the relevant columns for the final output
final_df = df.select(*[col for col in df.columns if col not in ['column9', 'time']])

# Write the cleaned DataFrame to a CSV file
final_df.write.csv('hdfs:///jerry/proj2output', header=True)

# Stop the Spark session
spark.stop()
