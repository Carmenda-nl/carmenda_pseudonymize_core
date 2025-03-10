# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('PySpark Sample DataFrame').getOrCreate()

# Define Schema
col_schema = ["Language", "Version"]

# Prepare Data
Data = (("Jdk", "17.0.12"), ("Python", "3.11.9"), ("Spark", "3.5.1"),
        ("Hadoop", "3.3 and later"), ("Winutils", "3.6"),
        )

# Create DataFrame
df = spark.createDataFrame(data=Data, schema=col_schema)
df.printSchema()
df.show(5, truncate=False)
