from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName('PySpark Example').getOrCreate()

# Load the CSV
df = spark.read.csv('D:/terraform/input_data.csv', header=True, inferSchema=True)

# Show the data and schema
df.show()
df.printSchema()

# Filter or select using the 'Industry' column
df_filtered = df.filter(df.Industry.contains('Technology'))
df_filtered.show()

# Stop the Spark session
spark.stop()