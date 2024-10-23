from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("example3").getOrCreate()

data = [(1, "John", 28), (2, "Jane", 35), (3, "Sam", 24), (4, "Jake", 28)]
columns = ["ID", "Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

df_grouped = df.groupBy("Age").agg(F.count("ID").alias("Count"), F.avg("ID").alias("Average_ID"))
df_grouped.show()

df_with_total = df.withColumn("Total_Age", F.sum("Age").over(Window.partitionBy()))
df_with_total.show()
