from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example4").getOrCreate()

data = [(1, "John", 28), (2, "Jane", None), (3, None, 24)]
columns = ["ID", "Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

df_dropna = df.dropna()
df_dropna.show()

df_fillna = df.fillna({"Name": "Unknown", "Age": 0})
df_fillna.show()

df_replace = df.na.replace("John", "Jonathan")
df_replace.show()
