from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example1").getOrCreate()

data = [(1, "John", 28), (2, "Jane", 35), (3, "Sam", 24)]
columns = ["ID", "Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

df_selected = df.select("Name", "Age")
df_selected.show()

df_filtered = df.filter(df.Age > 30)
df_filtered.show()
