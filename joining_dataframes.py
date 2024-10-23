from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example2").getOrCreate()

data1 = [(1, "John", 28), (2, "Jane", 35), (3, "Sam", 24)]
data2 = [(1, "New York"), (2, "Los Angeles"), (3, "Chicago")]

columns1 = ["ID", "Name", "Age"]
columns2 = ["ID", "City"]

df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

df_joined = df1.join(df2, on="ID", how="inner")
df_joined.show()

df_left_joined = df1.join(df2, on="ID", how="left")
df_left_joined.show()
