from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def age_category(age):
    if age > 30:
        return "Senior"
    else:
        return "Junior"

age_category_udf = udf(age_category, StringType())

df_with_category = df.withColumn("Category", age_category_udf(df.Age))
df_with_category.show()
