# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = sf_restaurant_health_violations.where(col("business_name") == "Roxanne Cafe").withColumn("year", year(col("inspection_date"))).groupBy(col("year")).agg(countDistinct(col("violation_id")).alias('violation_code')).orderBy(desc(col("violation_code")))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
