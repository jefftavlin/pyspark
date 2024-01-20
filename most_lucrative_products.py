# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = online_orders.where(col("date") >= "2022-01-01").where(col("date") <= '2022-06-30').groupBy(col("product_id")).agg(sum(col("cost_in_dollars") * col("units_sold")).alias("revenue")).orderBy("revenue", ascending = False).limit(5)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
