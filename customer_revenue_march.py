# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = orders.where(year(col("order_date")) == 2019).where(month(col("order_date")) == 3).groupBy(col("cust_id")).agg(sum("total_order_cost").alias("revenue"))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
