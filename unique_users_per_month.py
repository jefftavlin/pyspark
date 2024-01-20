# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = fact_events.groupBy(["client_id", month(col("time_id"))]).agg(countDistinct(col("user_id")))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
