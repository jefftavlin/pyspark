# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = worker.where(col("joining_date") >= "2014-04-01").where(col("department") == "Admin").agg(count("worker_id"))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
