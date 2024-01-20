# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = worker.where(col("joining_date") >= "2014-04-01").groupBy(col("department")).agg(count(col("department")).alias("count")).orderBy("count", ascending = False)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
