# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = forbes_global_2010_2014.orderBy("profits", ascending = False).limit(3).select("company", "profits")

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
