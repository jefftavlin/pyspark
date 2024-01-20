# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = oscar_nominees.where(col("nominee") == 'Abigail Breslin').agg(countDistinct(col("movie")))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
