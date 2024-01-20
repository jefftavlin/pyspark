# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = facebook_reactions.where(col("reaction") == 'heart')

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
