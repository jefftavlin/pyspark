# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
df = employee.orderBy("salary", ascending = False).select("salary").limit(2).orderBy("salary").limit(1)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
