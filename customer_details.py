# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = customers.join(orders, customers.id == orders.cust_id, how = "left").select("first_name", "last_name", "city", "order_details").orderBy("first_name", "order_details")

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
