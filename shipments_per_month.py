# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = amazon_shipment.withColumn("key", col("shipment_id") + col("sub_id")).withColumn("year_month", date_format(col("shipment_date"), "yyyy-MM")).groupBy("year_month").agg(count(col("key")))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
