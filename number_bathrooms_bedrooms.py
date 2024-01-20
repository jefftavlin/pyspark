# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = airbnb_search_details.groupBy(col("city"), col("property_type")).agg(avg(col("bathrooms")), avg(col("bedrooms")))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
