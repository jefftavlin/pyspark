# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = user_flags.where(col("flag_id").isNotNull()).fillna("placeholder").withColumn("unique_user", concat(col("user_firstname"), "user_lastname")).groupBy(col("video_id")).agg(countDistinct("unique_user"))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
