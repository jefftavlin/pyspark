# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
flag_review = flag_review.where(col("reviewed_outcome") == "APPROVED")
joined = user_flags.join(flag_review, user_flags.flag_id == flag_review.flag_id)
result = joined.withColumn("username", concat(col("user_firstname"), lit(" "), col("user_lastname"))).groupBy(col("username")).agg(countDistinct("video_id").alias("count")).orderBy("count", ascending = False)
final = result.where(col("count") >= result.agg(max(col("count"))).collect()[0][0]).select("username")

# To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
