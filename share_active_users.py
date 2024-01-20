# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
fb_active_users = fb_active_users.where(col("country") == "USA").groupBy(col("status")).agg(count(col("status")).alias('count'))

open = fb_active_users.where(col("status") == 'open')
summed = fb_active_users.agg(sum('count').alias('count_a'))
final = open.join(summed)
final = final.withColumn('active_users_share', col("count") / col('count_a')).drop('count', 'count_a','status')

# To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
