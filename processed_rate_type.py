# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
result = facebook_complaints.groupBy(col("type")).agg(count("type").alias("count"))
result_2 = facebook_complaints.where(col("processed") == True).groupBy(col("type")).agg(count("processed").alias('count_2')).withColumnRenamed("type", "type_2")
final = result.join(result_2, result.type == result_2.type_2).withColumn("processed_rate", col("count_2") / col("count")).select("type", "processed_rate")

# To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
