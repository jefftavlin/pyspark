# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
median = sat_scores.approxQuantile("sat_writing", [0.5],0)[0]
final = sat_scores.where(col("sat_writing") == median).select("student_id")

# To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
