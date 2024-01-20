# Import your libraries
import pyspark
from pyspark.sql.functions import * 

# Start writing code
joined = db_employee.join(db_dept, db_employee.department_id == db_dept.id)
engineering = joined.where(col("department") == 'engineering').agg(max(col("salary")).alias("e_salary"))
marketing = joined.where(col("department") == "marketing").agg(max(col("salary")).alias("m_salary"))
final = marketing.join(engineering).withColumn("salary_difference", abs(col("e_salary") - col("m_salary"))).select("salary_difference")

# To validate your solution, convert your final pySpark df to a pandas df
final.toPandas()
