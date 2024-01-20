# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
ms_employee_salary = ms_employee_salary.groupBy("id", "first_name", "last_name", "department_id").agg(max(col("salary")).alias('salary')).orderBy("id")

# To validate your solution, convert your final pySpark df to a pandas df
ms_employee_salary.toPandas()
