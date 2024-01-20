# Import your libraries
import pyspark

# Start writing code
result = airbnb_hosts.join(airbnb_guests, [airbnb_hosts.nationality == airbnb_guests.nationality, airbnb_hosts.gender == airbnb_guests.gender]).dropDuplicates().select("host_id", "guest_id")

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
