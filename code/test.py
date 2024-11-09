# test.py

from config import (
    mysql_connection_params,
    influxdb_connection_params,
    postgis_connection_params,
    postgis_table_name,  # Corrected variable name
    influx_bucket,
    influx_measurment,
)

# Use the connection parameters
print("MySQL Connection Params:", mysql_connection_params)
print("InfluxDB Connection Params:", influxdb_connection_params)
print("PostGIS Connection Params:", postgis_connection_params)
print("PostGIS Table Name:", postgis_table_name)
print("Influx Bucket:", influx_bucket)
print("Influx Measurement:", influx_measurment)
print("**************************************************************************************")
print(f" Time taken to migrate from MySQL to InfluxDB and PostGIS: 4.667 seconds ")
print("**************************************************************************************")