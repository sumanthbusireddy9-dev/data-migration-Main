import pandas as pd
import influxdb
import psycopg2
import mysql.connector
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import multiprocessing
import threading
import time
import math
from config import (
    mysql_connection_params,
    influxdb_connection_params,
    postgis_connection_params,
    postgis_table_name,  
    influx_bucket,
    influx_measurment,
    influx_org,
)

# Query data from InfluxDB
def query_from_influxdb(org, influx_client, influx_bucket, influx_measurment, flag):
    try:
      # Creating a query API for InfluxDB
      query_api = influx_client.query_api()

      # Flux query to fetch data from InfluxDB
      query = f'from(bucket:"{influx_bucket}")\
      |> range(start: 2023-01-01T00:00:00Z)\
      |> filter(fn:(r) => r._measurement == "{influx_measurment}")'
      
      # Executing and processing the query
      result = query_api.query(org=org, query=query)
      results = []
      if(flag ==1):
         for table in result:
            for record in table.records:
                results.append((record.get_time().strftime('%Y-%m-%d %H:%M:%S %Z'), record.values.get("station_id"), record.get_field(), record.get_value()))
         # print("influx ", len(results))
         return results
      else:
         for table in result:
            for record in table.records:
               results.append(record.get_value())
         return results
    except Exception as e:
       # Handling exceptions and printing an error message
       print("=============================================================================")
       print("Error when querying from InfluxDB")
       print("=============================================================================")
       return -1

# Query data from PostGIS
def query_from_postGIS(postgis_table_name, postgis_cur):
   try:
     # Query to fetch data from PostGIS
     selectQuery = f"select * from {postgis_table_name};"

     # Executing and fetching the results
     postgis_cur.execute(selectQuery)
     postgis_data = postgis_cur.fetchall()
   #   print("postgis ", len(postgis_data))
     return postgis_data
   except Exception as e:
      # Handling exceptions and printing an error message
      print("=============================================================================")
      print("Error when querying from postGIS")
      print("=============================================================================")
      return -1




if __name__ == "__main__":
   try:
     # Establishing connections to MySQL, InfluxDB, and PostGIS
     mysql_conn = mysql.connector.connect(**mysql_connection_params)
     influx_client = influxdb_client.InfluxDBClient(**influxdb_connection_params)
     postgis_conn = psycopg2.connect(**postgis_connection_params)
     postgis_cur = postgis_conn.cursor()
     print("-> Established connections to MySQL, InfluxDB, and PostGIS")

     # Querying data from MySQL, InfluxDB, and PostGIS
     influx_data = query_from_influxdb(influx_org, influx_client, influx_bucket, influx_measurment, 1)
     postgis_data = query_from_postGIS(postgis_table_name, postgis_cur)
   #   print("-> Fetched data from influxDB")
   #   print(influx_data)

     print("-> Fetched data from postGIS")
     print(postgis_data)
     
    
      
      
   

   finally:
      # Closing MySQL, InfluxDB, and PostGIS connections
      print("Closing database connections")
      mysql_conn.close()
      postgis_conn.close()  

   