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
         return len(results)
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
     return len(postgis_data)
   except Exception as e:
      # Handling exceptions and printing an error message
      print("=============================================================================")
      print("Error when querying from postGIS")
      print("=============================================================================")
      return -1

# Query data from MySQL
def query_from_mySQL(mysql_conn, flag):
   try:
     # MySQL queries to fetch data
     mysql_query_influx = "select ts.timestamp as timestamp, st.station_id as station_id, pr.parameter_name as parameter, ts.value as value from stations st, time_series_data ts, weather_parameters pr where st.station_id = ts.station_id and pr.parameter_id = ts.parameter_id"
     mysql_query_postgis = "select st.station_id as station_id, st.station_name as station_name, st.latitude as latitude, st.longitude as longitude, st.elevation as elevation, sd.region as region, sd.country as country from stations st, spatial_data sd where st.station_id = sd.station_id"

     # Reading data from MySQL into influx and postgis DataFrames
     df_influx = pd.read_sql(mysql_query_influx, mysql_conn)
     df_postgis = pd.read_sql(mysql_query_postgis, mysql_conn)
     if(flag == 1):
      #   print(len(df_influx.index))
      #   print(len(df_postgis.index))
        return len(df_influx.index), len(df_postgis.index)
     else:
        return df_influx["value"]
   except Exception as e:
      # Handling exceptions and printing an error message
      print("=============================================================================")
      print("Error when querying from mySQL")
      print("=============================================================================")
      return -1
   
# Validate data for count
def validataData(mysql_influx_data, mysql_postgis_data, influx_data, postgis_data):
   if (mysql_influx_data == influx_data) and (mysql_postgis_data == postgis_data):
      return True
   return False

#validate data for sum
def validateDataForSum(mysql_influx_data, influx_data):
   # print(math.ceil(sum(mysql_influx_data)),"++++++++++")
   # print("**************",math.ceil(sum(influx_data)))
   if(math.ceil(sum(mysql_influx_data)) == math.ceil(sum(influx_data))):
      return True
   return False


if __name__ == "__main__":
   try:
     # Establishing connections to MySQL, InfluxDB, and PostGIS
     mysql_conn = mysql.connector.connect(**mysql_connection_params)
     influx_client = influxdb_client.InfluxDBClient(**influxdb_connection_params)
     postgis_conn = psycopg2.connect(**postgis_connection_params)
     postgis_cur = postgis_conn.cursor()
     print("-> Established connections to MySQL, InfluxDB, and PostGIS")

     # Querying data from MySQL, InfluxDB, and PostGIS
     mysql_influx_data_len, mysql_postgis_data_len = query_from_mySQL(mysql_conn, 1)
     influx_data_len = query_from_influxdb(influx_org, influx_client, influx_bucket, influx_measurment, 1)
     postgis_data_len = query_from_postGIS(postgis_table_name, postgis_cur)
     print("-> Fetched data from MySQL, InfluxDB and PostGIS")
     
     # Validating data using records count
     if (validataData(mysql_influx_data_len, mysql_postgis_data_len, influx_data_len, postgis_data_len)):
        print("**************************************************************************************")
        print(" Data is validated. Records count matched ")
        print("**************************************************************************************")
     else:
        print("**************************************************************************************")
        print(" Data validation failed. There is some mismatch in the records count ")
        print("**************************************************************************************")
      
    # validate time series data using sum of values 
     mysql_influx_data = query_from_mySQL(mysql_conn, 2)
     influx_data = query_from_influxdb(influx_org, influx_client, influx_bucket, influx_measurment, 2)

     # Validating data using sum of the values of the records
     if (validateDataForSum(mysql_influx_data,influx_data)):
        print("**************************************************************************************")
        print(" Data is validated. Sum of values column matched ")
        print("**************************************************************************************")
     else:
        print("**************************************************************************************")
        print(" Data validation failed. There is some mismatch in the sum of values column")
        print("**************************************************************************************")

      
      
   

   finally:
      # Closing MySQL, InfluxDB, and PostGIS connections
      print("Closing database connections")
      mysql_conn.close()
      postgis_conn.close()  

   