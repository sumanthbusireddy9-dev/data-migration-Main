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
from config import (
    mysql_connection_params,
    influxdb_connection_params,
    postgis_connection_params,
    postgis_table_name,  
    influx_bucket,
    influx_measurment,
    influx_org,
)

# Migrate data to PostGIS
def migrate_to_postgis(df_postgis, postgis_table_name):
    try:
      # SQL query to insert data into PostGIS table 
      sql = f"INSERT INTO {postgis_table_name} (station_id, station_name, location, elevation, region, country) VALUES (%s, %s, st_GeomFromText('Point(%s %s)'), %s, %s, %s);"
      # Iterating through each row of the DataFrame and executing the SQL query
      for _, row in df_postgis.iterrows():
          postgis_cur.execute(sql, (row['station_id'], row['station_name'], row['latitude'], row['longitude'], row['elevation'], row['region'], row['country']))
    except Exception as e:
      # Handling exceptions and printing an error message
      print("=============================================================================")
      print("Error when migrating to postGIS: ", e)
      print("=============================================================================")

# Migrate data to InfluxDB
def migrate_to_influxdb(df_influx, influx_client, influx_bucket, influx_measurment, influx_org):
    try:
      # Creating a write API for InfluxDB
      write_api = influx_client.write_api(write_options=SYNCHRONOUS)
      # Iterating through each row of the DataFrame and writing data to InfluxDB
      for index,row in df_influx.iterrows():
        point = (
          Point(influx_measurment)
          .time(row["timestamp"])
          .tag("station_id",row["station_id"], )
          .field(row["parameter"], row["value"])
        )
        write_api.write(bucket=influx_bucket, org=influx_org, record=point)
    except Exception as e:
       # Handling exceptions and printing an error message
       print("=============================================================================")
       print("Error when migrating to influx: ",e)
       print("=============================================================================")
       

# Main execution block
if __name__ == "__main__":
    # Time at which program starts
    start_time = time.time()
    try:
      # Establishing connections to MySQL, InfluxDB, and PostGIS
      mysql_conn = mysql.connector.connect(**mysql_connection_params)
      influx_client = influxdb_client.InfluxDBClient(**influxdb_connection_params)
      postgis_conn = psycopg2.connect(**postgis_connection_params)
      postgis_cur = postgis_conn.cursor()
      print("-> Established connections to MySQL, InfluxDB, and PostGIS")
      
      # MySQL queries to fetch data
      mysql_query_influx = "select ts.timestamp as timestamp, st.station_id as station_id, pr.parameter_name as parameter, ts.value as value from stations st, time_series_data ts, weather_parameters pr where st.station_id = ts.station_id and pr.parameter_id = ts.parameter_id"
      mysql_query_postgis = "select st.station_id as station_id, st.station_name as station_name, st.latitude as latitude, st.longitude as longitude, st.elevation as elevation, sd.region as region, sd.country as country from stations st, spatial_data sd where st.station_id = sd.station_id"
      print("-> Fetched data from MySQL")
      
      # Reading data from MySQL into influx DataFrame and formatting the timestamp
      df_influx = pd.read_sql(mysql_query_influx, mysql_conn)
      df_influx['timestamp'] = pd.to_datetime(df_influx['timestamp'])
      df_influx['timestamp'] = pd.to_datetime(df_influx['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
      print("-> Created influx dataframe")
      
      # Reading data from MySQL into postgis DataFrame
      df_postgis = pd.read_sql(mysql_query_postgis, mysql_conn)
      postgis_table_name = postgis_table_name
      print("-> Created postgis dataframe")
      
      # Create the PostGIS table if it doesn't exist
      postgis_cur.execute(f"CREATE TABLE IF NOT EXISTS {postgis_table_name} ("
                         "station_id integer PRIMARY KEY, "
                         "station_name varchar(255), "
                         "location geography(Point, 4326), "
                         "elevation DOUBLE PRECISION, "
                         "region varchar(255), "
                         "country varchar(255));")
      
      
      # Migrating data to InfluxDB and PostGIS
      print("-> Migrating data to InfluxDB and PostGIS")
      migrate_to_influxdb(df_influx, influx_client, influx_bucket,influx_measurment, influx_org)
      migrate_to_postgis(df_postgis, postgis_table_name)
      
      # Committing changes to PostGIS
      postgis_conn.commit()
      print("**MIGRATION SUCCESSFUL**")

    except Exception as e:
       # Handling exceptions and printing an error message
       print("=============================================================================")
       print("Error: ",e)
       print("=============================================================================")
       
    finally:
    # Close MySQL, InfluxDB and PostGIS connections
      print("-> Closing database connections")
      mysql_conn.close()
      postgis_conn.close()

    # Time at which program ends
    end_time = time.time()

    # Time taken for the program to execute
    elapsed_time = end_time - start_time
    print("**************************************************************************************")
    print(f" Time taken to migrate from MySQL to InfluxDB and PostGIS: {elapsed_time:.4f} seconds ")
    print("**************************************************************************************")