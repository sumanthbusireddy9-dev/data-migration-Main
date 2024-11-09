#mySQL connection parameters
mysql_connection_params = {
    'host': 'localhost',
    'user': 'root',
    'password': '###',
    'database': 'test_weather10',
}

#InfluxDB connection parameters
influxdb_connection_params = {
    'url': 'http://localhost:8086',
    'token': "QE6fZAqb3JdjWtUxrj3JdhYqblTPwrkdC_sCQa0uJJ1KWv8dytQwNH-tGn8QzP3LVnFB9BzzQS56Q6gQGhOiOQ==",
    'org': 'cps691group4',
}

#postGIS connection parameters
postgis_connection_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgis_34_sample',
    'user': 'postgres',
    'password': '###',
}


influx_measurment="weather_timeseries20"

influx_bucket="test3"

influx_org = "cps691group4"

num_threads = 16

postgis_table_name = "test_weatherspatial54"






