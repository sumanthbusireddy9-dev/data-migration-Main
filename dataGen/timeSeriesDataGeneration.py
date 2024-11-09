import random
from datetime import datetime, timedelta
import mysql.connector
from Constants import MySQLValues
mValues=MySQLValues()
print(mValues.getHost())
# class Constants:
#     host = '127.0.0.1',
#     user = 'root@localhost',
#     password = 'root',
#     database = 'test_weather'


startDate = datetime(2023, 1, 1)
numberOfDays = 1
parameterIds = range(1, 4)
stationIds = range(1, 6)

try:
    conn = mysql.connector.connect(
        host=mValues.getHost(),
        user=mValues.getUser(),
        password=mValues.getPassword(),
        database=mValues.getDatabaseName(),
    )
    database = mValues.getDatabaseName()
    print(database)
    cursor = conn.cursor()
    if conn.is_connected():
        for _ in range(numberOfDays):
            print(startDate)
            currentTime = startDate
            for _ in range(24):  # 24 hours in a day
                for _ in range(60):  # 60 records per hour
                    for stationId in stationIds:
                        for parameterId in parameterIds:
                            timeStamp = currentTime.strftime('%Y-%m-%d %H:%M:%S')
                            if parameterId == 1:
                                value = random.uniform(-10, 40)
                            elif parameterId == 2:
                                value = random.uniform(10, 90)
                            else:
                                value = random.uniform(0.1, 100)
                            insertStatementValues = f"INSERT INTO {database}.time_series_data (station_id, parameter_id, timestamp, value) VALUES ({stationId}, {parameterId}, '{timeStamp}', {value:.2f})"
                            # print(insertStatementValues)
                            cursor.execute(insertStatementValues)
                    currentTime += timedelta(minutes=1)  # Increment timestamp by 1 minute
            startDate += timedelta(days=1)
    conn.commit()
    print("inserted successfully")
    # Process the fetched records

except mysql.connector.Error as e:
    print(f"Error in  connecting to MySQL: {e}")
except Exception as e:
    print(e)
finally:
    # Close the connection in the 'finally' block to ensure it's always closed
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        cursor.close()
        print("MySQL connection closed.")
