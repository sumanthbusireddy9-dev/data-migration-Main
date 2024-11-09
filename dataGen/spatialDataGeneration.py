import csv
from Constants import MySQLValues

mValues=MySQLValues()
# Define the range of station IDs (1 to 20)
station_ids = range(1, 21)

database = mValues.getDatabaseName()
print(database)

# Define the regions
regions = ['Region A', 'Region B', 'Region C', 'Region D']

# Generate and write the SQL INSERT statements to a file
with open('insert_spatial_data_regions.sql', 'w') as sqlfile:
    for station_id in station_ids:
        region = regions[(station_id - 1) // 5]  # Divide stations into 4 regions
        insert_statement = f"INSERT INTO {database}.spatial_data (station_id, region, country) VALUES ({station_id}, '{region}', 'USA');\n"
        sqlfile.write(insert_statement)
    print(f"Generated {len(station_ids)} INSERT statements for spatial_data with regions and saved to insert_spatial_data_regions.sql")
