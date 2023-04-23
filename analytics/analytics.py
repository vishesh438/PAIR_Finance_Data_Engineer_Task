import os
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import json
import math
from datetime import datetime

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        psql_conn = psql_engine.raw_connection()
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')


# Connect to MySQL
mysql_engine = create_engine(os.environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
print('Connection to MySQL successful.')

# Write the solution here



# Functions for aggregations

# The maximum temperatures measured for every device per hours.
def get_max_temp_per_device_per_hour(device_id, start_time, end_time):
    query = f"SELECT MAX(temperature) FROM devices WHERE device_id='{device_id}' AND time >= '{start_time}' AND time < '{end_time}'"
    result = psql_engine.execute(query).fetchone()
    return result[0]

# The amount of data points aggregated for every device per hours.
def get_count_per_device_per_hour(device_id, start_time, end_time):
    query = f"SELECT COUNT(*) FROM devices WHERE device_id='{device_id}' AND time >= '{start_time}' AND time < '{end_time}'"
    result = psql_engine.execute(query).fetchone()
    return result[0]

# Total distance of device movement for every device per hours. 
def get_distance_per_device_per_hour(device_id, start_time, end_time):
    query = f"SELECT location FROM devices WHERE device_id='{device_id}' AND time >= '{start_time}' AND time < '{end_time}' ORDER BY time ASC"
    results = psql_engine.execute(query).fetchall()
    total_distance = 0
    for i in range(len(results) - 1):
        lat1, lon1 = json.loads(results[i][0])
        lat2, lon2 = json.loads(results[i+1][0])
        total_distance += distance(lat1, lon1, lat2, lon2)
    return total_distance


def distance(lat1, lon1, lat2, lon2):
    R = 6371  # radius of the Earth in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


# Get the start time of the data
query = "SELECT MIN(time) FROM devices"

psql_cursor = psql_conn.cursor()
psql_cursor.execute(query)
start_time = psql_cursor.fetchone()[0]
print(query)
print("Output of Above Query")
print(start_time)

#start_time = datetime.utcfromtimestamp(start_time).replace(minute=0, second=0, microsecond=0)
#print(start_time)
#
#
#
#with psql_engine.connect() as conn:
#    start_time = conn.execute(query).fetchone()[0]
#    start_time = datetime.utcfromtimestamp(start_time).replace(minute=0, second=0, microsecond=0)
#
## Aggregate data per device per hour and store the results in MySQL
#
#while True:
#    end_time = start_time.replace(hour=start_time.hour+1)
#    if end_time > datetime.utcnow():
#        break
#    print(f"Aggregating data for {start_time}...")
#    for device_id in psql_engine.execute("SELECT DISTINCT device_id FROM devices").fetchall():
#        device_id = device_id[0]
#        max_temp = get_max_temp_per_device_per_hour(device_id, start_time.timestamp(), end_time.timestamp())
#        count = get_count_per_device_per_hour(device_id, start_time.timestamp(), end_time.timestamp())
#        distance = get_distance_per_device_per_hour(device_id, start_time.timestamp(), end_time.timestamp())
#        query = f"INSERT INTO device_aggregates (device_id, time, max_temp, count, distance) VALUES ('{device_id}', '{start_time}', {max_temp}, {count}, {distance})"
#        mysql_engine.execute(query)
#    start_time = end_time
