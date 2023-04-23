# Data Engineer Technical Assessment - PAIR Finance - Vishesh Shrivastava

# Overview

* This project involves pulling data from a PostgresSQL database, aggregating the data per device per hour, and storing the results in a MySQL database. The data being processed comes from a table named devices with columns for device_id, temperature, location, and time.

* The solution consists of a Python script named ``` analytics.py ``` , which performs the following steps:

1. Waits for a data generator to start generating data.
2. Connects to the PostgresSQL database and retrieves the earliest time value from the devices table.
3. Loops through the data in the devices table, aggregating the data per device per hour and storing the results in a MySQL database.
4. Sleeps for 1 hour and repeats steps 3-4.


## Requirements
The following packages are required to run the ``` analytics.py ``` script:

- psycopg2 or psycopg2-binary for connecting to PostgresSQL
- mysql-connector-python for connecting to MySQL
- sqlalchemy for database ORM


## Usage
To run the analytics.py script, first make sure you have set the following environment variables:

- POSTGRESQL_CS: the connection string for the PostgresSQL database.
- MYSQL_CS: the connection string for the MySQL database.

Then, simply run the following command:

``` python analytics.py ``` 
The script will start executing and will run indefinitely until manually stopped.

## Configuration
The following configuration options can be customized in the analytics.py script:

- AGGREGATION_INTERVAL: the interval (in minutes) over which to aggregate data per device. Default is 60 minutes.
- BATCH_SIZE: the number of data points to process at once. Default is 1000.
- DEBUG_MODE: whether to enable debug output. Default is False.


## Output

I want to bring your attention to an issue that I encountered while running the program using the docker-compose up command. Specifically, the program was unable to fetch any data from the "devices" table in PostgresSQL, despite my best efforts to write the program in accordance with the problem statement.

To provide some context, I have attached a screenshot of the terminal window where no data was displayed when attempting to fetch data from the "devices" table. I have double-checked the code and the configuration, but unfortunately, I was unable to resolve the issue.

I would greatly appreciate it if you could advise me on how to proceed. Please let me know if you require any further information from my side, and I look forward to hearing from you soon.


## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.