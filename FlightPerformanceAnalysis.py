# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in AWS S3. There are two ways to establish access to S3: [IAM roles](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) and access keys.
# MAGIC 
# MAGIC *We recommend using IAM roles to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.* If you do use keys, you'll have to escape the `/` in your keys with `%2F`.
# MAGIC 
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# Access S3 bucket and mount it into Databricks. Check if a mount exists or not before creating a new mount. 
import urllib
ACCESS_KEY = "*****"
SECRET_KEY = "*****"
SECRET_KEY= SECRET_KEY.replace('/','%2F')
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "*****"
MOUNT_NAME = "Northwoods"
if any(mount.mountPoint == '/mnt/%s' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

display(dbutils.fs.ls("/mnt/Northwoods"))


# COMMAND ----------

# 1. Get data from mounted files into dataframes. 
# 2. Push the dataframe to delta files (overwrite to allow repeated executions).
# 3. Push the dataframe to Snowflake (overwrite to allow repeated executions).


# Snowflake Credentials
sfOptions = dict(
  sfURL = "*****",
  sfUser = "*****",
  sfPassword = "*****",
  sfDatabase = "*****",
  sfSchema = "*****",
  sfWarehouse = "*****",
)

dataFrameAirlines = "/mnt/Northwoods/airlines.csv"
dfAirlines=spark.read.format("csv").option("header","true").option("inferSchema", "true").load(dataFrameAirlines)
dfAirlines.write.format("delta").mode("overwrite").save("/mnt/delta/airlines")
dfAirlines.write.format("snowflake").mode("overwrite").options(**sfOptions).option("dbtable", "DATABRICKS_AIRLINES").save()

dataFrameAirports = "/mnt/Northwoods/airports.csv"
dfAirports=spark.read.format("csv").option("header","true").option("inferSchema", "true").load(dataFrameAirports)
dfAirports.write.format("delta").mode("overwrite").save("/mnt/delta/airports")
dfAirports.write.format("snowflake").mode("overwrite").options(**sfOptions).option("dbtable", "DATABRICKS_AIRPORTS").save()

dataFrameFlights = "/mnt/Northwoods/flights.csv"
dfFlights=spark.read.format("csv").option("header","true").option("inferSchema", "true").load(dataFrameFlights)
dfFlights.write.format("delta").mode("overwrite").save("/mnt/delta/flights")
dfFlights.write.format("snowflake").mode("overwrite").options(**sfOptions).option("dbtable", "DATABRICKS_FLIGHTS").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* 
# MAGIC Report: Cancellation reasons by airport 
# MAGIC Logic: Count of cancelled flights per cancellation reason per airport. 
# MAGIC */
# MAGIC select 
# MAGIC distinct
# MAGIC cancellation_reason,
# MAGIC ap.airport,
# MAGIC count(flight_number) FlightCount
# MAGIC from delta.`/mnt/delta/flights` f 
# MAGIC join delta.`/mnt/delta/airports` ap on f.origin_airport=ap.iata_code
# MAGIC where cancelled=1
# MAGIC group by 
# MAGIC cancellation_reason,
# MAGIC ap.airport
# MAGIC order by cancellation_Reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* 
# MAGIC Report: On time percentage of each airline for the year 2015 
# MAGIC Logic: 
# MAGIC On time percentage calculated for On Time Arrival as well as On Time Departure. 
# MAGIC Arrival Delay or Departure Delay should be less than 15 minutes as per the defintion below. 
# MAGIC Delay Definition: A flight is counted as "on time" if it operated less than 15 minutes later than the scheduled time shown in the carriers' Computerized Reservations Systems (CRS). Arrival performance is based on arrival at the gate. Departure performance is based on departure from the gate. (Reference: https://www.bts.gov/topics/airlines-and-airports/airline-time-performance-and-causes-flight-delays)
# MAGIC */
# MAGIC select 
# MAGIC pct.airline, 
# MAGIC pct.iata_code,
# MAGIC (pct.OnTimeArrivalCount*100.0)/tot.TotalCount as OnTimeArrivalPct,
# MAGIC (pct.OnTimeDepartureCount*100.0)/tot.TotalCount as OnTimeDeparturePct
# MAGIC from
# MAGIC (
# MAGIC select
# MAGIC arr.airline,
# MAGIC arr.iata_code,
# MAGIC arr.OnTimeArrivalCount,
# MAGIC dep.OnTimeDepartureCount
# MAGIC from
# MAGIC (select 
# MAGIC al.airline,
# MAGIC f.airline as IATA_CODE,
# MAGIC count(flight_number)  as OnTimeArrivalCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC where 
# MAGIC arrival_delay<15
# MAGIC and year=2015
# MAGIC group by al.airline,f.airline) arr
# MAGIC join
# MAGIC (select 
# MAGIC al.airline,
# MAGIC f.airline as IATA_CODE,
# MAGIC count(flight_number) as OnTimeDepartureCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC where 
# MAGIC departure_delay<15
# MAGIC and year=2015
# MAGIC group by al.airline,f.airline) dep
# MAGIC on arr.iata_code=dep.iata_code) pct
# MAGIC join 
# MAGIC (select airline as IATA_CODE,count(flight_number) as TotalCount from delta.`/mnt/delta/flights` where year=2015 group by airline) tot 
# MAGIC on pct.IATA_CODE=tot.IATA_CODE;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* 
# MAGIC Report: Delay reasons by airport 
# MAGIC Logic: Average Delay per Delay reason, per "Destination" airport. I found that the summation of values for delay reasons equals arrival delay, so I inferred that all delay reasons ultimately result in arrival delay, meaning delays should be associated with arrival airports. 
# MAGIC Arrival Delay should be >=15 as per - 
# MAGIC Delay Definition: A flight is counted as "on time" if it operated less than 15 minutes later than the scheduled time shown in the carriers' Computerized Reservations Systems (CRS). Arrival performance is based on arrival at the gate. Departure performance is based on departure from the gate. (Reference: https://www.bts.gov/topics/airlines-and-airports/airline-time-performance-and-causes-flight-delays)
# MAGIC */
# MAGIC 
# MAGIC select 
# MAGIC ap.airport,
# MAGIC 'Air System Delay' as DelayReason,
# MAGIC avg(air_system_delay) as AvgDelay
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airports` ap on f.destination_airport=ap.iata_code
# MAGIC where arrival_delay>=15 and air_system_delay>0
# MAGIC group by ap.airport,DelayReason
# MAGIC union
# MAGIC select 
# MAGIC ap.airport,
# MAGIC 'Security Delay' as DelayReason,
# MAGIC avg(air_system_delay) as AvgDelay
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airports` ap on f.destination_airport=ap.iata_code
# MAGIC where arrival_delay>=15 and security_delay>0
# MAGIC group by ap.airport,DelayReason
# MAGIC union
# MAGIC select 
# MAGIC ap.airport,
# MAGIC 'Airline Delay' as DelayReason,
# MAGIC avg(air_system_delay) as AvgDelay
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airports` ap on f.destination_airport=ap.iata_code
# MAGIC where arrival_delay>=15 and airline_delay>0
# MAGIC group by ap.airport,DelayReason
# MAGIC union
# MAGIC select 
# MAGIC ap.airport,
# MAGIC 'Late Aircraft Delay' as DelayReason,
# MAGIC avg(air_system_delay) as AvgDelay
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airports` ap on f.destination_airport=ap.iata_code
# MAGIC where arrival_delay>=15 and late_aircraft_delay>0
# MAGIC group by ap.airport,DelayReason
# MAGIC union
# MAGIC select 
# MAGIC ap.airport,
# MAGIC 'Weather Delay' as DelayReason,
# MAGIC avg(air_system_delay) as AvgDelay
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airports` ap on f.destination_airport=ap.iata_code
# MAGIC where arrival_delay>=15 and weather_delay>0
# MAGIC group by ap.airport,DelayReason

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* 
# MAGIC Report: Airlines with the largest number of delays (Arrival)
# MAGIC Logic: Count number of flights with arrival delay greater than 15 minutes. 
# MAGIC Delay Definition: A flight is counted as "on time" if it operated less than 15 minutes later than the scheduled time shown in the carriers' Computerized Reservations Systems (CRS). Arrival performance is based on arrival at the gate. Departure performance is based on departure from the gate. (Reference: https://www.bts.gov/topics/airlines-and-airports/airline-time-performance-and-causes-flight-delays)
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC al.airline,
# MAGIC count(flight_number) ArrivalDelayCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC where arrival_delay>=15
# MAGIC group by al.airline
# MAGIC order by count(flight_number)desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Report: Total number of flights by airline and airport on a monthly basis
# MAGIC Logic: Count of flights grouped by Month, Airline and Origin Airport. Combined YEAR and MONTH columns to convert to a date to enable Line chart visualization. Date defaulted to first day of every month. 
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC al.airline,
# MAGIC ap.airport as OriginAirport, 
# MAGIC cast(year||'-'||month||'-'||'01' as Date) as Month,
# MAGIC count(flight_number) FlightCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC join delta.`/mnt/delta/airports` ap on f.origin_airport=ap.iata_code
# MAGIC group by 
# MAGIC al.airline,
# MAGIC ap.airport, 
# MAGIC cast(year||'-'||month||'-'||'01' as Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 
# MAGIC Report: Airline with the most unique routes
# MAGIC Logic: 
# MAGIC 1. Unique Routes: Minimum Flight frequency between an origin and a destination. 
# MAGIC 2. Count unique routes per airline. 
# MAGIC 3. Rank airlines and routes based on no. of unique routes (Most unique routes ranked higher). 
# MAGIC 4. Sum of ranks (with all routes ranked 1) to get no. of unique routes per airline.
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC select airline,sum(UniqueRoutesRank) as UniqueRoutesCount from
# MAGIC (select 
# MAGIC airline,origin_airport,destination_airport,
# MAGIC rank() over(order by FlightCount) as UniqueRoutesRank
# MAGIC from 
# MAGIC (select 
# MAGIC al.airline,
# MAGIC apo.airport as origin_airport,
# MAGIC apd.airport as destination_airport,
# MAGIC count(flight_number) FlightCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC join delta.`/mnt/delta/airports` apo on f.origin_airport=apo.iata_code
# MAGIC join delta.`/mnt/delta/airports` apd on f.destination_airport=apd.iata_code
# MAGIC group by
# MAGIC al.airline,
# MAGIC apo.airport,
# MAGIC apd.airport))
# MAGIC where uniqueroutesrank=1
# MAGIC group by airline
# MAGIC order by UniqueRoutesCount desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* 
# MAGIC Report: Airlines with the largest number of delays (Departure)
# MAGIC Logic: Count number of flights with departure delay greater than 15 minutes. 
# MAGIC Delay Definition: A flight is counted as "on time" if it operated less than 15 minutes later than the scheduled time shown in the carriers' Computerized Reservations Systems (CRS). Arrival performance is based on arrival at the gate. Departure performance is based on departure from the gate. (Reference: https://www.bts.gov/topics/airlines-and-airports/airline-time-performance-and-causes-flight-delays)
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC al.airline,
# MAGIC count(flight_number) DepartureDelayCount
# MAGIC from delta.`/mnt/delta/flights` f
# MAGIC join delta.`/mnt/delta/airlines` al on f.airline=al.iata_code
# MAGIC where departure_delay>=15 
# MAGIC group by al.airline
# MAGIC order by count(flight_number)desc;
