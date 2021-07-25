library(sparklyr)
library(DBI)
library(data.table)
library(xgboost)
library(dplyr)
library(glue)

loadData <- function(vehicle_id = NULL, trip_id = NULL) {
  # Create Spark connection
  sc <- spark_connect(master = "local")
  
  # Read Parquet files
  drive <- spark_read_parquet(sc, "drive", "data/drive")
  trip <- spark_read_parquet(sc, "trip", "data/trip")
  weather <- spark_read_parquet(sc, "weather", "data/weather")
  accel <- spark_read_csv(sc, "accel", "data/drive_features.csv")
  
  # Merge Spark tables
  query = " WITH temp_trip AS
                 (SELECT a.vehicle_id, a.trip_id
                  , min(a.datetime) as min_time
                  , max(a.datetime) as max_time
                  , max(a.velocity) as max_velocity
                  , avg(a.velocity) as avg_velocity
                  , max(b.engine_coolant_temp) as max_coolant_temp
                  , max(b.eng_load) as max_eng_load
                  , max(b.fuel_level) as max_fuel_level
                  , min(b.fuel_level) as min_fuel_level
                  , max(b.iat) as max_iat
                  , min(b.iat) as min_iat
                  , max(b.rpm) as max_rpm
                 FROM trip a
                 INNER JOIN drive b
                  ON a.vehicle_id = b.vehicle_id
                  AND a.trip_id = b.trip_id
                  AND a.datetime = b.datetime
                 GROUP BY a.vehicle_id, a.trip_id
                 HAVING max(a.velocity) > 0)
                SELECT b.vehicle_id
                  , b.min_time
                  , b.max_time
                  , b.max_velocity
                  , b.avg_velocity
                  , b.max_coolant_temp
                  , b.max_eng_load
                  , b.max_fuel_level
                  , b.min_fuel_level
                  , b.max_iat
                  , b.min_iat
                  , b.max_rpm
                  , a.ft_sum_hard_brakes_10_flg_val
                  , a.ft_sum_hard_brakes_3_flg_val
                FROM accel a, temp_trip b
                WHERE a.trip_id = b.trip_id"
  
  # In prediction, add given vehicle id and trip id to where clause 
  vehicle_clause <- " AND b.vehicle_id = {vehicle_id}"
  trip_clause <- " AND b.trip_id = '{trip_id}'"
  if(!is.null(vehicle_id))
    query <- paste(query, glue(vehicle_clause))
  if(!is.null(trip_id))
    query <- paste(query, glue(trip_clause))
  
  df <- dbGetQuery(sc, query)
  
  spark_disconnect(sc)
  
  return(df)
}
