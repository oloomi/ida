library(sparklyr)
library(DBI)
library(data.table)
library(xgboost)
library(dplyr)
library(splitTools)
library(Metrics)

##----- Utility functions -----##

# Converts datasets to XGBoost Dense Matrix (xgb.DMatrix)
convertyDataToXgBoost <- function (x, target, features = NULL)
{
  if (!target %in% names(x$trainData)) {
    stop("A valid target variable name must be provided")
  }
  # List of feature column names
  if (is.null(features)) {
    features = names(x$trainData)[!names(x$trainData) %in%
                                    target]
  }
  else {
    if (!all(features %in% names(x$trainData))) {
      stop("Not all features can be found in training data.")
    }
    if ((target %in% features)) {
      warning("You've also included target variable in features and it will be excluded")
      features = features[features != target]
    }
  }
  res = lapply(x, function(y) {
    setDT(y)
    if (nrow(y) > 0) {
      tmptarget = y[, target, with = F][[1]]
      if (is.factor(tmptarget)) {
        lev <- unique(unlist(tmptarget))
        tmptarget <- as.integer(factor(tmptarget, levels = lev)) -
          1
      }
      tmp = xgb.DMatrix(data.matrix(y[, features, with = F]),
                        label = tmptarget, missing = 0)
      return(tmp)
    }
    else {
      return(NULL)
    }
  })
  res[["targetTest"]] = x$testData[, target, with = F]
  res[["features"]] = features
  gc()
  return(res)
}


# Returns Pierson Correlation between actuals and predictions
evaluateModel <- function(actuals, preds) {
  return(cor(actuals, preds))
}


##----- Read and process datasets -----##

# Create Spark connection
sc <- spark_connect(master = "local")

# Read Parquet files
drive <- spark_read_parquet(sc, "drive", "data/drive")
trip <- spark_read_parquet(sc, "trip", "data/trip")
weather <- spark_read_parquet(sc, "weather", "data/weather")
accel <- spark_read_csv(sc, "accel", "data/drive_features.csv")

# Merge Spark tables
df <- dbGetQuery(
  sc,
  " WITH temp_trip AS
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
                WHERE a.trip_id = b.trip_id
                 "
)

spark_disconnect(sc)

# Feature engineering

# Normalisation of features based on trip durtion
df$duration <- as.numeric((df$max_time - df$min_time) / 3600)
df$ft_sum_hard_brakes_10_flg_val <-
  df$ft_sum_hard_brakes_10_flg_val / df$duration
df$ft_sum_hard_brakes_3_flg_val <-
  df$ft_sum_hard_brakes_3_flg_val / df$duration

#df$ft_cnt_vehicle_deaccel_val <- df$ft_cnt_vehicle_deaccel_val/df$duration
#df$ft_sum_time_deaccel_val <- df$ft_sum_time_deaccel_val/df$duration
#df$ft_cnt_vehicle_accel_val <- df$ft_cnt_vehicle_accel_val/df$duration
#df$ft_sum_hard_accel_10_flg_val <- df$ft_sum_hard_accel_10_flg_val/df$duration
#df$ft_sum_hard_accel_3_flg_val <- df$ft_sum_hard_accel_3_flg_val/df$duration
#df$ft_sum_time_accel_val <- df$ft_sum_time_accel_val/df$duration

df <- as.data.table(df)
df[, perc_hard_brakes_10 := ntile(ft_sum_hard_brakes_10_flg_val, 100)]
df[, perc_hard_brakes_3 := ntile(ft_sum_hard_brakes_3_flg_val, 100)]
df[, aggressiveness_score := perc_hard_brakes_10 + perc_hard_brakes_3]

# Remove following features from data
df[, ':='(
  duration = NULL,
  min_time = NULL,
  max_time = NULL,
  vehicle_id = NULL,
  #trip_id=NULL,
  perc_hard_brakes_10 = NULL,
  perc_hard_brakes_3 = NULL,
  #ft_sum_hard_brakes_10_flg_val=NULL,
  ft_sum_hard_brakes_3_flg_val = NULL
)]


##----- Training model -----##

# Split data into train, validation, test
set.seed(3451)
inds <-
  partition(df$aggressiveness_score, p = c(
    train = 0.6,
    valid = 0.2,
    test = 0.2
  ))
str(inds)

train <- df[inds$train, ]
eval <- df[inds$valid, ]
test <- df[inds$test, ]

datasets <- list(trainData = train,
                 testData = test,
                 evalData = eval)

datasets <- convertyDataToXgBoost(datasets, 'aggressiveness_score')

#XGB parameters
paramxgBoost <- list(
  objective  = 'reg:squarederror',
  eval_metric = 'rmse',
  eta =  0.01,
  subsample = 0.75,
  colsample_bytree = 0.8076,
  min_child_weight = 16,
  max_depth = 3
)

watchlist = list(train = datasets$trainData, eval = datasets$evalData)

# Train the model
train_model <- function(train) {
  model = xgb.train(
    nrounds = 1000,
    params = paramxgBoost,
    data = train,
    early_stop_round = 10,
    watchlist = watchlist,
    print_every_n = 50,
    verbose = 1
  )
  
  return(model)
}

model <- train_model(datasets$trainData)

# Generate predictions for test data
preds = predict(model, datasets$testData)

# Evaluate the performance of the model on test data
# Pearson Correlation
cat("test-r:", evaluateModel(test$aggressiveness_score, preds))
# RMSE
cat("test-rmse:", rmse(test$aggressiveness_score, preds))

# Save the model
saveRDS(model, "model.rds")
