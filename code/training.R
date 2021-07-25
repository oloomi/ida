library(sparklyr)
library(DBI)
library(data.table)
library(xgboost)
library(dplyr)
library(splitTools)
library(Metrics)

source('code/data_ingestion.R')
source('code/data_util.R')
source('code/feature_engineering.R')

# Returns Pearson Correlation between actuals and predictions
evaluateModel <- function(actuals, preds) {
  return(cor(actuals, preds))
}

# Trains model on the input data df and returns the model
trainModel <- function(df) {
  
  # Split data into train, validation, test
  set.seed(3451)
  inds <-
    partition(df$aggressiveness_score, p = c(
      train = 0.6,
      valid = 0.2,
      test = 0.2
    ))
  train <- df[inds$train, ]
  eval <- df[inds$valid, ]
  test <- df[inds$test, ]
  datasets <- list(trainData = train,
                   testData = test,
                   evalData = eval)
  
  # Convert datasets to XGBoost DMatrix
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
  
  watchlist <- list(train = datasets$trainData, eval = datasets$evalData)
  
  # Train the model
  model = xgb.train(
    nrounds = 1000,
    params = paramxgBoost,
    data = datasets$trainData,
    early_stop_round = 10,
    watchlist = watchlist,
    print_every_n = 50,
    verbose = 1
  )
  
  # Generate predictions for test data
  preds = predict(model, datasets$testData)
  
  # Evaluate the performance of the model on test data
  # Pearson Correlation
  cat("test-r:", evaluateModel(test$aggressiveness_score, preds))
  # RMSE
  cat("test-rmse:", rmse(test$aggressiveness_score, preds))
  
  return(model)
}

runModelTraining <- function() {
  # Ingest data
  df = loadData()
  
  # Feature engineering
  df = processFeatures(df)
  
  # Train the model
  model = trainModel(df)
  
  # Save the trained model
  saveRDS(model, 'model.rds')
}

runModelTraining()
