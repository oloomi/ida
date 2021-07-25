source('modules/data_ingestion.R')
source('modules/data_util.R')
source('modules/feature_engineering.R')

runPrediction <- function(vehicle_id = NULL,
                          trip_id = NULL,
                          envr = "dev") {
  
  # Load data for the given vehicle's trip
  df <- loadData(vehicle_id, trip_id, envr)
  
  # Feature engineering
  df <- processFeatures(df)
  
  # Convert datasets to XGBoost DMatrix
  #ToDo: the implementation for this section should be improved
  datasets <- list(trainData = df,
                   testData = df,
                   evalData = df)
  datasets <- convertyDataToXgBoost(datasets, 'aggressiveness_score')
  
  # Loading saved model
  model <- readRDS("model.rds")
  
  # Predict using the model
  preds <- predict(model, datasets$testData)
  
  return(preds)
}

predicted_score <- runPrediction(vehicle_id = 1000516,
                                 trip_id = '36417dfecb2049769ecb656e97fae95d')
cat('Aggressiveness Score: ', predicted_score)