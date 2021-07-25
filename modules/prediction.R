
runPrediction <- function() {
  # Ingest data
  df <- loadData()
  
  # Feature engineering
  df <- processFeatures(df)
  
  # Loading saved model
  model <- readRDS("model.rds")
  
  # Predict using the model
  preds <- predict(model, datasets$testData)
}

runPrediction()