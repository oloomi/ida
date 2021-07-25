library(jsonlite)

source('modules/data_util.R')
source('modules/feature_engineering.R')

init <- function()
{
  # Get the path to the model location of the registered model in Azure ML
  model_path <- Sys.getenv("AZUREML_MODEL_DIR")
  
  # Load the model
  model <- readRDS(file.path(model_path, "ida_model.rds"))
  message("logistic regression model loaded")
  
  # The following method will be called by Azure ML each time the deployed web service is invoked
  function(data)
  {
    # Deserialize the input data to the service
    df <- as.data.frame(fromJSON(data))
    
    # Feature engineering
    df <- processFeatures(df)
    
    # Convert datasets to XGBoost DMatrix
    #ToDo: the implementation for this section should be improved
    datasets <- list(trainData = df,
                     testData = df,
                     evalData = df)
    datasets <- convertyDataToXgBoost(datasets, 'aggressiveness_score')
    
    # Evaluate the data on the deployed model
    prediction <- predict(model, datasets$testData, type="response")
    
    # Return the prediction serialized to JSON
    toJSON(prediction)
  }
}