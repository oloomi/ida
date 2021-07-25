# Intelligent Driver Assist (IDA)
IDA is a sample R model productionised using AzureML R SDK (https://github.com/Azure/azureml-sdk-for-r). A predictive machine learning model is built where the output is an aggressiveness score for a driver's trip based on various metrics, including fuel consumption, RPM, speed, and so on.

## Getting Started
In order to run the training and prediction in your local development environment, update the data path in `config/dev.yaml`, set the working directory to the repository's root directory, and run `modules/training.R` and `modules/prediction.R`.

The AML pipeline is located in `pipelines/aml_training_inference.R` and the Azure DevOps pipeline is located at `deploy/ida-devops-pipeline.yml`.
