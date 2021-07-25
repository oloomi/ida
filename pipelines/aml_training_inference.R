# Install Azure ML SDK from CRAN
# install.packages("azuremlsdk")

library("azuremlsdk")

ws <- get_workspace(
  name = workspace_name ,
  subscription_id = subscription_id,
  resource_group = resource_group_name
)

# ws <- load_workspace_from_config()

# Define estimator
est <- estimator(
  source_directory = "modules",
  entry_script = "training.R",
  compute_target = "local"
)

# Initialize experiment
experiment_name <- "IDA_Training_Experiment"
exp <- experiment(ws, experiment_name)

# Submit job and display the run details
run <- submit_experiment(exp, est)
wait_for_run_completion(run, show_output = TRUE)

# Get the run metrics
metrics <- get_run_metrics(run)
metrics

# Register the model
model <-
  register_model(ws, model_path = "ida_model.rds", model_name = "ida_model")

# Create environment
# ToD: check base image and add missing packages
r_env <- r_environment(name = "ida_r_env")

# Create inference config
inference_config <- inference_config(
  entry_script = "score.R",
  source_directory = "pipelines",
  environment = r_env)

# Create ACI deployment config
deployment_config <- aci_webservice_deployment_config(cpu_cores = 1,
                                                      memory_gb = 1)

# Deploy the web service
service_name <- 'ida-aciwebservice-1'
service <- deploy_model(ws, 
                        service_name, 
                        list(model), 
                        inference_config, 
                        deployment_config)
wait_for_deployment(service, show_output = TRUE)

# Test the web service
service <- get_webservice(ws, service_name)
# ToDo: create a sample df for testing inference
predicted_score <- invoke_webservice(service, toJSON(sample_df))
