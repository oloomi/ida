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