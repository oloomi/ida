# Feature engineering

processFeatures <- function(df) {
  # Normalisation of features based on trip duration
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
  
  return(df)
}
