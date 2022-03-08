## Dim Interval ----
dir_dim_interval <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
vct_pk_dim_interval <- paste0("interval_name")

if(file.exists(dir_dim_interval)){
  old_dim_interval <- arrow::read_parquet(dir_dim_interval)
  dim_interval <- read.csv(
    paste0(c(dir_ref,"maintenance_dim_interval.csv"),collapse="/")
    ,stringsAsFactors = F
    ,na.string=c("")
  ) %>% as_tibble()
  dim_interval <- fn_db_merge_dim(dim_interval,old_dim_interval,vct_pk_dim_interval,"dim_interval_id")
} else {
  dim_interval <- read.csv(
    paste0(c(dir_ref,"maintenance_dim_interval.csv"),collapse="/")
    ,stringsAsFactors = F
    ,na.string=c("")
  ) %>% as_tibble() %>%
    mutate(
      dim_interval_id = row_number()
    )
  dim_interval <- select_at(dim_interval,c("dim_interval_id",colnames(dim_interval)[which(!colnames(dim_interval) %in% c("dim_interval_id"))]))
}
write_parquet(dim_interval,dir_dim_interval)



