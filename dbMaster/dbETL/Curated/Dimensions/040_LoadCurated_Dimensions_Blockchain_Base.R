## Get necessary dims ----
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)


regex_find_date = "(.*?)(\\d{4}-)(\\d{1,2}-)(\\d{1,2})(.*)"
dir_blockchain <- paste0(c(dir_raw,"blockchain"),collapse="/")
dest_dir <- paste0(c(dir_data_cur_dim,"dim_block_dates.parquet"),collapse="/")

list_res <- list()
for(i in seq_along(list.files(dir_blockchain))){
  print(i)
  
  blockchain_name <- list.files(dir_blockchain)[i]
  src_dir <- paste0(c(dir_raw,"blockchain",blockchain_name,"base"),collapse="/")
  # dest_dir <- paste0(c(dir_data_cur_dim,paste0(blockchain_name,"_block_date.parquet")),collapse="/")
  
  src_files <- list.files(paste0(c(dir_raw,"blockchain",blockchain_name,"base"),collapse="/"),full.names=T,recursive=T)
  df_base <- arrow::open_dataset(src_files,format="parquet") %>% collect()  
  
  ## Should only do a delta load
  ## However this only makes practical sense if you can build polybase view or synapse db ontop of the parquet files
  ## Not possible in simple/local interface
  
  df_res <- 
    df_base %>%
    rename(
      datetime = target_time      
      ,attempt_datetime = attempt_time
    ) %>%
    mutate(
      date = as_date(datetime)
      ,attempt_date = as_date(attempt_datetime)
      ,blockchain = blockchain_name
    ) %>%
    select(blockchain,datetime,date,attempt_date,attempt_datetime,attempt_block,avg_block_time,diff_sec,diff_min) 
  
  list_res[[i]] <- df_res
}
res_out <- bind_rows(list_res)
arrow::write_parquet(res_out,dest_dir)

