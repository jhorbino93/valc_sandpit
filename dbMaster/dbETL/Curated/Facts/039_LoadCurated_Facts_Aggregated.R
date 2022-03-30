## Get necessary dims
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dir_fact_prices   <- paste0(c(dir_data_cur_fact,"fact_prices.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)

regex_find_date = "(.*?)(\\d{4}-)(\\d{1,2}-)(\\d{1,2})(.*)"

# dir_cur_fact_src <- list.files(dir_data_cur_fact)
# dir_cur_fact_src <- dir_cur_fact_src[which(dir_cur_fact_src != "Aggregated")]

dir_cur_fact_src <- "Calculated"
dest_dir = paste0(c(dir_data_cur_fact,"Aggregated"),collapse="/")

if(length(list.files(dest_dir)) == 0){
  
  ## Collect all partitions and write as single dataset 
  cat(paste0("File directory NOT found for ",dest_dir,"\n"))
  cat("Creating directory\n")
  dir.create(dest_dir,recursive=T)
  cat("Retrieving all curated fact prices data\n")
  df_raw = arrow::open_dataset(
    list.files(paste0(dir_data_cur_fact,"/",dir_cur_fact_src),full.names=T,recursive = T)
  ) %>% collect() %>% as_tibble()
  
  write_parquet(df_raw,paste0(c(dest_dir,"fact_prices_calculated.parquet"),collapse="/"))
  
} else {
  cat(paste0("File directory found for ",dest_dir,"\n"))
  
  list_src_max_date <- 
    lapply(
      setNames(list.files(paste0(c(dir_data_cur_fact,dir_cur_fact_src),collapse="/")),list.files(paste0(c(dir_data_cur_fact,dir_cur_fact_src),collapse="/")))
      ,function(x){
        files = list.files(paste0(c(dir_data_cur_fact,dir_cur_fact_src,x),collapse="/"),full.names=T,recursive=T)
        files = sub(regex_find_date,"\\2\\3\\4",files)
        return(data.frame(max_date = max(as_date(files))))
      }
    )
  
  list_df_raw2 <- list()
    for(i in seq_along(names(list_src_max_date))){
      print(i)
      sel_asset_name = names(list_src_max_date)[i]
      sel_max_date = list_src_max_date[[i]][1,1]
      print(sel_asset_name)
      
      src_dir = paste0(c(dir_data_cur_fact,dir_cur_fact_src,sel_asset_name),collapse="/")
      
      src_dir_files = list.files(src_dir)
      if(length(src_dir_files)!=0){
        src_dir_files_idx = as_date(gsub("date=","",src_dir_files))
        src_dir_files = list.files(src_dir,full.names = T,recursive = T)[src_dir_files_idx >= sel_max_date]
        
        loop_res = arrow::open_dataset(src_dir_files) %>% collect() %>% as_tibble()
        list_df_raw2[[i]] = loop_res
      }
    }
  
  df_src_max_date <- bind_rows(list_src_max_date,.id="asset_name") %>% as_tibble() %>%
    inner_join(
      filter(dim_asset,ticker_src_network == "Calculated") %>% select(dim_asset_id,asset_name)
      ,by="asset_name"
    )
  
  df_raw = arrow::read_parquet(paste0(c(dest_dir,"fact_prices_calculated.parquet"),collapse="/"))
  df_raw1 = df_raw %>% inner_join(df_src_max_date,by="dim_asset_id") %>% filter(as_date(datetime) < max_date) %>% select_at(colnames(df_raw))
  df_raw2 = bind_rows(list_df_raw2)
  
  res_out = bind_rows(df_raw1,df_raw2)
  
  write_parquet(res_out,paste0(c(dest_dir,"fact_prices_calculated.parquet"),collapse="/"))
}


