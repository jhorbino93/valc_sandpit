## Get necessary dims
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)

cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)
parallelPackages=c("arrow","tidyverse","lubridate")

vct_schema_cols <- c("datetime","date","dim_asset_id","dim_interval_id","quote_type","data_src"
                     ,"o","h","l","c","v","qav","num_trades"
)

vct_binance_assets <- list.files(paste0(dir_raw,"/binance"))

for(i in seq_along(vct_binance_assets)){
  print(i)
  
  asset <- vct_binance_assets[i]
  dest_dir <- paste0(c(dir_data_cur_fact,"Binance",asset),collapse="/")
  src_dir <- paste0(c(dir_raw,"binance",asset),collapse="/")
  
  print(paste0("Loop for asset ",asset))
  
  if(length(list.files(dest_dir)) == 0){
    cat(paste0("File directory NOT found for ",asset,"\n"))
    loopStartDate <- min(as_date(gsub("open_date=","",list.files(src_dir))))
    cat(paste0("Creating directory at ",dest_dir,"\n"))
    dir.create(dest_dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",asset,"\n"))
    loopStartDate <- max(as.Date(gsub("date=","",list.files(dest_dir))))
  }
  cat(paste0("Starting data retrieval from ",loopStartDate,"\n"))
  
  print(paste0("Removing latest day data = ",loopStartDate))
  unlink(paste0(dest_dir,"/open_date=",loopStartDate),force=T,recursive=T)
  
  src_dir_files <- list.files(src_dir)
  src_dir_files_idx <- as_date(gsub("open_date=","",src_dir_files))
  src_dir_files <- list.files(src_dir,full.names=T,recursive=T)[src_dir_files_idx>=loopStartDate]
  
  print(paste0("Begin loop data curation for Binance Data ",asset))
  listk <-
    # for(k in seq_along(src_dir_files_idx)){
    foreach(
      x=seq_along(src_dir_files)
      ,.packages = parallelPackages
    ) %dopar% {
      
      dir <- src_dir_files[x]
      
      raw <- read_parquet(dir)
      
      df1 <- raw %>% 
        mutate(
          quote_type = "USD"
        ) %>%
        mutate(data_src="binance",ticker = str_to_lower(ticker)) %>%
        select(
          open_time,o,h,l,c,v,qav,num_trades
          ,ticker,interval,network,data_src
          ,quote_type
        ) %>%
        inner_join(
          select(dim_asset,dim_asset_id,ticker_name,ticker_src_network) %>% mutate(ticker_src_network = str_to_lower(ticker_src_network))
          ,by=c("ticker"="ticker_name","network"="ticker_src_network")
        ) %>%
        inner_join(
          select(dim_interval,dim_interval_id,interval_shortname)
          ,by=c("interval"="interval_shortname")
        ) %>%
        mutate(date = as_date(open_time)) %>%
        rename(datetime = open_time) %>%
        select_at(vct_schema_cols)
      
      return(df1)
    }
  resOut <- bind_rows(listk)

  cat(paste0("Writing to parquet files","\n"))
  df_search <- distinct(resOut,date) %>% arrange(date)
  l <- 1L
  while(l <= nrow(df_search)){
    ref <- slice(df_search,l:(l+1007L))
    arrow::write_dataset(
      inner_join(resOut,ref)
      ,dest_dir
      ,format = "parquet"
      ,partitioning = c("date")
    )
    l <- l+1008L
  }
  gc()
}
stopCluster(cl)
