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

vct_dexscreener <- list.files(paste0(dir_raw,"/dexscreener"))
for(i in seq_along(vct_dexscreener)){
  print(i)
  
  network <- vct_dexscreener[i]
  vct_address <- list.files(paste0(c(dir_raw,"dexscreener",network),collapse="/"))
  
  for(k in seq_along(vct_address)){
    print(k)
    
    asset <- vct_address[k]
    src_dir <- paste0(c(dir_raw,"dexscreener",network,asset),collapse="/")
    dest_dir <- paste0(c(dir_data_cur_fact,"dexscreener",network,asset),collapse="/")
    
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
    
    print(paste0(c("Begin loop data curation for Dexscreener",network,asset),collapse=" "))
    
    list_res <-
      foreach(
        x=seq_along(src_dir_files)
        ,.packages = parallelPackages
      ) %dopar% {
        dir <- src_dir_files[x]
        raw <- read_parquet(dir) %>%
          as_tibble(raw) %>%
          mutate(
            network = case_when(
              network == "harmony" ~ "Harmony Network"
              ,T ~ network
            )
          ) %>%
          inner_join(
            select(dim_asset,ticker_name,ticker_src_network,asset_to,asset_from)
            ,by=c("ticker"="ticker_name","network"="ticker_src_network")
          ) %>%
          select(-c("schemaVersion","bars.timestamp"))
        
        raw1 <-
          raw %>%
          rename(
            datetime = open_time
            ,o = bars.openUsd
            ,h = bars.highUsd
            ,l = bars.lowUsd
            ,c = bars.closeUsd
            ,qav = bars.volumeUsd
          ) %>% mutate(
            v = qav/mean(c(o,h,l,c),na.rm=T) ## Derive from avg price
            ,num_trades = NA_integer_
            ,quote_type = "USD"
            ,date = as_date(datetime)
          ) %>%
          # select(asset_to,network)
          inner_join(
            select(dim_asset,dim_asset_id,asset_name,onchain_network)
            ,by=c("asset_to"="asset_name","network"="onchain_network")
          ) %>%
          inner_join(
            select(dim_interval,dim_interval_id,interval_shortname)
            ,by=c("interval"="interval_shortname")
          ) %>%
          select_at(vct_schema_cols)
        
        raw2 <- 
          raw %>%
          mutate(
            datetime = open_time
            ,o = bars.openUsd/bars.open
            ,h = bars.highUsd/bars.high
            ,l = bars.lowUsd/bars.low
            ,c = bars.closeUsd/bars.close
            ,qav = bars.volumeUsd
            ,v = qav/mean(c(o,h,l,c),na.rm=T)
            ,num_trades = NA_integer_
            ,quote_type = "USD"
            ,date = as_date(datetime)
          ) %>%
          inner_join(
            select(dim_asset,dim_asset_id,asset_name,onchain_network)
            ,by=c("asset_from"="asset_name","network"="onchain_network")
          ) %>%
          inner_join(
            select(dim_interval,dim_interval_id,interval_shortname)
            ,by=c("interval"="interval_shortname")
          ) %>%
          select_at(vct_schema_cols)
        
        res <- bind_rows(raw1,raw2)
        return(res)
      }
    resOut <- bind_rows(list_res)
    
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
  }
  gc()
}
stopCluster(cl)





