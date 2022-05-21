library(tidyverse)
library(arrow)
library(sparklyr)
library(lubridate)
library(devtools)

## Prep Data ----
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dir_dim_account   <- paste0(c(dir_data_cur_dim,"dim_account.parquet"),collapse="/")
dir_bridge_account_product <- paste0(c(dir_data_cur_dim,"bridge_account_product.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)
dim_account       <- read_parquet(dir_dim_account)
bridge_account_product <- read_parquet(dir_bridge_account_product)

regex_find_date = "(.*?)(\\d{4}-)(\\d{1,2}-)(\\d{1,2})(.*)"

vct_asset_short_name <-  filter(dim_asset,asset_type_l2 == "VWAP",!str_detect(asset_name,"/")) %>% distinct(asset_short_name) %>% pull()
for(i in seq_along(vct_asset_short_name)){
  print(i)
  
  sel_asset_short_name <- vct_asset_short_name[i]
  dim_asset_loop <- filter(dim_asset,asset_short_name == sel_asset_short_name,data_src != "Calculated")
  dest_dir <- paste0(c(dir_data_cur_fact,"Calculated",sel_asset_short_name),collapse="/")
  
  print(sel_asset_short_name)
  
  df_dir_cex <- 
    select(dim_asset_loop,ticker_name,data_src) %>% 
    filter(data_src != "On-chain") %>%
    mutate(ticker_name = str_to_upper(ticker_name), data_src = str_to_title(data_src))
    
  vct_dir_cex <-   
    df_dir_cex %>% 
    rowwise() %>%
    mutate(
      dir = paste0(c(dir_data_cur_fact,data_src,ticker_name),collapse="/")
    ) %>% ungroup() %>% pull(dir)
  
  df_dir_dex <-
    bridge_account_product %>%
    inner_join(
      filter(dim_account,account_type == "LP") %>%
        select(dim_account_id,account_address,network)
      ,by=c("dim_account_id","network")
    ) %>%
    inner_join(
      filter(dim_asset_loop,data_src=="On-chain") %>%
        select(dim_asset_id)
      , by = c("product_dim_asset_id"="dim_asset_id")
    ) %>%
    inner_join(
      select(dim_asset,ticker_name,ticker_src_network,data_src)
      ,by=c("account_address"="ticker_name")
    ) %>%
    mutate(
      network = str_trim(gsub("network","",str_to_lower(network)))
    ) %>%
    select(account_address,data_src,network)
  
  vct_dir_dex <- 
    df_dir_dex %>%
    rowwise() %>%
    mutate(
      dir = paste0(c(dir_data_cur_fact,data_src,network,account_address),collapse="/")
    ) %>% ungroup() %>% pull(dir)
  
  ## Get max date of src files
  src_max_date <-
    max(
      as_date(gsub("date=","",list.files(vct_dir_dex)))
      ,as_date(gsub("date=","",list.files(vct_dir_cex)))
    )
  
  src_min_date <-
    min(
      as_date(gsub("date=","",list.files(vct_dir_dex)))
      ,as_date(gsub("date=","",list.files(vct_dir_cex)))
    )
  
  if(length(c(list.files(vct_dir_dex),list.files(vct_dir_cex)))!=0){
    if(length(list.files(dest_dir)) == 0){
      cat(paste0("File directory NOT found for ",sel_asset_short_name,"\n"))
      loopStartDate <- src_min_date
      cat(paste0("Creating directory at ",dest_dir,"\n"))
      dir.create(dest_dir,recursive=T)
    } else {
      cat(paste0("File directory found for ",sel_asset_short_name,"\n"))
      if(sel_op_save == 2){
        loopStartDate <- date_load_from
      } else {
        loopStartDate <- max(as.Date(gsub("date=","",list.files(dest_dir))))
      }
    }
    cat(paste0("Starting data retrieval from ",loopStartDate,"\n"))
    
    cat(paste0("Removing latest day data = ",loopStartDate,"\n"))
    dest_dir_files_full <- list.files(dest_dir,full.names = T)
    dest_dir_files <- list.files(dest_dir)
    dest_dir_files_idx <- as_date(gsub("date=","",dest_dir_files))
    dest_dir_files_full <- dest_dir_files_full[dest_dir_files_idx>=loopStartDate]
    unlink(dest_dir_files_full,force=T,recursive=T)
    
    src_dir_files <- list.files(c(vct_dir_cex,vct_dir_dex),full.names=T,recursive=T)
    src_dir_files_idx <- as_date(sub(regex_find_date,"\\2\\3\\4",src_dir_files))
    src_dir_files <- src_dir_files[src_dir_files_idx>=loopStartDate]
    
    res_raw = arrow::open_dataset(src_dir_files) %>% collect() %>% as_tibble()
    res_out = res_raw %>% 
      inner_join(filter(dim_asset,asset_type_l2 != "LP") %>% select(dim_asset_id,asset_name,asset_short_name),by="dim_asset_id") %>%
      filter(asset_short_name == sel_asset_short_name) %>%
      group_by(datetime,asset_short_name,dim_interval_id,quote_type) %>%
      summarise(
        o = sum(o*qav,na.rm=T)/sum(qav,na.rm=T)
        ,h = sum(h*qav,na.rm=T)/sum(qav,na.rm=T)
        ,l = sum(l*qav,na.rm=T)/sum(qav,na.rm=T)
        ,c = sum(c*qav,na.rm=T)/sum(qav,na.rm=T)
        ,v = sum(v,na.rm=T)
        ,qav = sum(qav,na.rm=T)
        ,num_trades = sum(num_trades,na.rm=T)
      ) %>% ungroup() %>%
      inner_join(
        filter(dim_asset,data_src == "Calculated") %>% select(dim_asset_id,asset_name)
        ,by=c("asset_short_name"="asset_name")
      ) %>%
      mutate(
        data_src = "calculated"
      ) %>%
      select_at(colnames(res_raw)) %>%
      mutate(date = as_date(datetime))
    
    df_search <- distinct(res_out,date) %>% arrange(date)
    l <- 1L
    while(l <= nrow(df_search)){
      ref <- slice(df_search,l:(l+1007L))
      arrow::write_dataset(
        inner_join(res_out,ref)
        ,dest_dir
        ,format = "parquet"
        ,partitioning = c("date")
      )
      l <- l+1008L
    }
    gc()
  }
}



