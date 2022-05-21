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

cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)
parallelPackages=c("arrow","tidyverse","lubridate")

for(i in seq_along(list.files(dir_blockchain))){
  print(i)
  
  blockchain_name <- list.files(dir_blockchain)[i]
  
  # dir_contract <- list.files(paste0(c(dir_blockchain,blockchain_name,"supply"),collapse="/"))
  
  dir_contract <- sapply(
    list.files(paste0(c(dir_blockchain,blockchain_name,"supply"),collapse="/"),full.names = T)
    ,function(f){
      length(list.files(f))
    }
  )
  dir_contract <- list.files(paste0(c(dir_blockchain,blockchain_name,"supply"),collapse="/"))[which(dir_contract > 0L)]
  
  loop_dim_asset <-         
    filter(
      dim_asset
      ,tolower(gsub(" Network","",onchain_network)) == blockchain_name
    ) %>%
    select(dim_asset_id,onchain_address)
  
  for(k in seq_along(dir_contract)){
    print(k)
    
    contract <- dir_contract[k]
    print(contract)
    
    src_dir <- paste0(c(dir_raw,"blockchain",blockchain_name,"supply",contract),collapse="/")
    dest_dir <- paste0(c(dir_data_cur_fact,"blockchain",blockchain_name,"supply",contract),collapse="/")
    
    if(length(list.files(dest_dir)) == 0){
      cat(paste0("File directory NOT found for ",contract,"\n"))
      loopStartDate <- min(as_date(gsub("target_date=","",list.files(src_dir))))
      cat(paste0("Creating directory at ",dest_dir,"\n"))
      dir.create(dest_dir,recursive=T)
    } else {
      cat(paste0("File directory found for ",contract,"\n"))
      if(sel_op_save == 2){
        loopStartDate <- date_load_from
      } else {
        loopStartDate <- max(as.Date(gsub("date=","",list.files(dest_dir))))
      }
      
    }
    cat(paste0("Starting data retrieval from ",loopStartDate,"\n"))
    
    print(paste0("Removing latest day data = ",loopStartDate))
    dest_dir_files_full <- list.files(dest_dir,full.names = T)
    dest_dir_files <- list.files(dest_dir)
    dest_dir_files_idx <- as_date(gsub("date=","",dest_dir_files))
    dest_dir_files_full <- dest_dir_files_full[dest_dir_files_idx>=loopStartDate]
    unlink(dest_dir_files_full,force=T,recursive=T)
    
    src_dir_files <- list.files(src_dir)
    src_dir_files_idx <- as_date(gsub("target_date=","",src_dir_files))
    src_dir_files <- list.files(src_dir,full.names=T,recursive=T)[src_dir_files_idx>=loopStartDate]
    
    print(paste0(c("Begin loop data curation for Blockchain supply",blockchain_name,contract),collapse=" "))
    
    list_res <- 
      foreach(
        x=seq_along(src_dir_files)
        ,.packages=parallelPackages
      ) %dopar% {
        dir <- src_dir_files[x]
        
        raw <- read_parquet(dir) %>% as_tibble(raw) %>%
          inner_join(
            loop_dim_asset
            ,by=c("address"="onchain_address")
          ) %>%
          mutate(
            date = as_date(target_time)
          ) %>%
          select(dim_asset_id,block,supply,date)
        return(raw) 
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
}
stopCluster(cl)