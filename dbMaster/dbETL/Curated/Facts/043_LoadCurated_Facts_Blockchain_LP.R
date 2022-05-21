## Get necessary dims ----
dir_dim_datetime   <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval   <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset      <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dir_dim_lp         <- paste0(c(dir_data_cur_dim,"dim_lp.parquet"),collapse="/")
dir_dim_masterchef <- paste0(c(dir_data_cur_dim,"dim_masterchef.parquet"),collapse="/")
dir_dim_account    <- paste0(c(dir_data_cur_dim,"dim_account.parquet"),collapse="/")
dir_bridge_account <- paste0(c(dir_data_cur_dim,"bridge_account_product.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)
dim_lp            <- read_parquet(dir_dim_lp)
dim_masterchef    <- read_parquet(dir_dim_masterchef)
dim_account       <- read_parquet(dir_dim_account)
bridge_account    <- read_parquet(dir_bridge_account)

dir_data_raw_blockchain <- paste0(c(dir_raw,"blockchain"),collapse="/")


regex_find_date = "(.*?)(\\d{4}-)(\\d{1,2}-)(\\d{1,2})(.*)"
dir_blockchain <- paste0(c(dir_raw,"blockchain"),collapse="/")
dest_dir <- paste0(c(dir_data_cur_dim,"dim_block_dates.parquet"),collapse="/")

cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)
parallelPackages=c("arrow","tidyverse","lubridate")

for(i in seq_along(list.files(dir_blockchain))){
  print(i)
  
  loop_blockchain <- list.files(dir_blockchain)[i]
  loop_dir_base   <- paste0(c(dir_data_cur_fact,"blockchain",loop_blockchain),collapse="/")
  loop_dir_lp     <- paste0(c(loop_dir_base,"lp"),collapse="/")
  
  loop_dir_lp_raw <- paste0(c(dir_data_raw_blockchain,loop_blockchain,"lp"),collapse="/")
  
  loop_vct_lp_address <- list.files(loop_dir_lp_raw)
  
  for(k in seq_along(loop_vct_lp_address)){
    print(k)
    
    loop_sel_address <- loop_vct_lp_address[k]
    
    loop_src_dir <- paste0(c(loop_dir_lp_raw,loop_sel_address),collapse="/")
    loop_dest_dir <- paste0(c(loop_dir_lp,loop_sel_address),collapse="/")
    
    if(length(list.files(loop_dest_dir)) == 0){
      cat(paste0("File directory NOT found for ",loop_sel_address,"\n"))
      loopStartDate <- min(as_date(gsub("target_date=","",list.files(loop_src_dir))))
      cat(paste0("Creating directory at ",loop_dest_dir,"\n"))
      dir.create(loop_dest_dir,recursive=T)
    } else {
      cat(paste0("File directory found for ",loop_sel_address,"\n"))
      if(sel_op_save == 2){
        loopStartDate <- date_load_from
      } else {
        loopStartDate <- max(as.Date(gsub("date=","",list.files(loop_dest_dir))))
      }
      
    }
    cat(paste0("Starting data retrieval from ",loopStartDate,"\n"))
    
    
    print(paste0("Removing latest day data = ",loopStartDate))
    dest_dir_files_full <- list.files(loop_dest_dir,full.names = T)
    dest_dir_files <- list.files(loop_dest_dir)
    dest_dir_files_idx <- as_date(gsub("date=","",dest_dir_files))
    dest_dir_files_full <- dest_dir_files_full[dest_dir_files_idx>=loopStartDate]
    unlink(dest_dir_files_full,force=T,recursive=T)
    
    src_dir_files <- list.files(loop_src_dir)
    src_dir_files_idx <- as_date(gsub("target_date=","",src_dir_files))
    src_dir_files <- list.files(loop_src_dir,full.names=T,recursive=T)[src_dir_files_idx>=loopStartDate]
    
    if(length(src_dir_files)>0){
      print(paste0(c("Begin loop data curation for Blockchain LP data",loop_blockchain,loop_sel_address),collapse=" "))
      
      list_res <-
        foreach(
          x=seq_along(src_dir_files)
          ,.packages=parallelPackages
        ) %dopar% {
          dir <- src_dir_files[x]
          
          raw <- read_parquet(dir) %>% as_tibble(raw)
          
          res <- raw %>%
            inner_join(
              filter(dim_account,network == paste0(str_to_title(loop_blockchain)," Network"),account_type=="LP") %>%
                select(account_address,dim_account_id)
              ,by=c("address"="account_address")
            ) %>%
            mutate(
              date = as_date(target_time)
            ) %>%
            rename(
              datetime = target_time
            ) %>%
            select(
              datetime,date,attempt_block,dim_account_id,alloc_points,total_alloc_points,emission
            )
          
          return(res)
          
        }
      resOut <- bind_rows(list_res)
      
      ## Write parquet file ----
      cat(paste0("Writing to parquet files","\n"))
      df_search <- distinct(resOut,date) %>% arrange(date)
      l <- 1L
      while(l <= nrow(df_search)){
        ref <- slice(df_search,l:(l+1007L))
        arrow::write_dataset(
          inner_join(resOut,ref)
          ,loop_dest_dir
          ,format = "parquet"
          ,partitioning = c("date")
        )
        l <- l+1008L
      }
    } else {print("No valid files to process.")}
  }
}
stopCluster(cl)