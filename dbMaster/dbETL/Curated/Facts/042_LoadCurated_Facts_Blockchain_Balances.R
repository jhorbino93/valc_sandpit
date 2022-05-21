## Get necessary dims ----
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dir_dim_account   <- paste0(c(dir_data_cur_dim,"dim_account.parquet"),collapse="/")
dir_bridge_account <- paste0(c(dir_data_cur_dim,"bridge_account_product.parquet"),collapse="/")
dir_dim_block_dates <- paste0(c(dir_data_cur_dim,"dim_block_dates.parquet"),collapse="/")


dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)
dim_account       <- read_parquet(dir_dim_account)
bridge_account    <- read_parquet(dir_bridge_account)
dim_block_dates   <- read_parquet(dir_dim_block_dates)

dir_data_raw_blockchain <- paste0(c(dir_raw,"blockchain"),collapse="/")

## Get supply facts ----
dir_fact_blockchain <- paste0(c(dir_data_cur_fact,"blockchain"),collapse="/")
vct_blockchain <- list.files(dir_fact_blockchain)

list_dir_supply <- 
  lapply(
    list.files(dir_fact_blockchain)
    ,function(x){
      paste0(c(dir_fact_blockchain,x,"supply"),collapse="/")
    }
  )

## Get account balance mapping ----
df_account_balances <- 
  dim_account %>%
  inner_join(bridge_account) %>%
  inner_join(
    select(dim_asset,dim_asset_id,asset_name,ticker_name,onchain_network)
    ,by=c("product_dim_asset_id"="dim_asset_id","network"="onchain_network")
  )

## Initiate parallelism ----
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)
parallelPackages=c("arrow","tidyverse","lubridate")

## Start loop ----
for(i in seq_along(vct_blockchain)){
  print(i)
  
  loop_blockchain <- vct_blockchain[i]
  loop_dir_base   <- paste0(c(dir_data_cur_fact,"blockchain",loop_blockchain),collapse="/")
  loop_dir_supply <- paste0(c(loop_dir_base,"supply"),collapse="/")
  
  loop_dir_account_raw <- paste0(c(dir_data_raw_blockchain,loop_blockchain,"balances"),collapse="/")
  
  loop_ref_tokens <- dim_asset %>%
    filter(
      ticker_src_network == paste0(str_to_title(loop_blockchain)," Network")
      ,!asset_type_l2 %in% c("LP","CEX ticker")
    ) %>% pull(ticker_name)
  
  loop_vct_supply_addresses <- list.files(loop_dir_supply)
  loop_vct_supply_addresses <- loop_vct_supply_addresses[which(loop_vct_supply_addresses %in% loop_ref_tokens)]
  
  
  for(k in seq_along(loop_vct_supply_addresses)){
    ## Consolidates all holders into 1 file
    ## Consider having tree structure down to asset > holder > date level. This will allow greater control
    print(k)
    
    sel_address <- loop_vct_supply_addresses[k]
    ## Unique to not duplicate due to MM supply/borrow entries
    vct_accounts <- df_account_balances %>% filter(ticker_name == sel_address,!is.na(account_address)) %>% pull(account_address) %>% unique()

    loop_dir_supply_address <- paste0(c(loop_dir_supply,sel_address),collapse="/")
    dest_dir <- paste0(c(loop_dir_base,"balances",sel_address),collapse="/")
    
    ## Get dim_account_id
    loop_df_account <- 
      df_account_balances %>%
      filter(
        network == paste0(str_to_title(loop_blockchain)," Network")
        ,ticker_name == sel_address
        ,account_name != "Other"
      )
    
    loop_df_account_other <- 
      df_account_balances %>%
      filter(
        network == paste0(str_to_title(loop_blockchain)," Network")
        ,ticker_name == sel_address
        ,account_name == "Other"
      )
    
    ## Check if supply directory exists
    ## Perform start date logic
    if(length(list.files(dest_dir)) == 0){
      cat(paste0("File directory NOT found for ",sel_address,"\n"))
      loopStartDate <- min(as_date(gsub("date=","", list.files(loop_dir_supply_address))))
      cat(paste0("Creating directory at ",dest_dir,"\n"))
      dir.create(dest_dir,recursive=T)
    } else {
      cat(paste0("File directory found for ",sel_address,"\n"))
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
    
    ## Loop for each date in supply and rebuild balances from holders
    src_dir_files <- list.files(loop_dir_supply_address)
    src_dir_files_idx <- as_date(gsub("date=","",src_dir_files))
    src_dir_files_idx <- src_dir_files_idx[src_dir_files_idx>=loopStartDate]
    
    ## Parallel construct holders per supply address
    list_res <-
      foreach(
        x = src_dir_files_idx
        ,.packages=parallelPackages
      ) %dopar% {
        
        sel_date = x
        
        ## Get supply for loop date
        full_src_supply_dir <- paste0(c(loop_dir_supply_address,paste0("date=",sel_date)),collapse="/")
        df_supply <- read_parquet(list.files(full_src_supply_dir,full.names=T)) %>% as_tibble()
        
        
        ## Get balances for loop date
        list_accounts <- list()
        if(length(vct_accounts)!=0L){
          for(m in seq_along(vct_accounts)){
            print(m)
            sel_account <- vct_accounts[m]
            full_src_accounts_dir <- paste0(c(loop_dir_account_raw,sel_account,paste0("target_date=",sel_date)),collapse="/")
            
            ## Check if any of the account dirs have files/or real
            if(dir.exists(full_src_accounts_dir)){
              res_account <- read_parquet(list.files(full_src_accounts_dir,full.names=T)) %>%
                filter(product_address == sel_address) %>%
                select(attempt_block,account_address,account_type,amount)
            } else {
              res_account <- tibble(
                attempt_block = double()
                ,account_address = character()
                ,account_type = character()
                ,amount = double()
              )
            }
            list_accounts[[m]] <- res_account
          }
        } else {
          list_accounts[[1]] <- tibble(
            attempt_block = double()
            ,account_address = character()
            ,account_type = character()
            ,amount = double()
          )
        }
        
        vct_accounts_nrow <- unlist(lapply(list_accounts,nrow))
        
        df_accounts <- bind_rows(list_accounts)
        
        df_account_other <- 
          df_supply %>%
          left_join(
            df_accounts %>%
              group_by(attempt_block) %>%
              summarise(
                accounted_balance = sum(amount,na.rm=T)
              )
            ,by=c("block"="attempt_block")
          ) %>%
          mutate(
            amount = supply - coalesce(accounted_balance,0)
          ) %>%
          select(dim_asset_id,block,amount)
        
        df_balances <-
          df_accounts %>%
          inner_join(loop_df_account,by=c("account_address","account_type")) %>%
          rename(
            dim_asset_id=product_dim_asset_id
            ,block = attempt_block
          ) %>%
          select(block,dim_account_id,dim_asset_id,amount) 
        
        df_balances_other <- 
          df_account_other %>%
          inner_join(
            loop_df_account_other,by=c("dim_asset_id"="product_dim_asset_id")
          ) %>%
          select_at(colnames(df_balances))
        
        df_res <- bind_rows(df_balances,df_balances_other) %>%
          inner_join(
            dim_block_dates %>% filter(blockchain == loop_blockchain) %>% select(date,attempt_block)
            ,by=c("block"="attempt_block")
          )
        return(df_res)
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
        ,dest_dir
        ,format = "parquet"
        ,partitioning = c("date")
      )
      l <- l+1008L
    }
  }
}
stopCluster(cl)
