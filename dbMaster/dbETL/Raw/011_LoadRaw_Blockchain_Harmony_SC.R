library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)

library(foreach)
library(doParallel)

## Parallel Parameters ----
parallelPackages=c("httr","jsonlite","ether","dplyr","lubridate")

## Environment ----
dir_harmony         <- paste0(raw_dir,"/blockchain/harmony/base")
dir_supply          <- paste0(raw_dir,"/blockchain/harmony/supply")
dir_balances        <- paste0(raw_dir,"/blockchain/harmony/balances")
dir_lp              <- paste0(raw_dir,"/blockchain/harmony/lp")
rpc                 <- "https://a.api.s0.t.hmny.io/"

## Get raw blocks data ----
list_blocks <- lapply(
  list.files(dir_harmony,recursive = T)
  ,function(x){
    arrow::read_parquet(paste0(c(dir_harmony,x),collapse="/"))   
  }
)
df_blocks <- bind_rows(list_blocks)

## Get Circulating Supply ----
for(i in 1:nrow(maintenance_pid)){
  print(i)
  
  loop_df         <- maintenance_pid[i,]
  address         <- loop_df$address
  name            <- loop_df$product_name
  
  dir             <- paste0(c(dir_supply,address),collapse="/")
  
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir,"\n"))
    
    minBlockSearch <- fn_getCodeStartBlock(address=address)$res
    loopStartDate  <- filter(df_blocks,attempt_block >= minBlockSearch) %>%
                      mutate(target_time = as_date(target_time)) %>%
                      pull() %>% min()
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",address,"\n"))
    loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)
  
  cores <- detectCores()
  cl <- makeCluster(cores[1]-1)
  registerDoParallel(cl)
  
  list_supply <- foreach(
    x=filter(df_blocks,as_date(target_time)>=loopStartDate) %>% pull(attempt_block)
    ,.packages = parallelPackages
  ) %dopar% {
    
    supply  <- fn_hmyv2_call_totalSupply(address,block=x,rpc=rpc,autoconv = F)
  
    dfRes   <- data.frame(block=x,supply=supply)
    
    return(dfRes)
  }
  stopCluster(cl)  
  
  dec <- fn_hmyv2_call_decimals(address,rpc=rpc)
  
  df_supply <- 
    bind_rows(
      list_supply
    ) %>%
    filter(!is.na(supply)) %>% as_tibble() %>%
    inner_join(
      select(df_blocks,attempt_block,target_time)
      ,by=c("block" = "attempt_block")
    ) %>%
    mutate(
      target_date = as_date(target_time)
      ,address=address
      ,name=name
      ,supply=supply/(10^dec)
    )
  
  cat("Writing raw supply data to parquet files","\n")
  writeSupplyGridSearch<- unique(df_supply[,c("target_date")])
  l <- 1L
  while(l <= nrow(writeSupplyGridSearch)){
    arrow::write_dataset(
      df_supply[
        which(
          df_supply$target_date %in% writeSupplyGridSearch[l:(min(nrow(writeSupplyGridSearch),l+1023L)),]$target_date
        )
        ,]
      ,dir
      ,format = "parquet"
      ,partitioning = c("target_date")
      ,basename_template = paste0(c("harmony_supply_",address,"_",l,"_{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}


## Get Balances ----
vct_min_token_dates <- 
  lapply(
    setNames(list.files(dir_supply),list.files(dir_supply))
    ,function(x){
      res = min(as_date(gsub("target_date=","",list.files(paste0(c(dir_supply,x),collapse="/")))))
      res
    }
  ) %>% do.call("c",.)
df_min_token_dates <-
  data.frame(
    address = names(vct_min_token_dates)
    ,min_date = vct_min_token_dates
  )

account_balance_dist <- unique(maintenance_account_balance$product_address)
for(i in seq_along(account_balance_dist)){
  print(i)
  
  sel_product_address <- account_balance_dist[i]
  loop_df         <- filter(maintenance_account_balance,product_address == sel_product_address)
  
  dir             <- paste0(c(dir_balances,sel_product_address),collapse="/")
  
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir,"\n"))
    loopStartDate <- min(as_date(df_blocks$target_time))
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",sel_product_address))
    loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)

  tokenBalGrid <- merge(
                    filter(df_blocks,target_time >= loopStartDate) %>% select(attempt_block,target_time)
                    ,loop_df
                  ) %>% inner_join(df_min_token_dates,by=c("account_address"="address")) %>%
                  filter(target_time >= min_date) %>% select(-c(min_date,target_time))
  tokenBalGrid <- split(tokenBalGrid,seq(nrow(tokenBalGrid)))
  
  cores <- detectCores()
  cl <- makeCluster(cores[1]-1)
  registerDoParallel(cl)
  
  start.time <- Sys.time()
  list_bal <- foreach(
    x=tokenBalGrid
    ,.packages = parallelPackages
  ) %dopar% {
    
    bal   <- fn_hmyv2_call_balanceOf(x$account_address,x$product_address,rpc=rpc,block=x$attempt_block,autoconv = F)
    dfRes <- mutate(x,amount=bal)
    
    return(dfRes)
  }
  stopCluster(cl)
  
  dec <- fn_hmyv2_call_decimals(address,rpc=rpc)
  
  df_bal <- bind_rows(list_bal) %>% as_tibble() %>% 
    filter(!is.na(amount),amount>0) %>%
    inner_join(
      select(df_blocks,attempt_block,target_time)
      ,by="attempt_block"
    ) %>%
    mutate(
      target_date = as_date(target_time)
      ,amount = amount/(10^dec)
    )
  
  cat("Writing raw balances data to parquet files","\n")
  writeBalGridSearch <- unique(df_bal[,c("target_date")])
  l <- 1L
  while(l <= nrow(writeBalGridSearch)){
    arrow::write_dataset(
      df_bal[
        which(
          df_bal$target_date %in% writeBalGridSearch[l:(min(nrow(writeBalGridSearch),l+1023L)),]$target_date
        )
        ,]
      ,dir
      ,format = "parquet"
      ,partitioning = c("target_date")
      ,basename_template = paste0(c("harmony_balances_",sel_product_address,"_",l,"_{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}

## Get LP Data ----
## Includes allocPoints, totalAllocPoints, emission
ref_lp <- filter(maintenance_pid,product_type == "LP")
for(i in 1:nrow(ref_lp)){
  print(i)
  
  name        <- ref_lp[i,]$product_name
  address     <- ref_lp[i,]$address
  pid         <- ref_lp[i,]$pid
  platform    <- ref_lp[i,]$platform
  masterchef  <- filter(maintenance_masterchef,platform==platform) %>% pull(masterchef_address)
  
  dir         <- paste0(c(dir_lp,address),collapse="/")
  
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir,"\n"))
    loopStartDate <- min(as_date(df_blocks$target_time))
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",address))
    loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)
  
  ## Begin parallel retrieval
  startBlock <- fn_hmyv2_Call_startBlock(masterchef)
  tmp_blocks <- filter(df_blocks,as_date(target_time) >= loopStartDate, attempt_block >= startBlock)
  tmp_blocks <- split(tmp_blocks,seq(nrow(tmp_blocks)))
  
  cores <- detectCores()
  cl <- makeCluster(cores[1]-1)
  registerDoParallel
  
  list_lp <- foreach(
    x = tmp_blocks
    ,.packages = parallelPackages
  ) %dopar% {
    alloc_points        <- fn_poolInfo_allocPoints(
                            content(fn_hmyv2_call_poolInfo(masterchef,pid=pid,block=x$attempt_block,rpc=rpc))$result
                          )
    total_alloc_points  <- fn_hmyv2_call_totalAllocPoints(masterchef,.block=x$attempt_block)
    emission            <- fn_bnToReal(
                            as.numeric(
                              content(
                                fn_hmyv2_call_emissionPerBlock(masterchef,block=x$attempt_block,rpc=rpc)
                              )$result
                            )
                          )
    dfRes             <- data.frame(
                            attempt_block       = x$attempt_block
                            ,alloc_points       = alloc_points
                            ,total_alloc_points = total_alloc_points
                            ,emission           = emission
                          )
    return(dfRes)
  }
  stopCluster(cl)
  
  df_lp <- bind_rows(list_lp) %>% as_tibble() %>%
    filter(!is.na(alloc_points) & !is.na(total_alloc_points) & !is.na(emission)) %>%
    inner_join(
      select(df_blocks,attempt_block,target_time)
      ,by="attempt_block"
    ) %>%
    mutate(
      target_date = as_date(target_time)
      ,address = address
      ,masterchef_address = masterchef
    )
  
  cat("Writing raw balances data to parquet files","\n")
  writeLPGridSearch <- unique(df_lp[,c("target_date","address","masterchef_address")])
  l <- 1L
  while(l <= nrow(writeLPGridSearch)){
    arrow::write_dataset(
      df_lp[
        which(
          df_lp$target_date %in% writeLPGridSearch[l:(min(nrow(writeLPGridSearch),l+1023L)),]$target_date
          & df_lp$address %in% writeLPGridSearch[l:(min(nrow(writeLPGridSearch),l+1023L)),]$address
          & df_lp$masterchef_address %in% writeLPGridSearch[l:(min(nrow(writeLPGridSearch),l+1023L)),]$masterchef_address
        )
        ,]
      ,dir
      ,format = "parquet"
      ,partitioning = c("target_date","masterchef_address")
      ,basename_template = paste0(c("harmony_lp_info_",l,"{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}
  
  
  
  
  
  
