## Parallel Parameters ----
parallelPackages=c("httr","jsonlite","ether","dplyr","lubridate")

## Environment ----
dir_harmony         <- paste0(dir_raw,"/blockchain/harmony/base")
dir_supply          <- paste0(dir_raw,"/blockchain/harmony/supply")
dir_balances        <- paste0(dir_raw,"/blockchain/harmony/balances")
dir_lp              <- paste0(dir_raw,"/blockchain/harmony/lp")
rpc                 <- "https://a.api.s0.t.hmny.io/"


cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

## Get raw blocks data ----
list_blocks <- lapply(
  list.files(dir_harmony,recursive = T)
  ,function(x){
    arrow::read_parquet(paste0(c(dir_harmony,x),collapse="/"))   
  }
)
df_blocks <- bind_rows(list_blocks)

base011_obj <- c(ls(),"base011_obj")

## Get Circulating Supply ----

pid_df <-
  maintenance_pid  %>%
  filter(
    product_type %in% c("LP","HRC20")
    ,!friendly_alias %in% c("tricrypto") # Curve tricrypto is a contract not token
  )

for(i in 1:nrow(pid_df)){
  print(i)
  
  loop_df         <- pid_df[i,]
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
    if(sel_op_save == 2){
      loopStartDate <- date_load_from
    } else {
      loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
    }
    
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)
  
  tmp_block_search <- filter(df_blocks,as_date(target_time)>=loopStartDate) %>% pull(attempt_block)
  
  list_supply <- foreach(
    x=tmp_block_search
    ,.packages = parallelPackages
  ) %dopar% {
    
    supply  <- fn_hmyv2_call_totalSupply(address,block=x,rpc=rpc,autoconv = T)
  
    dfRes   <- data.frame(block=x,supply=supply)
    
    return(dfRes)
  }
  
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
rm(list=setdiff(ls(),base011_obj))

## Get Balances Main ----
vct_min_token_dates <- 
  lapply(
    setNames(list.files(dir_supply),list.files(dir_supply))
    ,function(x){
      res = min(as_date(gsub("target_date=","",list.files(paste0(c(dir_supply,x),collapse="/")))))

      if(is.na(res) | is.infinite(res)){
        res = filter(maintenance_account_balance,account_address == x) %>% slice(1) %>% pull(min_date)
      }
      res
    }
  ) %>% do.call("c",.)

df_min_token_dates <-
  tibble(
    address = names(vct_min_token_dates)
    ,min_date = as.Date(vct_min_token_dates)
  )

account_balance_dist <- 
  filter(
    maintenance_account_balance
     ,!platform %in% c("tranquil finance")
     ,!account_type %in% c("MM Borrow","MM Supply")
     ,network == "Harmony Network"
  ) %>% pull(account_address) %>% unique()

for(i in seq_along(account_balance_dist)){
  print(i)
  
  sel_account_address <- account_balance_dist[i]
  loop_df         <- filter(maintenance_account_balance,account_address == sel_account_address) %>%
                      select(-min_date) %>%
                      mutate(max_date = coalesce(max_date,max(as_date(df_blocks$target_time))+days(1)))
  
  dir             <- paste0(c(dir_balances,sel_account_address),collapse="/")
  
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir," or is empty.\n"))
    loopStartDate <- min(as_date(df_blocks$target_time))
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",sel_account_address))
    if(sel_op_save == 2){
      loopStartDate <- date_load_from
    } else {
      loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
    }
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)

  tokenBalGrid <- merge(
                    filter(df_blocks,target_time >= loopStartDate) %>% select(attempt_block,target_time)
                    ,loop_df
                  ) %>% inner_join(df_min_token_dates,by=c("product_address"="address")) %>%
                  filter(target_time >= min_date,target_time <= max_date) %>% select(-c(min_date,max_date,target_time))
  tokenBalGrid <- split(tokenBalGrid,seq(nrow(tokenBalGrid)))
  
  list_bal <- foreach(
    x=tokenBalGrid
    ,.packages = parallelPackages
  ) %dopar% {
    
    bal   <- fn_hmyv2_call_balanceOf(x$product_address,x$account_address,rpc=rpc,block=x$attempt_block,autoconv = T)
    dfRes <- mutate(x,amount=bal)
    
    return(dfRes)
  }
  
  df_bal <- bind_rows(list_bal) %>% as_tibble() %>% 
    filter(!is.na(amount),amount>0) %>%
    inner_join(
      select(df_blocks,attempt_block,target_time)
      ,by="attempt_block"
    ) %>%
    mutate(
      target_date = as_date(target_time)
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
      ,basename_template = paste0(c("harmony_balances_",sel_account_address,"_",l,"_{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}

## Get Balances Borrow/Lending ----
account_balance_mm <- 
  filter(
    maintenance_account_balance
    ,platform %in% c("tranquil finance")
    ,account_type %in% c("MM Borrow","MM Supply")
    ,network == "Harmony Network"
  ) %>% distinct(account_address,network)

for(i in 1:nrow(account_balance_mm)){
  print(i)
  
  sel_account_address <- account_balance_mm$account_address[i]
  sel_network         <- account_balance_mm$network[i]
  # sel_account_type    <- account_balance_mm$account_type[i]
  
  loop_df         <- filter(maintenance_account_balance,account_address == sel_account_address,network == sel_network) %>% 
                      select(-min_date) %>%
                      mutate(max_date = coalesce(max_date,max(as_date(df_blocks$target_time))+days(1)))
  
  sel_underlying <- loop_df$product_address[1] ## Should be unique within all, otherwise need to adj logic
  
  sel_dec <- fn_hmyv2_call_decimals(sel_underlying)
  
  dir             <- paste0(c(dir_balances,sel_account_address),collapse="/")
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir," or is empty./n"))
    loopStartDate <- min(as_date(df_blocks$target_time))
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",sel_account_address))
    if(sel_op_save == 2){
      loopStartDate <- date_load_from
    } else {
      loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
    }
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)
  
  tokenBalGrid <- merge(
    filter(df_blocks,target_time >= loopStartDate) %>% select(attempt_block,target_time)
    ,loop_df
  ) %>% inner_join(df_min_token_dates,by=c("product_address"="address")) %>%
    filter(target_time >= min_date,target_time <= max_date) %>% select(-c(min_date,max_date,target_time))
  tokenBalGrid <- split(tokenBalGrid,seq(nrow(tokenBalGrid)))
  
  list_bal <- foreach(
    x = tokenBalGrid
    ,.packages=parallelPackages
  ) %dopar% {
    if(x$account_type == "MM Supply"){
      bal <- fn_hmyv2_call_balanceOf(x$product_address,x$account_address,rpc=rpc,block=x$attempt_block,autoconv=T,dec=sel_dec)
    } else if (x$account_type == "MM Borrow"){
      bal <- fn_hmyv2_call_totalBorrows(x$account_address,rpc=rpc,block=x$attempt_block,autoconv=T,dec=sel_dec)
    }
    
    dfRes <- mutate(x,amount=bal)
    
  }
  
  # Split into supply/borrow. Alternative could be pivot operation.
  tmpRes <- bind_rows(list_bal) %>% as_tibble()
  supplyRes <- filter(tmpRes,account_type == "MM Supply")
  borrowRes <- filter(tmpRes,account_type == "MM Borrow")
  
  # Get TOTAL supply 
  supplyRes <- 
    left_join(
      supplyRes
      ,select(borrowRes,c("attempt_block","platform","account_name","account_address","product_address","network","amount"))
      ,by=c("attempt_block","platform","account_name","account_address","product_address","network")
    ) %>%
    mutate(amount = amount.x+amount.y) %>%
    select(-c("amount.x","amount.y"))
  
  # Turn borrow to negative
  borrowRes <-
    borrowRes %>%
    mutate(
      amount = -1*amount
    )
  
  # Combine
  df_bal <- bind_rows(supplyRes,borrowRes) %>%
    filter(!is.na(amount)) %>%
    inner_join(
      select(df_blocks,attempt_block,target_time)
      ,by="attempt_block"
    ) %>%
    mutate(
      target_date = as_date(target_time)
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
      ,basename_template = paste0(c("harmony_balances_",sel_account_address,"_",l,"_{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
  
}
rm(list=setdiff(ls(),base011_obj))

## Get LP Data ----
## Includes allocPoints, totalAllocPoints, emission

ref_lp <- filter(maintenance_pid,product_type == "LP",!is.na(pid))
for(i in 1:nrow(ref_lp)){
  print(i)
  
  ## Get core attributes
  name             <- ref_lp[i,]$product_name
  address          <- ref_lp[i,]$address
  pid              <- ref_lp[i,]$pid
  sel_platform     <- ref_lp[i,]$masterchef_platform
  sel_network      <- ref_lp[i,]$network
  end_date         <- coalesce(ref_lp[i,]$lp_end_date,as_date(refTime+days(1)))
  masterchef       <- filter(maintenance_masterchef,platform==sel_platform,network == sel_network) %>% pull(masterchef_address)
  
  
  ## Get ABIs
  masterchef_abi   <- maintenance_masterchef_functions[which(maintenance_masterchef_functions$masterchef_address == masterchef),]
  sel_pool_info_fn <- masterchef_abi[which(masterchef_abi$fn_desc=="Pool Info"),]$fn_abi
  sel_total_farming_alloc_fn <- masterchef_abi[which(masterchef_abi$fn_desc=="Total Farming Allocation"),]$fn_abi
  sel_farming_emission_vol_fn <- masterchef_abi[which(masterchef_abi$fn_desc=="Total Emission Volume"),]$fn_abi
  sel_farming_multiplier_fn <- masterchef_abi[which(masterchef_abi$fn_desc=="Emission Multiplier"),]$fn_abi
  
  dir         <- paste0(c(dir_lp,address),collapse="/")
  
  if(length(list.files(dir))==0){
    cat(paste0("File directory NOT found for ",dir,"\n"))
    loopStartDate <- min(as_date(df_blocks$target_time))
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",address))
    if(sel_op_save == 2){
      loopStartDate <- date_load_from
    } else {
      loopStartDate <- max(as.Date(gsub("target_date=","",list.files(dir))))
    }
    
  }
  
  vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir)))>=loopStartDate
  unlink(paste0(dir,"/",list.files(dir)[vctRemoveDate]),force=T,recursive=T)
  
  ## Begin parallel retrieval
  startBlock <- if(sel_platform == "plutus"){
    fn_hmyv2_call_startBlock(masterchef)
  } else if (sel_platform == "defi kingdoms"){
    fn_hmyv2_call_START_BLOCK(masterchef)
  }
  tmp_blocks <- filter(df_blocks,as_date(target_time) >= loopStartDate, attempt_block >= startBlock, as_date(target_time) <= end_date)
  # tmp_blocks <- split(tmp_blocks,1:max(1,nrow(tmp_blocks)))
  tmp_blocks <- tmp_blocks$attempt_block
  
  if(length(tmp_blocks)>0){
    list_lp <- foreach(
      x = tmp_blocks
      ,.packages = parallelPackages
    ) %dopar% {
      res_alloc_points <- fn_poolInfo_allocPoints(
        content(
          fn_hmyv2_call_poolInfo(masterchef,pid=pid,abi=sel_pool_info_fn,block=x)
        )$result
      )
      
      res_total_alloc_points <- fn_hmyv2_call_totalAllocPoints(masterchef,.data=sel_total_farming_alloc_fn,.block = x,.rpc=rpc)
      
      if(sel_platform == "plutus"){
        res_emission <- fn_bnToReal(
          as.numeric(
            content(
              fn_hmyv2_call_emissionPerBlock(masterchef,data = sel_farming_emission_vol_fn,block=block,rpc=rpc)
            )$result
          )
        )
      } else if (sel_platform == "defi kingdoms"){
        res_base_emission <- as.numeric(
          content(
            fn_hmyv2_call_emissionPerBlock(masterchef,data=sel_farming_emission_vol_fn,block=x,rpc=rpc)
          )$result
        )/1e18
        
        current_epoch <- fn_get_dfk_epoch(x)
        res_farming_multiplier <- fn_hmyv2_call_REWARD_MULTIPLIER(masterchef,current_epoch,sel_farming_multiplier_fn,.block=x)
        res_emission <- res_base_emission*res_farming_multiplier
      }
      
      
      
      dfRes             <- data.frame(
        attempt_block       = x
        ,alloc_points       = res_alloc_points
        ,total_alloc_points = res_total_alloc_points
        ,emission           = res_emission
      )
      return(dfRes)
    }
  
    
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
}
rm(list=setdiff(ls(),base011_obj))

stopCluster(cl)    
  
  
  
  
  
