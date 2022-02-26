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

## Blockchain Queries ----
dir_harmony         <- paste0(raw_dir,"/blockchain/harmony/base")
dir_supply          <- paste0(raw_dir,"/blockchain/harmony/supply")
dir_balances        <- paste0(raw_dir,"/blockchain/harmony/balances")
rpc                 <- "https://a.api.s0.t.hmny.io/"

## Get Max Dates ----
maxBlockDate     <- max(as_date(gsub("target_date=","",list.files(dir_harmony))))
maxSupplyDate     <- max(as_date(gsub("target_date=","",list.files(dir_supply)))) 

## Remove On/After Max Block Date From Supply Dir ----
vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir_supply)))>=maxBlockDate
unlink(paste0(dir_supply,"/",list.files(dir_supply)[vctRemoveDate]),force=T,recursive=T)

## Remove On/After Max Block Date From Supply Dir ----
vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir_balances)))>=maxBlockDate
unlink(paste0(dir_balances,"/",list.files(dir_balances)[vctRemoveDate]),force=T,recursive=T)

## Get raw blocks data ----
vctFillDate      <- as_date(gsub("target_date=","",list.files(dir_harmony)))>=maxBlockDate

list_blocks <- lapply(
  # list.files(dir_harmony,recursive = T)[vctFillDate]
  list.files(dir_harmony,recursive = T)
  ,function(x){
    arrow::read_parquet(paste0(c(dir_harmony,x),collapse="/"))   
  }
)
df_blocks <- bind_rows(list_blocks)


## Get Circulating Supply ----
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

list_supply <- foreach(
  x=df_blocks$attempt_block
  ,.packages = parallelPackages
  ,.verbose=T
) %dopar% {

  listRes <- list()
  for(i in 1:nrow(maintenance_pid)){
    address = maintenance_pid[i,]$address
    name    = maintenance_pid[i,]$product_name
    
    supply  = fn_hmyv2_call_totalSupply(address,block=x,rpc=rpc)
    
    listRes[[i]] <- data.frame(address=address,name=name,supply=supply)
  }
  dfRes   <- bind_rows(listRes) %>% mutate(block=x)
     
  return(dfRes)
}
stopCluster(cl)

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
  )

cat("Writing raw supply data to parquet files","\n")
writeSupplyGridSearch<- unique(df_supply[,c("target_date","address")])
l <- 1L
while(l <= nrow(writeSupplyGridSearch)){
  arrow::write_dataset(
    df_supply[
      which(
        df_supply$target_date %in% writeSupplyGridSearch[l:(min(nrow(writeSupplyGridSearch),l+1023L)),]$target_date
        & df_supply$address %in% writeSupplyGridSearch[l:(min(nrow(writeSupplyGridSearch),l+1023L)),]$address
      )
      ,]
    ,dir_supply
    ,format = "parquet"
    ,partitioning = c("target_date","address")
    ,basename_template = paste0(c("harmony_hrc20_",l,"{i}.parquet"),collapse="_")
  )
  l <- l+1024L
}


## Get Balances ----
library(data.table)
tokenBalGrid <- merge(df_blocks[,"attempt_block"],maintenance_account_balance)
tokenBalGrid <- split(tokenBalGrid,seq(nrow(tokenBalGrid)))

cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

start.time <- Sys.time()
list_bal <- foreach(
  # x=df_blocks$attempt_block
  x=tokenBalGrid
  ,.packages = parallelPackages
  ,.verbose=T
) %dopar% {
  
  bal   <- fn_hmyv2_call_balanceOf(x$account_address,x$product_address,rpc=rpc,block=x$attempt_block)
  dfRes <- mutate(x,amount=bal)
            
  return(dfRes)
}
end.time <- Sys.time()
stopCluster(cl)
time.taken <- end.time-start.time
time.taken

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
writeBalGridSearch <- unique(df_bal[,c("target_date","product_address")])
l <- 1L
while(l <= nrow(writeBalGridSearch)){
  arrow::write_dataset(
    df_bal[
      which(
        df_bal$target_date %in% writeBalGridSearch[l:(min(nrow(writeBalGridSearch),l+1023L)),]$target_date
        & df_bal$product_address %in% writeBalGridSearch[l:(min(nrow(writeBalGridSearch),l+1023L)),]$product_address
      )
      ,]
    ,dir_balances
    ,format = "parquet"
    ,partitioning = c("target_date","product_address")
    ,basename_template = paste0(c("harmony_balances_",l,"{i}.parquet"),collapse="_")
  )
  l <- l+1024L
}

## Get LP Data ----
## Includes allocPoints, totalAllocPoints, emission
maintenance_pid
maintenance_account_balance


