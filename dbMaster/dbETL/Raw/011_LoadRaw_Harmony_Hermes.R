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
dir_hrc20           <- paste0(raw_dir,"/blockchain/harmony/hrc20")
rpc              <- "https://a.api.s0.t.hmny.io/"

## Get Max Dates ----
maxBlockDate     <- max(as_date(gsub("target_date=","",list.files(dir_harmony))))
maxHrc20Date     <- max(as_date(gsub("target_date=","",list.files(dir_hrc20)))) 

## Remove On/After Max Block Date From HRC Dir ----
vctRemoveDate    <- as_date(gsub("target_date=","",list.files(dir_hrc20)))>=maxBlockDate
unlink(paste0(dir_hrc20,"/",list.files(dir_hrc20)[vctRemoveDate]),force=T,recursive=T)

## Get raw blocks data ----
vctFillDate      <- as_date(gsub("target_date=","",list.files(dir_harmony)))>=maxBlockDate

list_blocks <- lapply(
  list.files(dir_harmony,recursive = T)[vctFillDate]
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
supply_grid_search <- unique(df_supply[,c("target_date","address")])
l <- 1L
while(l <= length(vct_target_date)){
  arrow::write_dataset(
    df_supply[
      which(
        df_supply$target_date %in% supply_grid_search[l:(min(nrow(supply_grid_search),l+1023L)),]$target_date
        & df_supply$address %in% supply_grid_search[l:(min(nrow(supply_grid_search),l+1023L)),]$address
      )
      ,]
    ,dir_hrc20
    ,format = "parquet"
    ,partitioning = c("target_date","address")
    ,basename_template = paste0(c("harmony_hrc20_",l,"{i}.parquet"),collapse="_")
  )
  l <- l+1024L
}


## Get Balances ----

