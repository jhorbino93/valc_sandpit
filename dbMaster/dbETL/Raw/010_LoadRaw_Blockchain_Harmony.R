library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)

library(foreach)
library(doParallel)

## Blockchain Queries ----
dir              <- paste0(raw_dir,"/blockchain/harmony")

parallelPackages=c("httr","jsonlite","ether","dplyr","lubridate")
parallelExport = c("fn_unixToTime","fn_hmyv2_getBlockByNumber","fn_getClosestBlock")

rpc              <- "https://a.api.s0.t.hmny.io/"
glbStartTime     <- with_tz(as_datetime("2021-07-01 00:00:00"),"UTC")
maxDirDate       <- max(as.Date(gsub("target_date=","",list.files(dir))))
startTime        <- with_tz(as_datetime(maxDirDate),"UTC")

## Remove latest day data
unlink(paste0(dir,"/target_date=",maxDirDate),force=T,recursive=T) 

## Get latest block data
currentBlock     <- content(fn_hmyv2_getBlock(rpc=rpc))$result
currentBlockTime <- fn_unixToTime(content(fn_hmyv2_getBlockByNumber(currentBlock))$result$timestamp)
vct_time         <- seq(startTime,currentBlockTime,by="hour")


## Start parallel retrieval
start.time <- Sys.time()
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
on.exit(stopCluster(cl))
registerDoParallel(cl)

list_blocks <- foreach(
  x=vct_time
  ,.packages=parallelPackages
) %dopar% {
  
  target <- x
  print(target)
  
  fn_getClosestBlock(currentBlock,target,attempts=6)  
}
stopCluster(cl)
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

resOut <- lapply(
  list_blocks
  ,function(x){
    data.frame(
      target_time = x$target
      ,attempt_time = x$attempt_time
      ,attempt_block = x$attempt_block
      ,avg_block_time = x$avg_block_time
    )
  }
) %>% bind_rows() %>% as_tibble() %>%
  mutate(
    diff_sec = interval(target_time,attempt_time) %/% seconds(1)
    ,diff_min = diff_sec/60
    ,target_date = as_date(target_time)
  ) 

cat(paste0("Writing to parquet files","\n"))
vct_target_date <- unique(resOut$target_date)
l <- 1L
while(l <= length(vct_target_date)){
  arrow::write_dataset(
    resOut[which(resOut$target_date %in% vct_target_date[l:(min(length(vct_target_date),l+1023L))]),]
    ,dir
    ,format = "parquet"
    ,partitioning = "target_date"
    ,basename_template = paste0(c("harmony_",l,"{i}.parquet"),collapse="_")
  )
  l <- l+1024L
}