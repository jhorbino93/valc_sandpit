library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)

library(foreach)
library(doParallel)

## Blockchain Queries ----
dir_harmony         <- paste0(raw_dir,"/blockchain/harmony/base")
dir_hermes          <- paste0(raw_dir,"/blockchain/harmony/hermes_defi")

rpc              <- "https://a.api.s0.t.hmny.io/"
glbStartTime     <- with_tz(as_datetime("2021-07-01 00:00:00"),"UTC")
maxDirDate       <- max(as.Date(gsub("target_date=","",list.files(dir))))
startTime        <- with_tz(as_datetime(maxDirDate),"UTC")

fn_hmyv2_

fn_unixToTime(content(fn_hmyv2_getBlockByNumber(21048525))$result$timestamp)



test <- fn_hmyv2_call(token_address = "0x8c8dca27e450d7d93fa951e79ec354dce543629e",rpc=rpc,data="0x48cd4cb1")

as.numeric(content(test)$result)

fn_hmyv2_call_startBlock()


list_blocks <- lapply(
  list.files(dir_harmony,recursive = T)
  ,function(x){
    arrow::read_parquet(paste0(c(dir_harmony,x),collapse="/"))   
  }
)

df_blocks <- bind_rows(list_blocks)

