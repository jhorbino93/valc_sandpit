library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)

library(foreach)
library(doParallel)

## Initialise Environment ----
options(scipen = 999)

## Load Functions
url_r_misc_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/misc_functions.R"
source_url(url_r_misc_fn)

url_r_hmy_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/hmy_functions.R"
source_url(url_r_hmy_fn)

## Local data directory
raw_dir <- "C:/Users/jehor/Documents/GitHub/Hermes/dbMaster/dbData/001Raw"

## Github directory
base_github <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster"
ref_dir <- "/dbReference"

## Get Reference Data ----
maintenance_dim_ticker  <- read.csv(paste0(base_github,ref_dir,"/maintenance_dim_ticker.csv"),stringsAsFactors = F)
maintenance_dim_headers <- read.csv(paste0(base_github,ref_dir,"/maintenance_dim_headers.csv"),stringsAsFactors = F)
refTime                 <- as.POSIXct(format(Sys.time()),tz="UTC")
refTimeUnix             <- fnConvTimeToUnix(refTime)

## CEX BINANCE ----
vct_tickers <- maintenance_dim_ticker[which(maintenance_dim_ticker$ticker_src_network == "binance" & maintenance_dim_ticker$data_src == "binance"),]$id
glbStartTime <- with_tz(as_datetime("2017-09-01 00:00:00"),"UTC")
binance_base <- "https://api.binance.com/api/v3/klines"
loopDayJump  <- 14

for(i in seq_along(vct_tickers)){
  k             <- vct_tickers[i]
  ticker        <- maintenance_dim_ticker[k,]$ticker_name
  data_src      <- maintenance_dim_ticker[k,]$data_src
  dir           <- paste0(raw_dir,"/binance/",ticker)
  
  if(length(list.files(dir)) == 0){
    cat(paste0("File directory NOT found for ",ticker,"\n"))
    loopStartDate <- as.Date(glbStartTime)
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",ticker,"\n"))
    loopStartDate <- max(as.Date(gsub("open_date=","",list.files(dir))))
  }
  cat(paste0("Starting data retrieval from ",loopStartdate,"\n"))
  
  unlink(paste0(dir,"/open_date=",loopStartDate),force=T,recursive=T)
  
  cat(paste0("Begin retrieval for ",ticker," from Binance","\n"))
  j <- 1
  startTime <- as_datetime(loopStartDate,tz="UTC")
  qryList   <- list()
  while(startTime <= refTime){
    cat(paste0("Loop ",j," from ",startTime," to ",endTime,"\n"))
    
    endTime <- min(refTime,startTime + days(loopDayJump) - hours(1))
    
    startTimeUnix <- fnConvTimeToUnix(startTime)
    endTimeUnix   <- fnConvTimeToUnix(endTime)
    
    url           <- paste0(binance_base
                            ,"?symbol=",ticker
                            ,"&interval=1h"
                            ,"&startTime=",startTimeUnix
                            ,"&endTime=",endTimeUnix)

    retryCounter  <- 0
    resCont <- "seed"
    while((resCont == "Internal Server Error" | resCont == "seed") & retryCounter < 3){
      retryCounter <- retryCounter+1
      print(paste0("Trying GET request, attempt = ",retryCounter))
      resGet        <- httr::GET(url)
      resCont       <- content(resGet,"text")
    }

    if(resCont != "Internal Server Error"){
      resJson       <- fromJSON(resCont,flatten=T)  
      resRaw        <- as.data.frame(resJson)
      
      if(nrow(resRaw)>0){
        colnames(resRaw) <- maintenance_dim_headers$binance
        qryList[[j]] <- resRaw
      } else {
        cat(paste0("No data retrieved for ",ticker," in network ",network," at start time = ",startTime," and end time = ",endTime,"\n"))
      }
    }
    
    j <- j+1
    startTime <- endTime + hours(1)
  }
  cat(paste0("Preparing data for parquet write","\n"))
  resOut <- bind_rows(qryList) %>% mutate_all(as.numeric) %>%
        mutate(
          open_time    = as.POSIXct(open_time/1000 , origin="1970-01-01", tz = "UTC")
          ,close_time  = as.POSIXct(close_time/1000, origin="1970-01-01", tz = "UTC")
          ,open_date   = as_date(open_time)
          ,close_date  = as_date(close_time)
          
          ,ticker      = ticker
          ,interval    = interval
          ,network     = "binance"
          ,data_src    = data_src
          ,last_modified = Sys.time()
        )
  
  cat(paste0("Writing to parquet files","\n"))
  vct_open_date <- unique(resOut$open_date)
  l <- 1L
  while(l <= length(vct_open_date)){
    arrow::write_dataset(
      resOut[which(resOut$open_date %in% vct_open_date[l:(min(length(vct_open_date),l+1023L))]),]
      ,dir
      ,format = "parquet"
      ,partitioning = "open_date"
      ,basename_template = paste0(c(ticker,interval,l,"{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}

## DEXSCREENER ----
vct_tickers      <- maintenance_dim_ticker[which(maintenance_dim_ticker$ticker_src_network == "harmony" & maintenance_dim_ticker$data_src == "dexscreener"),]$id
glbStartTime     <- with_tz(as_datetime("2021-07-01 00:00:00"),"UTC")
dexscreener_base <- "io5.dexscreener.io"
bar              <- 60

for(i in seq_along(vct_tickers)){
  k              <- vct_tickers[i]
  ticker         <- maintenance_dim_ticker[k,]$ticker_name
  network        <- maintenance_dim_ticker[k,]$ticker_src_network
  data_src       <- maintenance_dim_ticker[k,]$data_src
  dir            <- paste0(raw_dir,"/dexscreener/",network,"/",ticker) 
  
  ## Get latest day existing data
  if(length(list.files(dir)) == 0){
    cat(paste0("File directory NOT found for ",ticker,"\n"))
    loopStartDate <- as.Date(glbStartTime)
    cat(paste0("Creating directory at ",dir,"\n"))
    dir.create(dir,recursive=T)
  } else {
    cat(paste0("File directory found for ",ticker,"\n"))
    loopStartDate <- max(as.Date(gsub("open_date=","",list.files(dir))))
  }
  cat(paste0("Starting data retrieval from ",loopStartdate,"\n"))
  
  ## Remove latest day data
  unlink(paste0(dir,"/open_date=",loopStartDate),force=T,recursive=T) 
  
  cat(paste0("Begin retrieval for ",ticker," from dexscreener on network ",network,"\n"))
  j <- 1
  startTime <- as_datetime(loopStartDate,tz="UTC")
  qryList   <- list()
  while(startTime <= refTime){
    endTime       <- min(refTime,startTime + days(loopDayJump) - hours(1))
    cat(paste0("Loop ",j," from ",startTime," to ",endTime,"\n"))
    cb            <- interval(startTime,endTime) %/% hours(1)
    
    startTimeUnix <- fnConvTimeToUnix(startTime)
    endTimeUnix   <- fnConvTimeToUnix(endTime)
    
    url           <- paste0(
                        paste0(c(dexscreener_base,"u/chart/bars",network,ticker),collapse="/")
                        ,"?from=",startTimeUnix
                        ,"&to=",endTimeUnix
                        ,"&res=",bar
                        ,"&cb=",cb
                      )

    retryCounter  <- 0
    resCont <- "seed"
    while((resCont == "Internal Server Error" | resCont == "seed") & retryCounter < 3){
      retryCounter <- retryCounter+1
      print(paste0("Trying GET request, attempt = ",retryCounter))
      resGet        <- httr::GET(url)
      resCont       <- content(resGet,"text")
    }
    
    
    
    if(resCont != "Internal Server Error"){
      resJson       <- fromJSON(resCont,flatten=T)  
      
      if(!is.null(resJson$bars)){
        resRaw       <- as.data.frame(resJson)  
        resRaw       <- resRaw[which(resRaw$bars.timestamp >= startTimeUnix),] ## dexscreener query can go before startTime
        qryList[[j]] <- resRaw
      } else {
        cat(paste0("No data retrieved for ",ticker," in network ",network," at start time = ",startTime," and end time = ",endTime,"\n"))
      }
    }

    j <- j+1
    startTime <- endTime + hours(1)
  }
  cat(paste0("Preparing data for parquet write","\n"))
  resOut <- bind_rows(qryList) %>% 
    mutate_at(vars(starts_with("bars.")),as.numeric) %>%
    mutate(
      ticker      = ticker
      ,interval   = interval
      ,network    = network
      ,data_src   = data_src
      
      ,open_time  = as.POSIXct(bars.timestamp/1000,origin="1970-01-01",tz="UTC")
      ,open_date  = as_date(open_time)
      ,last_modified = Sys.time()
    )
  
  cat(paste0("Writing to parquet files","\n"))
  vct_open_date <- unique(resOut$open_date)
  l <- 1L
  while(l <= length(vct_open_date)){
    arrow::write_dataset(
      resOut[which(resOut$open_date %in% vct_open_date[l:(min(length(vct_open_date),l+1023L))]),]
      ,dir
      ,format = "parquet"
      ,partitioning = "open_date"
      ,basename_template = paste0(c(ticker,interval,l,"{i}.parquet"),collapse="_")
    )
    l <- l+1024L
  }
}

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