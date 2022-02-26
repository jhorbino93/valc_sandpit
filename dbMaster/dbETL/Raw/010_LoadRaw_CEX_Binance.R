library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(arrow)

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
