library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(arrow)

## DEXSCREENER ----
vct_tickers      <- maintenance_dim_ticker[which(maintenance_dim_ticker$ticker_src_network == "harmony" & maintenance_dim_ticker$data_src == "dexscreener"),]$id
glbStartTime     <- with_tz(as_datetime("2021-07-01 00:00:00"),"UTC")
dexscreener_base <- "io5.dexscreener.io"
bar              <- 60
loopDayJump      <- 14

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
  cat(paste0("Starting data retrieval from ",loopStartDate,"\n"))
  
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