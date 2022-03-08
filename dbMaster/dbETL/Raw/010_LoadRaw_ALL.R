library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)
library(ether)

library(foreach)
library(doParallel)


## Initialise Env ----
options(scipen = 999)
setwd("C:/Users/jehor/Documents/GitHub/Hermes")

dir_raw <- "./dbMaster/dbData/001Raw"
dir_cur <- "./dbMaster/dbData/020Curated"
dir_ref <- "./dbMaster/dbReference"

dir_data_cur_dim <- paste0(c(dir_cur,"Dim"),collapse="/")
dir_data_cur_fact <- paste0(c(dir_cur,"Fact"),collapse="/")

dir_etl <- "./dbMaster/dbETL"
dir_etl_raw <- paste0(c(dir_etl,"Raw"),collapse="/")
dir_etl_cur_dim <- paste0(c(dir_etl,"Curated","Dimensions"),collapse="/")
dir_etl_cur_fact <- paste0(c(dir_etl,"Curated","Facts"),collapse="/")

dir_functions <- "./r_functions"

## Load Functions ----
sapply(list.files(dir_functions,full.names = T,recursive=T),source)

## Get Reference Data ----
maintenance_dim_ticker  <- read.csv(
  paste0(c(dir_ref,"maintenance_dim_ticker.csv"),collapse="/")
  ,stringsAsFactors = F
  ,na.string=c("")
) 

maintenance_masterchef <- read.csv(
  paste0(c(dir_ref,"maintenance_masterchef.csv"),collapse="/")
  ,stringsAsFactors = F
  ,colClasses=c(
    "masterchef_address"="character"
  )
  ,na.string=c("")
)

maintenance_masterchef_emission <- read.csv(
  paste0(c(dir_ref,"maintenance_masterchef_emission.csv"),collapse="/")
  ,stringsAsFactors = F
  ,colClasses=c(
    "masterchef_address"="character"
    ,"emission_token_address"="character"
  )
  ,na.string=c("")
)

maintenance_pid <- read.csv(
  paste0(c(dir_ref,"maintenance_pid.csv"),collapse="/")
  ,colClasses=c(
    "address"="character"
    ,"masterchef_address"="character"
    ,"token1_address"="character"
    ,"token2_address"="character"
  )
  ,na.string=c("")
)

maintenance_account_balance <- read.csv(
  paste0(c(dir_ref,"maintenance_account_balance.csv"),collapse="/")
  ,stringsAsFactors = F
  ,colClasses=c(
    "product_address"="character"
    ,"account_address"="character"
  )
  ,na.string=c("")
)

refTime                 <- as.POSIXct(format(Sys.time()),tz="UTC")
refTimeUnix             <- fnConvTimeToUnix(refTime)

## Begin load ----
baseRObj <- c(ls(),"baseRObj")

## Load Binance ----
source(paste0(c(dir_etl_raw,"010_LoadRaw_CEX_Binance.R"),collapse="/"),echo=T)
rm(list=setdiff(ls(),baseRObj))

## Load Dexscreener ----
source(paste0(c(dir_etl_raw,"010_LoadRaw_DEX_Dexscreener.R"),collapse="/"))
rm(list=setdiff(ls(),baseRObj))

## Load Blockchain Harmony ----
source(paste0(c(dir_etl_raw,"010_LoadRaw_Blockchain_Harmony.R"),collapse="/"))
rm(list=setdiff(ls(),baseRObj))

## Load Blockchain Additional Harmony ----
source(paste0(c(dir_etl_raw,"011_LoadRaw_Blockchain_Harmony_SC.R"),collapse="/"))
rm(list=setdiff(ls(),baseRObj))
