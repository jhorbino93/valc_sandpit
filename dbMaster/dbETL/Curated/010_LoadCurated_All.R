library(tidyverse)
library(arrow)
library(sparklyr)
library(lubridate)
library(devtools)

library(foreach)
library(doParallel)

## Initialise Spark
# config <- spark_config()
# config$`sparklyr.shell.driver-memory`   <- '16G'
# config$`sparklyr.shell.executor-memory` <- '16G'
# config$`sparklyr.cores.local` <- 4


# sc <- spark_connect(master = "local"
#                     ,spark_home = paste0("C:/Users/",Sys.getenv("USERNAME"),"/AppData/Local/spark/spark-3.0.2-bin-hadoop2.7")
#                     ,packages = 'io.delta:delta-core_2.12:0.8.0'
#                     ,config = config
# )
# 
# sparklyr::invoke_static(sc, "java.util.TimeZone",  "getTimeZone", "GMT") %>%
# sparklyr::invoke_static(sc, "java.util.TimeZone", "setDefault", .)
# 
# spark_disconnect_all()

## Initialise Env ----
setwd("C:/Users/jehor/Documents/GitHub/Hermes")


dir_raw <- "./dbMaster/dbData/001Raw"
dir_cur <- "./dbMaster/dbData/020Curated"
dir_ref <- "./dbMaster/dbReference"

dir_etl <- "./dbMaster/dbETL"

dir_data_cur_dim <- paste0(c(dir_cur,"Dim"),collapse="/")
dir_data_cur_fact <- paste0(c(dir_cur,"Fact"),collapse="/")

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

## Begin load ----
baseRObj <- c(ls(),"baseRObj")


  ## Execute dimensions load
  source(paste0(c(dir_etl,"Curated","020_LoadCurated_Dimensions.R"),collapse="/"))
  rm(list=setdiff(ls(),baseRObj))
    
  ## Execute fact load
  source(paste0(c(dir_etl,"Curated","030_LoadCurated_Facts.R"),collapse="/"))
  rm(list=setdiff(ls(),baseRObj))
  gc()  

