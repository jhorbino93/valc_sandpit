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
dir_raw <- "C:/Users/jehor/Documents/GitHub/Hermes/dbMaster/dbData/001Raw"
dir_cur <- "C:/Users/jehor/Documents/GitHub/Hermes/dbMaster/dbData/020Curated"

base_github <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster"
ref_dir <- "/dbReference"
dir_etl_cur_dim <- "dbETL/Curated/020_LoadCurated_Dimensions.R"
dir_etl_cur_fact <- "dbETL/Curated/020_LoadCurated_Facts.R"

default_start_date = as_date("1970-01-01")
default_end_date = as_date("2099-12-31")
current_date = as_date(Sys.Date())
default_prior_date = current_date - days(1)

## Load Functions ----
url_r_misc_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/misc_functions.R"
source_url(url_r_misc_fn)

url_r_hmy_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/hmy_functions.R"
source_url(url_r_hmy_fn)

## Get Reference Data ----
maintenance_dim_ticker  <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_dim_ticker.csv")
  ,stringsAsFactors = F
  ,na.string=c("")
)  

maintenance_masterchef <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_masterchef.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "masterchef_address"="character"
  )
  ,na.string=c("")
)

maintenance_masterchef_emission <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_masterchef_emission.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "masterchef_address"="character"
    ,"emission_token_address"="character"
  )
  ,na.string=c("")
)

maintenance_pid <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_pid.csv")
  # ,stringsAsFactors = F
  ,colClasses=c(
    "address"="character"
    ,"masterchef_address"="character"
    ,"token1_address"="character"
    ,"token2_address"="character"
  )
  ,na.string=c("")
)

maintenance_account_balance <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_account_balance.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "product_address"="character"
    ,"account_address"="character"
  )
  ,na.string=c("")
)

## Misc ----
baseRObj <- c(ls(),"baseRObj")
