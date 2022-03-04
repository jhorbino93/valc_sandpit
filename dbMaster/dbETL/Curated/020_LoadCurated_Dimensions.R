library(tidyverse)
library(arrow)
library(sparklyr)
library(lubridate)
library(devtools)

# library(foreach)
# library(doParallel)

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

default_start_date = as_date("1970-01-01")
default_end_date = as_date("2099-12-31")
current_date = as_date(Sys.Date())
default_prior_date = current_date - days(1)

## Github directory
base_github <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster"
ref_dir <- "/dbReference"

## Load Functions ----
url_r_misc_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/misc_functions.R"
source_url(url_r_misc_fn)

url_r_hmy_fn <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/r_functions/hmy_functions.R"
source_url(url_r_hmy_fn)


## Github directory
base_github <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster"
ref_dir <- "/dbReference"

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



dir_dim_asset     <- paste0(c(dir_cur,"Dim","dim_asset.parquet"),collapse="/")
dim_asset         <- read_parquet(dir_dim_asset)

dir_dim_masterchef     <- paste0(c(dir_cur,"Dim","dim_masterchef.parquet"),collapse="/")
dim_masterchef    <- read_parquet(dir_dim_masterchef)

select(dim_asset,dim_asset_id,onchain_address) %>%
  rename(account_dim_asset_id=dim_asset_id)

dim_asset$onchain_network

df_account <- 
  as_tibble(maintenance_account_balance) %>%
  inner_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network) %>%
      rename(account_dim_asset_id=dim_asset_id)
    ,by=c("account_address"="onchain_address","network"="onchain_network")
  ) %>%
  inner_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network) %>%
      rename(product_dim_asset_id=dim_asset_id)
    ,by=c("product_address"="onchain_address","network"="onchain_network")
  ) %>%
  select(platform,account_name,account_type,account_dim_asset_id,product_dim_asset_id,network)

nrow(maintenance_account_balance)
  
as_tibble(maintenance_account_balance) %>%
  mutate(
    account_address = str_to_lower(account_address)
    ,product_address = str_to_lower(product_address)
  ) %>%
  left_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network) %>%
      rename(account_dim_asset_id=dim_asset_id)
    ,by=c("account_address"="onchain_address","network"="onchain_network")
  ) %>%
  filter(is.na(account_dim_asset_id))

filter(dim_asset,asset_short_name == "IRIS/WONE")
       
       view(dim_asset)



## Facts ----

  ## Get necessary dims
  dir_dim_datetime  <- paste0(c(dir_cur,"Dim","dim_datetime.parquet"),collapse="/")
  dir_dim_interval  <- paste0(c(dir_cur,"Dim","dim_interval.parquet"),collapse="/")
  dir_dim_asset     <- paste0(c(dir_cur,"Dim","dim_asset.parquet"),collapse="/")
  
  dim_datetime      <- read_parquet(dir_dim_datetime)
  dim_interval      <- read_parquet(dir_dim_interval)
  dim_asset         <- read_parquet(dir_dim_asset)

  vct_schema_cols <- c("datetime","date","dim_asset_id","dim_interval_id","quote_type","data_src"
                       ,"o","h","l","c","v","qav","num_trades"
  )

