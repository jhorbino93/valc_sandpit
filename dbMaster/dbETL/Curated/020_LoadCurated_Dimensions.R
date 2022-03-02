library(tidyverse)
library(arrow)
library(sparklyr)
library(lubridate)
library(devtools)

## Initialise Spark
config <- spark_config()
config$`sparklyr.shell.driver-memory`   <- '16G'
config$`sparklyr.shell.executor-memory` <- '16G'
config$`sparklyr.cores.local` <- 4


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
    ,"treasury_address"="character"
    ,"emission_token1_lp_address"="character"
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



  
## Load dim_assets ----  
  tmp_asset1 <- 
    as_tibble(maintenance_dim_ticker) %>%
    ## Add other attributes 
    mutate(
      masterchef_address = NA_character_
      ,onchain_address = ticker_name
      ,pid = NA_integer_
      ,asset_type_l1 = case_when(
        ticker_src_cat == "cex"~"CEX"
        ,ticker_src_cat == "dex"~"On-chain"
      )
      ,asset_type_l2 = case_when(
        ticker_src_cat == "cex"~"CEX ticker"
        ,ticker_src_cat == "dex"~"LP"
        ,T ~ "Other"
      )
      ,asset_type_l3 = asset_type_l2
      ,onchain_network = ticker_src_network
      ,ticker_alias2 = ticker_alias
    ) %>%
    ## Select order
    select(
      short_name
      ,ticker_alias
      ,ticker_alias2
      ,asset_type_l1
      ,asset_type_l2
      
      ## Ticker related columns
      ,ticker_name
      ,ticker_src_cat
      ,ticker_src_network
      ,asset1
      ,asset2
      ,data_src
      ,exchange_name
      
      ## On chain stuff
      ,onchain_network
      ,onchain_address
    ) %>%
    
    ## Rename output 
    rename(
      asset_name = short_name
      ,asset_to = asset1
      ,asset_from = asset2
    )
  
  tmp_asset2 <- as_tibble(maintenance_pid) %>%
    filter(!address %in% tmp_asset1$onchain_address) %>%
    mutate(
      ticker_name = address
      ,ticker_alias = product_name
      ,ticker_alias2 = friendly_alias
      ,asset_type_l1 = "On-chain"
      ,asset_type_l2 = product_type
      ,asset_type_l3 = asset_type_l2
      ,ticker_src_cat = case_when(
        product_type == "LP" ~ "dex"
        ,product_type %in% c("HRC20") ~ "address"
        ,T ~ "Other"
      )
      ,ticker_src_network = network
      ,asset_to = NA_character_
      ,asset_from = NA_character_
      ,data_src = "On-chain"
      ,onchain_network = network
      ,onchain_address = address
      ,exchange_name = dex_platform
    ) %>%
    rename(
      asset_name = product_name
    ) %>%
    select_at(colnames(tmp_asset1))
  
  dim_asset <- bind_rows(tmp_asset1,tmp_asset2) %>%
    mutate(
      dim_asset_id = row_number()
    ) %>%
    select_at(c("dim_asset_id",colnames(tmp_asset1)))
  
  arrow::write_parquet(dim_asset,paste0(c(dir_cur,"dim_asset.parquet"),collapse="/"))


## Load dim products ----
unique(c(dim_ticker$asset1,dim_ticker$asset2))
  
## Binance ----
vct_binance_assets <- list.files(paste0(dir_raw,"/binance"))

  ## Generate spark tbls
  sdf_binance <-
    lapply(
      setNames(vct_binance_assets,vct_binance_assets)
      ,function(x){
        dir = paste0(dir_raw,"/binance/",x)
        spark_read_parquet(sc,name=x,path=dir) %>% sdf_register(name=x)
      }
    ) %>% do.call(sdf_bind_rows,.) %>% sdf_register("sdf_binance")
  
  ## Remove component tbls
  sapply(
    vct_binance_assets
    ,function(x){
      sdf_sql(sc,paste0("DROP TABLE ",str_to_lower(x)))
    }
  )
  
  sdf_binance %>% select(open_time,o,h,l,c,v,ticker,interval,network)

  
  
list.files(vct_binance_dir[1])

list_raw <- list()
for(i in seq_along(vct_binance_dir)){
  dir <- vct_binance_dir[i]
  print(i)
  print(dir)
  # res <- arrow::open_dataset(dir) %>% collect()
  # list_raw[[i]] <- res
  
  files <- list.files(dir,full.names = T,recursive=T)
  
  list_files <- list()
  for(k in seq_along(files)){
    print(k)
    list_files[[k]] <- arrow::read_parquet(files[[k]])
  }
  list_raw[[i]] <- list_files
}