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


### dim_lp ----
dim_asset <- arrow::read_parquet(paste0(c(dir_cur,"dim_asset.parquet"),collapse="/"))


dim_lp <-
  as_tibble(maintenance_pid) %>%
  filter(product_type == "LP") %>%
  select(masterchef_address,product_type,pid,address,network) %>%
  inner_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network)    
    ,by=c("address"="onchain_address","network"="onchain_network")
  ) %>%
  inner_join(
    maintenance_masterchef
    ,by=c("masterchef_address","network")
  ) %>%
  mutate(
    dim_lp_id = row_number()
  ) %>%
  rename(
    lp_dim_asset_id = dim_asset_id
  ) %>%
  select(dim_lp_id,lp_dim_asset_id,masterchef_address,emission_token1_id)
  


as_tibble(maintenance_masterchef)



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