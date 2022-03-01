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
)  

maintenance_masterchef <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_masterchef.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "masterchef_address"="character"
    ,"treasury_address"="character"
    ,"emission_token1_lp_address"="character"
  )
)
maintenance_pid <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_pid.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "address"="character"
    ,"token1_address"="character"
    ,"token2_address"="character"
  )
)

maintenance_account_balance <- read.csv(
  paste0(base_github,ref_dir,"/maintenance_account_balance.csv")
  ,stringsAsFactors = F
  ,colClasses=c(
    "product_address"="character"
    ,"account_address"="character"
  )
)

date_start <-  as_date("2017-01-01")
date_end   <-  Sys.Date() + days(100)

## Load dim_date ----
dim_date <- 
  data.frame(
    dim_date_id       = seq(date_start,date_end,1)
  ) %>% as_tibble() %>%
  mutate(
    dim_date_int   = year(dim_date_id)*10000 + month(dim_date_id)*100+day(dim_date_id)
    ,cy_start      = floor_date(dim_date_id,"year")
    ,cy_end        = ceiling_date(dim_date_id,"year")-days(1)
    ,fy_start      = as_date(ISOdate(ifelse(month(dim_date_id) >= 7,year(dim_date_id),year(dim_date_id)-1),7,1,0,0,tz="UTC"))
    ,fy_end        = as_date(ISOdate(ifelse(month(dim_date_id) >= 7,year(dim_date_id)+1,year(dim_date_id)),6,30,0,0,tz="UTC"))
    ,start_1M      = as_datetime(floor_date(dim_date_id,"month",week_start = getOption("lubridate.week.start", 1)))
    ,end_1M        = as_datetime(ceiling_date(dim_date_id,"month",week_start = getOption("lubridate.week.start", 1))) - hours(1)
    ,start_1w      = as_datetime(floor_date(dim_date_id,"week",week_start = getOption("lubridate.week.start", 1)))
    ,end_1w        = as_datetime(ceiling_date(dim_date_id,"week",week_start = getOption("lubridate.week.start", 1))) - hours(1)
    
    ,day_in_week   = as.integer(wday(dim_date_id,F))
    ,day_name      = wday(dim_date_id,T)
    ,is_weekend    = day_in_week %in% c(1,7)
    
    ,year          = year(dim_date_id)
    ,month         = month(dim_date_id)
    ,month_name    = month(dim_date_id,label=T)
    ,day           = day(dim_date_id)
  )

## Load dim_time ----
dim_time <-
  data.frame(
    hour_bucket = format( seq.POSIXt(as.POSIXct(Sys.Date()), as.POSIXct(Sys.Date()+1), by = "60 min"),
                          "%H%M", tz="GMT")[1:24]
  ) %>% as_tibble() %>%
  mutate(
    dim_time_id = as.integer(hour_bucket)
    ,time_start = paste0(str_sub(hour_bucket,1,2),":00")
    ,time_end   = paste0(str_sub(hour_bucket,1,2),":59")
  )

## Load dim_datetime ----

  ## Get max datetime
  tmp_btc_date <- as_date(gsub("open_date=","",list.files(paste0(dir_raw,"/binance/BTCUSDT"))))
  max_open_time <- arrow::read_parquet(
      list.files(paste0(dir_raw,"/binance/BTCUSDT"),full.name=T,recursive=T)[which(tmp_btc_date == max(tmp_btc_date))]
    ) %>% summarise(max(open_time)) %>% pull()
  rm(tmp_btc_date)

  ## Create dim_date_time
  dim_date_time <-
    data.frame(
      dim_date_time_id = seq(as_datetime(date_start),as_datetime(date_end),by="60 min")
    ) %>% as_tibble() %>%
    mutate(
      dim_time_id      = as.integer(hour(dim_date_time_id)*100)
      ,dim_date_id     = as_date(dim_date_time_id)
      ,dim_date_int    = year(dim_date_id)*10000 + month(dim_date_id)*100+day(dim_date_id)
      ,dim_date_time_int = year(dim_date_time_id)*1000000 + month(dim_date_time_id)*10000 + day(dim_date_time_id)*100+hour(dim_date_time_id)
    ) %>%
    left_join(
      select(dim_date,-c(dim_date_int))
      ,by="dim_date_id"
    ) %>%
    mutate(
      bit_start_1h     = T
      ,bit_start_2h    = 0==(hour(dim_date_time_id)%%2)
      ,bit_start_4h    = 0==(hour(dim_date_time_id)%%4)
      ,bit_start_6h    = 0==(hour(dim_date_time_id)%%6)
      ,bit_start_12h   = 0==(hour(dim_date_time_id)%%12)
      ,bit_start_1d    = 0==(hour(dim_date_time_id)%%24)
      ,bit_start_1w    = (day_in_week == 2 & dim_time_id == 0)
      ,bit_start_1M    = (day == 1 & dim_time_id == 0)
      
      ,start_1h        = dim_date_time_id 
      ,end_1h          = dim_date_time_id
      ,start_2h        = dim_date_time_id - hours(hour(dim_date_time_id)%%2)
      ,end_2h          = start_2h + hours(1)
      ,start_4h        = dim_date_time_id - hours(hour(dim_date_time_id)%%4)
      ,end_4h          = start_4h + hours(3)
      ,start_6h        = dim_date_time_id - hours(hour(dim_date_time_id)%%6)
      ,end_6h          = start_6h + hours(5)
      ,start_12h       = dim_date_time_id - hours(hour(dim_date_time_id)%%12)
      ,end_12h         = start_12h + hours(11)
      ,start_1d        = as_datetime(dim_date_id)
      ,end_1d          = start_1d + hours(23)
      
      ,bit_end_1h     = T
      ,bit_end_2h     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_2h))
      ,bit_end_4h     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_4h))
      ,bit_end_6h     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_6h))
      ,bit_end_12h    = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_12h))
      ,bit_end_1d     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_1d))
      ,bit_end_1w     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_1w))
      ,bit_end_1M     = ifelse(dim_date_time_id == max_open_time,T,(dim_date_time_id == end_1M))
    ) %>%
    mutate_at(
      vars(starts_with("start")|starts_with("end"))
      ,~funConvertDateTimeToInt(.)
    ) %>%
    select(-c(dim_date_id,cy_start,cy_end,fy_start,fy_end))
  
## Load dim_tickers ----  
maintenance_dim_ticker
maintenance_pid

as_tibble(maintenance_dim_ticker) %>%
  rename(product_name )
  




dim_ticker <- maintenance_dim_ticker %>% rename(dim_ticker_id = id)

maintenance_pid

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