## Load dim_date ----
date_start <-  as_date("2017-01-01")
date_end   <-  Sys.Date() + days(100)

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