library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)
library(ether)

library(foreach)
library(doParallel)

## Initialise Environment ----
options(scipen = 999)

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
maintenance_dim_ticker  <- read.csv(paste0(base_github,ref_dir,"/maintenance_dim_ticker.csv"),stringsAsFactors = F)
maintenance_dim_headers <- read.csv(paste0(base_github,ref_dir,"/maintenance_dim_headers.csv"),stringsAsFactors = F)
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

refTime                 <- as.POSIXct(format(Sys.time()),tz="UTC")
refTimeUnix             <- fnConvTimeToUnix(refTime)

## Misc ----
interval <- "1h"
baseRObj <- c(ls(),"baseRObj")

## Load Binance ----
url_010_LoadRaw_CEX_Binance <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster/dbETL/Raw/010_LoadRaw_CEX_Binance.R"
source_url(url_010_LoadRaw_CEX_Binance)
rm(list=setdiff(ls(),baseRObj))


## Load Dexscreener ----
url_010_LoadRaw_DEX_Dexscreener <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster/dbETL/Raw/010_LoadRaw_DEX_Dexscreener.R"
source_url(url_010_LoadRaw_DEX_Dexscreener)
rm(list=setdiff(ls(),baseRObj))

## Load Blockchain Harmony ----
url_010_LoadRaw_Blockchain_Harmony <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster/dbETL/Raw/010_LoadRaw_Blockchain_Harmony.R"
source_url(url_010_LoadRaw_Blockchain_Harmony)
rm(list=setdiff(ls(),baseRObj))

## Load Blockchain Additional Harmony ----
url_011_LoadRaw_Blockchain_Harmony_SC <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dbMaster/dbETL/Raw/011_LoadRaw_Blockchain_Harmony_SC.R"
source_url(url_011_LoadRaw_Blockchain_Additional_Harmony)
rm(list=setdiff(ls(),baseRObj))


