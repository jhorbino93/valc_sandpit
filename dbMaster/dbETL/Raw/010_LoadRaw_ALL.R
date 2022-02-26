library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(devtools)
library(arrow)

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
refTime                 <- as.POSIXct(format(Sys.time()),tz="UTC")
refTimeUnix             <- fnConvTimeToUnix(refTime)
