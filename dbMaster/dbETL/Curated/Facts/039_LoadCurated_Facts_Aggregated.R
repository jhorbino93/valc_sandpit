## Get necessary dims
dir_dim_datetime  <- paste0(c(dir_data_cur_dim,"dim_datetime.parquet"),collapse="/")
dir_dim_interval  <- paste0(c(dir_data_cur_dim,"dim_interval.parquet"),collapse="/")
dir_dim_asset     <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dir_fact_prices   <- paste0(c(dir_data_cur_fact,"fact_prices.parquet"),collapse="/")

dim_datetime      <- read_parquet(dir_dim_datetime)
dim_interval      <- read_parquet(dir_dim_interval)
dim_asset         <- read_parquet(dir_dim_asset)

regex_find_date = "(.*?)(\\d{4}-)(\\d{1,2}-)(\\d{1,2})(.*)"

# dir_cur_fact_src <- list.files(dir_data_cur_fact)
# dir_cur_fact_src <- dir_cur_fact_src[which(dir_cur_fact_src != "Aggregated")]

dir_cur_fact_src <- "Calculated"
dest_dir = paste0(c(dir_data_cur_fact,"Aggregated"),collapse="/")

## Collect all partitions and write as single dataset 
cat(paste0("File directory NOT found for ",dest_dir,"\n"))
cat("Creating directory\n")
dir.create(dest_dir,recursive=T)
cat("Retrieving all curated fact prices data\n")
df_raw = arrow::open_dataset(
  list.files(paste0(dir_data_cur_fact,"/",dir_cur_fact_src),full.names=T,recursive = T)
) %>% collect() %>% as_tibble()

write_parquet(df_raw,paste0(c(dest_dir,"fact_prices_calculated.parquet"),collapse="/"))