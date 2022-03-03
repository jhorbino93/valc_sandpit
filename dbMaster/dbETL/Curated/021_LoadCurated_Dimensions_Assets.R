dir_dim_asset <- paste0(c(dir_cur,"dim_asset.parquet"),collapse="/")

if(file.exists(dir_dim_asset)){
  old_dim_asset <- arrow::read_parquet(paste0(c(dir_cur,"dim_asset.parquet"),collapse="/"))
}

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
    ,asset_alias = short_name
    ,asset_name = ticker_alias
  ) %>%
  ## Select order
  select(
    asset_name
    ,short_name
    ,asset_alias
    ,asset_type_l1
    ,asset_type_l2
    
    ## Ticker related columns
    ,ticker_name
    ,ticker_alias
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
    asset_to = asset1
    ,asset_from = asset2
    ,asset_short_name = short_name
  )

tmp_asset2 <- as_tibble(maintenance_pid) %>%
  filter(!address %in% tmp_asset1$onchain_address) %>%
  mutate(
    ticker_name = address
    ,asset_short_name  = friendly_alias
    ,asset_alias = friendly_alias
    ,asset_type_l1 = "On-chain"
    ,asset_type_l2 = product_type
    ,asset_type_l3 = asset_type_l2
    ,ticker_alias = product_name
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


dim_asset <- bind_rows(tmp_asset1,tmp_asset2) %>%select_at(colnames(tmp_asset1))
rm(list=c("tmp_asset1","tmp_asset2"))

vct_pk_dim_asset <- c("asset_name","ticker_name","ticker_src_network","data_src")

## Match & merge
dim_asset <- fn_db_merge_dim(dim_asset,old_dim_asset,vct_pk_dim_asset,"dim_asset_id")

## Write to dir
arrow::write_parquet(dim_asset,paste0(c(dir_cur,"dim_asset.parquet"),collapse="/"))