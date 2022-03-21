dir_dim_asset <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
vct_pk_dim_asset <- c("asset_name","ticker_name","ticker_src_network","data_src")
if(file.exists(dir_dim_asset)){
  old_dim_asset <- arrow::read_parquet(dir_dim_asset)
}

## Load dim_assets ----  
tmp_asset1 <- 
  as_tibble(maintenance_dim_ticker) %>%
  ## Add other attributes 
  mutate(
    masterchef_address = NA_character_
    ,ticker_name = str_to_lower(ticker_name)
    ,onchain_address = str_to_lower(ticker_name)
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
    ,is_native = 1
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
    ,is_native
  ) %>%
  
  ## Rename output 
  rename(
    asset_to = asset1
    ,asset_from = asset2
    ,asset_short_name = short_name
  )

tmp_asset2 <- 
  as_tibble(maintenance_pid) %>%
  mutate(address = str_to_lower(address)) %>%
  filter(!address %in% tmp_asset1$onchain_address) %>%
  mutate(
    ticker_name = address
    ,asset_short_name  = product_short_name
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
    ,address = str_to_lower(address)
    ,masterchef_address = str_to_lower(masterchef_address)
    ,onchain_network = network
    ,onchain_address = address
    ,exchange_name = dex_platform
  ) %>%
  rename(
    asset_name = product_name
  ) %>%
  select_at(colnames(tmp_asset1))

dim_asset <- bind_rows(tmp_asset1,tmp_asset2) %>%select_at(colnames(tmp_asset1))

tmp_asset3 <- 
  dim_asset %>% distinct(asset_short_name) %>%
  mutate(
    asset_name = asset_short_name
    ,ticker_name = asset_name
    ,ticker_src_network = "Calculated"
    ,data_src = "Calculated"
    ,asset_alias = asset_short_name
    ,asset_alias = asset_short_name
    ,asset_type_l1 = "Calculated"
    # ,asset_type_l2 = ifelse(asset_type_l2 == "LP","LP VWAP","VWAP")
    ,asset_type_l2 = "VWAP"
    ,ticker_alias = asset_short_name
    ,ticker_src_cat = "calculated"
    ,asset_to = asset_short_name
    ,asset_from = "USD"
    ,exchange_name = "Calculated"
    ,onchain_network = "Calculated"
    ,onchain_address = "Calculated"
    ,is_native = NA
  )  %>%
  select_at(colnames(dim_asset))

dim_asset <- bind_rows(dim_asset,tmp_asset3)

## Match & merge
if(file.exists(dir_dim_asset)){
  dim_asset <- fn_db_merge_dim(dim_asset,old_dim_asset,vct_pk_dim_asset,"dim_asset_id")
} else {
  vct_attributes <- colnames(dim_asset)[which(!colnames(dim_asset) %in% c("dim_asset_id",vct_pk_dim_asset))]
  dim_asset <- mutate(dim_asset,dim_asset_id=row_number()) %>% select_at(c("dim_asset_id",vct_pk_dim_asset,vct_attributes))
}

## Write to dir
arrow::write_parquet(dim_asset,dir_dim_asset)

