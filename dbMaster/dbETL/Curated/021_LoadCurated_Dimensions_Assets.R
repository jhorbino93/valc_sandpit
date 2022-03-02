dir_dim_asset <- paste0(c(dir_cur,"dim_asset.parquet"),collapse="/")

if(file.exists(dir_dim_asset)){
  vct_exist_asset <- vct_arrow::read_parquet(paste0(c(dir_cur,"dim_asset.parquet"),collapse="/"))
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

arrow::write_parquet(dim_asset,dir_dim_asset)