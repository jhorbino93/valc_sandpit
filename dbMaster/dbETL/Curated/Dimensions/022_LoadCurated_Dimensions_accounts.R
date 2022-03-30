dir_dim_asset      <- paste0(c(dir_data_cur_dim,"dim_asset.parquet"),collapse="/")
dim_asset          <- read_parquet(dir_dim_asset)

## Start Dataframe
df_account <- 
  as_tibble(maintenance_account_balance) %>%
  mutate(
    account_address = str_to_lower(account_address)
    ,product_address = str_to_lower(product_address)
  ) %>%
  left_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network) %>%
      rename(account_dim_asset_id=dim_asset_id)
    ,by=c("account_address"="onchain_address","network"="onchain_network")
  ) %>%
  left_join(
    select(dim_asset,dim_asset_id,onchain_address,onchain_network) %>%
      rename(product_dim_asset_id=dim_asset_id)
    ,by=c("product_address"="onchain_address","network"="onchain_network")
  ) %>%
  select(platform,account_address,account_name,account_type,account_dim_asset_id,product_dim_asset_id,network,product_address
         ,product_name,product_address)


## Load Accounts
    dir_dim_account <- paste0(c(dir_data_cur_dim,"dim_account.parquet"),collapse="/")
    vct_pk_dim_account <- c("account_address","network")
    if(file.exists(dir_dim_account)){
      old_dim_account <- arrow::read_parquet(dir_dim_account)
    }
    
    ## Intermediate dim_account
    dim_account <- df_account %>% distinct(platform,account_address,account_name,account_type,network)
    
    dim_account_other <- distinct(dim_account,network) %>%
      mutate(
        account_address = NA
        ,platform = "Other"
        ,account_name = "Other"
        ,account_type = "Other"
      ) %>%
      select_at(colnames(dim_account))
    
    dim_account <- bind_rows(dim_account,dim_account_other)
    
    ## Match & merge
    if(file.exists(dir_dim_account)){
      dim_account <- fn_db_merge_dim(dim_account,old_dim_account,vct_pk_dim_account,"dim_account_id")
    } else {
      vct_attributes <- colnames(dim_account)[which(!colnames(dim_account) %in% c("dim_account_id",vct_pk_dim_account))]
      dim_account <- mutate(dim_account,dim_account_id=row_number()) %>% select_at(c("dim_account_id",vct_pk_dim_account,vct_attributes))
    }

    arrow::write_parquet(dim_account,dir_dim_account)
    
## Load Account Bridge To Products

    dir_bridge_account_product <- paste0(c(dir_data_cur_dim,"bridge_account_product.parquet"),collapse="/")
    bridge_account_product <- 
      inner_join(
        df_account
        ,dim_account
      ) %>% 
      select(dim_account_id,product_dim_asset_id,network)
    
    bridge_account_product_other <-
      dim_asset %>%
      filter(
        asset_type_l1 == "On-chain"
      ) %>%
      distinct(dim_asset_id,ticker_src_network) %>%
      rename(
        product_dim_asset_id = dim_asset_id
        ,network = ticker_src_network
      ) %>%
      inner_join(
        dim_account %>% filter(account_name == "Other")
        ,by="network"
      ) %>%
      select_at(colnames(bridge_account_product))
      
    bridge_account_product <- bind_rows(bridge_account_product,bridge_account_product_other)

    write_parquet(bridge_account_product,dir_bridge_account_product)
    
    