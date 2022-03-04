### Masterchef, Emission Token, LP ----
dim_asset <- arrow::read_parquet(paste0(c(dir_cur,"Dim","dim_asset.parquet"),collapse="/"))


## Masterchef
dir_dim_masterchef <- paste0(c(dir_cur,"Dim","dim_masterchef.parquet"),collapse="/")
vct_pk_dim_masterchef <- c("masterchef_address","platform","category","network")

if(file.exists(dir_dim_masterchef)){
  old_dim_masterchef <- arrow::read_parquet(dir_dim_masterchef)
  dim_masterchef <- maintenance_masterchef
  dim_masterchef <- fn_db_merge_dim(dim_masterchef,old_dim_masterchef,vct_pk_dim_masterchef,"dim_masterchef_id")
} else {
  dim_masterchef <- mutate(maintenance_masterchef,dim_masterchef_id = row_number()) %>% 
                    select_at(c("dim_masterchef_id",colnames(maintenance_masterchef)[which(!colnames(maintenance_masterchef) %in% c("dim_masterchef_id"))]))
}
write_parquet(dim_masterchef,paste0(dir_dim_masterchef))

## Masterchef Emissions
## Bridging table doesn't need type 2 checking
dir_bridge_masterchef_emission <- paste0(c(dir_cur,"Dim","bridge_masterchef_emission.parquet"),collapse="/")
bridge_masterchef_emission <- 
  maintenance_masterchef_emission %>%
  inner_join(select(dim_masterchef,dim_masterchef_id,masterchef_address,network),by=c("masterchef_address","network")) %>%
  inner_join(select(dim_asset,dim_asset_id,onchain_address,onchain_network)
             ,by=c("emission_token_address"="onchain_address","network"="onchain_network")) %>%
  select(dim_masterchef_id,dim_asset_id)
write_parquet(bridge_masterchef_emission,dir_bridge_masterchef_emission)

## Liquidity Pool
dir_dim_lp <- paste0(c(dir_cur,"Dim","dim_lp.parquet"),collapse="/")
vct_pk_dim_lp <- c("lp_dim_asset_id","dim_masterchef_id")

if(file.exists(dir_dim_lp)){
  old_dim_lp <- arrow::read_parquet(dir_dim_lp)
  dim_lp <-
    as_tibble(maintenance_pid) %>%
    filter(product_type == "LP") %>%
    select(masterchef_address,product_type,pid,address,network) %>%
    inner_join(
      select(dim_asset,dim_asset_id,onchain_address,onchain_network)    
      ,by=c("address"="onchain_address","network"="onchain_network")
    ) %>%
    inner_join(
      dim_masterchef
      ,by=c("masterchef_address","network")
    ) %>%
    rename(
      lp_dim_asset_id = dim_asset_id
    ) %>%
    select(lp_dim_asset_id,dim_masterchef_id,pid,network)
  dim_lp <- fn_db_merge_dim(dim_lp,old_dim_lp,vct_pk_dim_lp,"dim_lp_id")
} else {
  dim_lp <-
    as_tibble(maintenance_pid) %>%
    filter(product_type == "LP") %>%
    select(masterchef_address,product_type,pid,address,network) %>%
    inner_join(
      select(dim_asset,dim_asset_id,onchain_address,onchain_network)    
      ,by=c("address"="onchain_address","network"="onchain_network")
    ) %>%
    inner_join(
      dim_masterchef
      ,by=c("masterchef_address","network")
    ) %>%
    rename(
      lp_dim_asset_id = dim_asset_id
    ) %>%
    mutate(
      dim_lp_id = row_number()
    ) %>%
    select(dim_lp_id,lp_dim_asset_id,dim_masterchef_id,pid,network)
}
write_parquet(dim_lp,dir_dim_lp)