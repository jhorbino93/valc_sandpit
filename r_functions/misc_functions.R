
fn_formatPlotBigNum <- function(tx) { 
  div <- findInterval(as.numeric(gsub("\\,", "", tx)), 
                      c(0, 1e3, 1e6, 1e9, 1e12) )  # modify this if negative numbers are possible
  paste(round( as.numeric(gsub("\\,","",tx))/10^(3*(div-1)), 2), 
        c("","K","M","B","T")[div] )
}
funConvertDateTimeToInt = function(datetime){
  year(datetime)*1000000 + month(datetime)*10000 + day(datetime)*100+hour(datetime)
}
fnConvUnixToTime  <- function(x){as.POSIXct(x/1000,origin="1970-01-01",tz="UTC")}
funConvTimeToUnix <- function(x){as.numeric(difftime(as_datetime(x),as_datetime("1970-01-01 00:00:00"),units="secs"))*1000}
fnConvTimeToUnix <- function(x){as.numeric(difftime(as_datetime(x),as_datetime("1970-01-01 00:00:00"),units="secs"))*1000}
fnImpermanentLoss <- function(k,rho=0){((2-rho)*sqrt(k)-(rho*k))/((k+1)*(1-rho))-1}
fnScaleLabs       <- function(){function(x) format(100*x,digits=2)}
fn_getDexScreener <- function(network,address,start,end,bar,cb){
  qry_url  <- paste0("io5.dexscreener.io/u/chart/bars/",network,"/",address,"?from=",start,"&to=",end,"&res=",bar,"&cb=",cb)
  res_flat <- content(GET(qry_url),"text",flatten=T)
  res_fromJSON <- Vectorize(fromJSON(res_flat))
  res_df <- as.data.frame(res_fromJSON)
  
  res <- mutate_at(res_df,vars(starts_with("bars.")),as.numeric)
  return(res)
  # as.data.frame(fromJSON(content(GET(qry_url),"text",flatten=T))) %>% mutate_at(vars(starts_with("bars.")),as.numeric)
}

fn_db_merge_dim <- function(tbl1,tbl2,key_cols,key_id){
  vct_attributes <- colnames(tbl1)[which(!colnames(tbl1) %in% c(key_id,key_cols))]
  # Function doesn't delete records that don't exist as they can match to old fact entries
  
  # Match new tbl to old tbl to get existing IDs based on PK match.
  # Use new tbl attributes
  match_pk_new_attributes <-
    inner_join(
      tbl1
      ,select_at(tbl2,c(key_id,key_cols))
      ,by=key_cols
    )
  
  # Records in new tbl that doesn't exist in old tbl
  not_match_pk <-
    anti_join(
      tbl1
      ,select_at(tbl2,c(key_id,key_cols))
      ,by=key_cols
    ) %>%
    mutate(
      "{key_id}" := row_number() + max(match_pk_new_attributes[[key_id]])
    )
  
  return(bind_rows(match_pk_new_attributes,not_match_pk) %>% select_at(c(key_id,key_cols,vct_attributes)))
}