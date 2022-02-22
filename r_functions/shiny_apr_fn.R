
fnGetAPR <- function(
  granularity
  ,platform
  ,product
  ,blockrange
  ,rpc="https://a.api.s0.t.hmny.io/"
  ,dexscreen_url_base="io5.dexscreener.io"
  ,parallelPackages=c("httr","jsonlite","ether","dplyr","lubridate")
  ,parallelExport = c("fn_unixToTime","fn_hmyv2_getBlockByNumber","fn_bnToReal","fn_hmyv2_call","fn_poolInfo_allocPoints"
                    ,"fn_hmyv2_call_poolInfo","fn_hmyv2_call_totalAllocPoints","fn_hmyv2_call_emissionPerBlock"
                    ,"fn_getDexScreener","fn_hmyv2_call_balanceOf","fn_hmyv2_call_totalSupply"
                    ,"dim_pid","dim_masterchef")
){
  url_dim_masterchef <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dim_masterchef.csv"
  url_dim_pid <- "https://raw.githubusercontent.com/jhorbino93/ShinyHermes/main/dim_pid.csv"
  
  dim_masterchef <- read.csv(
    url_dim_masterchef
    ,stringsAsFactors = F
    ,colClasses=c(
      "masterchef_address"="character"
      ,"treasury_address"="character"
      ,"emission_token1_lp_address"="character"
    )
  )
  
  dim_pid <- read.csv(
    url_dim_pid
    ,stringsAsFactors = F
    ,colClasses=c(
      "address"="character"
      ,"token1_address"="character"
      ,"token2_address"="character"
    )
  )
  
  timeBlock      <- granularity*60*60 ## Number of theoretical seconds between each timepoint
  approxBlocks   <- timeBlock/2 ## Approx blocks between each time point
  
  masterchef_address  <- dim_masterchef[which(dim_masterchef$platform == platform),]$masterchef_address
  treasury_address    <- dim_masterchef[which(dim_masterchef$platform == platform),]$treasury_address
  emission_lp_address <- dim_masterchef[which(dim_masterchef$platform == platform),]$emission_token1_lp_address
  lp_address          <- dim_pid[which(dim_pid$product_name==product),]$address
  token1_address      <- dim_pid[which(dim_pid$product_name==product),]$token1_address
  pid                 <- dim_pid[which(dim_pid$product_name==product),]$pid
  
  if(dim_pid[which(dim_pid$product_name==product),]$product_type == "LP"){
    token2_address <- dim_pid[which(dim_pid$product_name==product),]$token2_address
  } else {
    if(exists(token2_address,envir=env)){rm("token2_address",envir=env)}
  }
  
  ## Query blockchain
  block_seq <- seq(content(fn_hmyv2_getBlock(rpc=rpc))$result,by=-approxBlocks,length.out=blockrange)
  cores     <- detectCores()
  cl        <- makeCluster(cores[1]-1)
  on.exit(stopCluster(cl))
  registerDoParallel(cl)
  list_res <- foreach(x=block_seq,.packages=vct_packages,.export = parallelExport) %dopar% {
    ## Get block time
    timestamp        <- fn_unixToTime(content(fn_hmyv2_getBlockByNumber(x))$result$timestamp)
    ## Get token1 balance in product
    token1_bal       <- fn_hmyv2_call_balanceOf(token1_address,lp_address,block=x,rpc=rpc)
    ## Get token2 balance in product if it has it, otherwise NA
    if(dim_pid[which(dim_pid$product_name==product),]$product_type == "LP"){
      token2_bal     <- fn_hmyv2_call_balanceOf(token2_address,lp_address,block=x,rpc=rpc)
    } else {
      token2_bal     <- NA_real_
    }
    
    ## Get product allocated multiplier points
    allocPoints      <- fn_poolInfo_allocPoints(content(fn_hmyv2_call_poolInfo(masterchef_address,pid=pid,block=x,rpc=rpc))$result)
    ## Get total platform multiplier poitns
    totalAllocPoints <- fn_hmyv2_call_totalAllocPoints(masterchef_address)
    ## Get emission rate
    em               <- fn_bnToReal(as.numeric(content(fn_hmyv2_call_emissionPerBlock(masterchef_address,block=x,rpc=rpc))$result))
    ## Get lp bal belonging to masterchef
    lp_bal           <- fn_hmyv2_call_balanceOf(lp_address,masterchef_address,rpc=rpc,block=x)
    ## Get total lp supply
    lp_totalSupply   <- fn_hmyv2_call_totalSupply(lp_address,block=x)
    
    res <- 
      data.frame(
        block             = x
        ,timestamp        = timestamp
        ,token1_bal       = token1_bal
        ,token2_bal       = token2_bal
        ,allocPoints      = allocPoints
        ,totalAllocPoints = totalAllocPoints
        ,lp_bal           = lp_bal
        ,lp_totalSupply   = lp_totalSupply
        ,em               = em
      )     
    return(res)
  }
  # stopCluster(cl)
  
  lp_bal <- bind_rows(list_res) %>% as_tibble() %>% mutate(datetime_round = round_date(timestamp,unit="hours"))
  
  ## Query Dex Screener ----
  startTime <- floor_date(min(lp_bal$timestamp),unit="hours")
  startUnix <- funConvTimeToUnix(startTime)
  endTime <- ceiling_date(max(lp_bal$timestamp),unit="hours")
  endUnix <- funConvTimeToUnix(endTime)
  
  cb <- interval(startTime,endTime) %/% hours(1)
  
  qry_url_lp_prices  <- paste0("io5.dexscreener.io/u/chart/bars/",network,"/",lp_address,"?from=",startUnix,"&to=",endUnix,"&res=",60,"&cb=",cb)
  res_lp_prices <- as.data.frame(fromJSON(content(GET(qry_url_lp_prices),"text",flatten=T))) %>% mutate_at(vars(starts_with("bars.")),as.numeric)
  
  qry_url_em_prices <- paste0("io5.dexscreener.io/u/chart/bars/",network,"/",emission_lp_address,"?from=",startUnix,"&to=",endUnix,"&res=",60,"&cb=",cb)
  res_emission_prices <- as.data.frame(fromJSON(content(GET(qry_url_em_prices),"text",flatten=T))) %>% mutate_at(vars(starts_with("bars.")),as.numeric)
  
  df_lp_prices <- 
    as_tibble(res_lp_prices) %>% 
    mutate(
      datetime_round = fnConvUnixToTime(bars.timestamp)
      ,token1Usd = bars.closeUsd
      ,token2Usd = 1/bars.close*bars.closeUsd
    ) %>%
    select(datetime_round,token1Usd,token2Usd) 
  
  df_emission_prices <- 
    as_tibble(res_emission_prices) %>%
    mutate(
      datetime_round = fnConvUnixToTime(bars.timestamp)
      ,emissionTokenUsd = bars.closeUsd
    ) %>%
    select(datetime_round,emissionTokenUsd)
  
  ## Build result DF -----
  ## Assumed multiplier weight
  annual_blocks = 60*60*24*365/2
  df_lp_res <- 
    lp_bal %>%
    inner_join(df_lp_prices) %>%
    inner_join(df_emission_prices) %>%
    group_by(datetime_round) %>%
    filter(rank(timestamp,ties.method="max")==1) %>% ungroup() %>%
    arrange(desc(datetime_round)) %>%
    mutate(
      cpf = token1_bal*token2_bal
      ,token1_tv = token1Usd*token1_bal
      ,token2_tv = token2Usd*token2_bal
      ,tv_lp = token1_tv+token2_tv
      ,owner_prop = lp_bal/lp_totalSupply
      
      ,total_annual_eff_em = em*annual_blocks*allocPoints/totalAllocPoints*emissionTokenUsd/owner_prop
      ,total_annual_eff_em_current = em[1]*annual_blocks*allocPoints[1]/totalAllocPoints*emissionTokenUsd/owner_prop
      ,apr = round(total_annual_eff_em/tv_lp*100,2)    
      ,apr_current = round(total_annual_eff_em_current/tv_lp*100,2)
    )
  
  df_plotly <- 
    df_lp_res %>%
    select(datetime_round,apr,apr_current,tv_lp,total_annual_eff_em,total_annual_eff_em_current) %>%
    pivot_longer(cols=-c("datetime_round"),names_to="val_name",values_to="value") %>%
    mutate(
      val_cat = case_when(
        val_name == "apr" ~ "APR (%)"
        ,val_name == "apr_current" ~ "APR (%)"
        ,val_name == "tv_lp" ~ "TV - LP ($)"
        ,val_name == "total_annual_eff_em" ~ "Total Annual Effective Emission ($)"
        ,val_name == "total_annual_eff_em_current" ~ "Total Annual Effective Emission ($)"
        ,T ~ "Other"
      )
      ,val_context = case_when(
        val_name %in% c("apr","tv_lp","total_annual_eff_em") ~ "Actual"
        ,val_name %in% c("apr_current","total_annual_eff_em_current") ~ "Current Context"
        ,T ~ "Other"
      )
    )
  
  ## Build plots ----
  
  p_apr <- 
    plot_ly(data=filter(df_plotly,val_cat == "APR (%)")) %>%
    add_trace(
      x=~datetime_round
      ,y=~value
      ,type="scatter"
      ,mode="line"
      ,color = ~val_context
      ,legendgroup = ~val_context
    ) %>%
    layout(
      yaxis = list(title="APR (%)")
      ,xaxis = list(title="")
      ,legend = list(
        orientation="h"
        ,xanchor = "center"
        ,x = 0.5
        ,y = 1.05
      )
    )
  
  p_lp_tv <-
    plot_ly(data=filter(df_plotly,val_cat == "TV - LP ($)")) %>%
    add_trace(
      x=~datetime_round
      ,y=~value
      ,type="scatter"
      ,mode="line"
      ,color = ~val_context
      ,legendgroup = ~val_context
      ,showlegend = F
    ) %>%
    layout(
      yaxis = list(
        title="LP Total Value $(000s)"
        ,layout.axis.tickformat=",d"
        ,layout.separators=",."
      )
      ,xaxis = list(title="")
    )
  
  p_lp_emissions <-
    plot_ly(data=filter(df_plotly,val_cat == "Total Annual Effective Emission ($)")) %>%
    add_trace(
      x=~datetime_round
      ,y=~value
      ,type="scatter"
      ,mode="line"
      ,color = ~val_context
      ,legendgroup = ~val_context
      ,showlegend = F
    ) %>%
    layout(
      yaxis = list(title="Total $ Emissions")
      ,xaxis = list(title="")
      ,legend = list(
        orientation="h"
        ,xanchor = "center"
        ,x = 0.5
        ,y = 1.05
      )
    )
  
  out_plotly <- subplot(
    p_apr
    ,p_lp_tv
    ,p_lp_emissions
    ,nrows=3
    ,shareX=T
    ,margin=0.05
    ,titleX=F
    ,titleY=T
  ) %>% layout(hovermode = "x unified")
  
  return(out_plotly)
}