fn_get_combo_w <- function(comb,nm,size=portfolio_slots){
  l = apply(comb,1,table)
  l = lapply(l,function(x,nm){
    w2 = rep(0,length(nm))
    names(w2) = nm
    w2[names(x)]=x
    return(w2)
  },nm=nm
  )
  
  return(
    matrix(unlist(l),ncol=length(nm),byrow=T,dimnames=list(seq_along(l),nm))/size
  )
}

fn_get_mat_stats <- function(m){
  
  # If return doesn't exist for one day, replace with rowMeans
  # Assumes that no position is available and investment would go to other assets evenly
  # Could make a switch to have different replacement behaviours?
  m[which(is.na(m),arr.ind=T)] <- rowMeans(m,na.rm=T)[which(is.na(m),arr.ind=T)[,1]]
  m <- m[complete.cases(m),]
  
  mean <- colMeans(m)
  cov <- cov(m)
  sd <- sqrt(diag(cov))
  
  return(
    list(
      mean = mean
      ,cov = cov
      ,sd  = sd
      ,m   = m
    )
  )
}

fn_get_synth_portfolios <- function(df,w=mtx_w){
  ## Transform to matrix
  m <- as.matrix(df[,-1])
  
  m_stats <- fn_get_mat_stats(m)
  m <- m_stats$m 
  ## Get results
  vct_rtn <-  mtx_w %*% m_stats$mean
  sd_rtn <- apply(mtx_w,1,function(x){sqrt(t(x) %*% m_stats$cov %*% x)})
  
  df_synth_portfolio_stats <-
    tibble(
      avg_rtn = as.vector(vct_rtn)
      ,sd_rtn = sd_rtn
      ,sr = avg_rtn/sd_rtn
      ,mtx_combo_id = 1:nrow(mtx_w)
    )
  return(df_synth_portfolio_stats)
}

fn_get_eff <- function(w=0.1,df=df_rtn_wide,type=c(1,2),sel_asset=NA,n_port=50){
  # Check for validity
  if(type[1] == 2 & is.na(sel_asset)){
    message("Type 2 output selected but no asset chosen, defaulting to first in input matrix")
    sel_asset = colnames(df)[2]
  }
  
  m <- as.matrix(df[,-1])
  m_stats = fn_get_mat_stats(m)
  m <- m_stats$m  
  rownames(m) <- as.character(df$date)
  
  
  
  # Construct base portfolio
  port <- portfolio.spec(assets=colnames(m))
  port <- add.constraint(port,type="full_invesment")
  port <- add.constraint(port,type="box",min=0,max=1)
  
  ## Get MVP and MRP
  if(type[1] == 1){
    port_mvp <- add.objective(port,type="risk",name="var")
    port_mvp_opt <- optimize.portfolio(R=m,portfolio=port_mvp,optimize_method="ROI")
    min_rtn <- (port_mvp_opt$weights%*%m_stats$mean)[1]
    max_rtn <- max(m_stats$mean)
    target_rtn <- seq(min_rtn,max_rtn,length.out=n_port)
  } else if (type[1]==2){
    # Add group, group min weight and max weight constraints
    
    tgt_ticker_pos <- seq_along(colnames(m))[as.integer(colnames(m)) == sel_asset]
    vct_assets <- seq_along(colnames(m))
    # group_list <- as.list(vct_assets)
    
    vct_assets_min_w <- rep(0,length(vct_assets))
    names(vct_assets_min_w) <- colnames(m)
    vct_assets_min_w[tgt_ticker_pos] <- w
    
    vct_assets_max_w <- rep(1-w,length(vct_assets))
    names(vct_assets_max_w) <- colnames(m)
    vct_assets_max_w[tgt_ticker_pos] <- 1
    
    port         <- add.constraint(port,type="box",min=vct_assets_min_w,max=vct_assets_max_w)
    
    port_mvp     <- add.objective(port,type="risk",name="var")
    port_mvp_out <- optimize.portfolio(R=m,portfolio=port_mvp,optimize_method="ROI")
    
    port_mrp <- add.objective(port,type="return",name="mean")
    port_mrp_opt <- optimize.portfolio(R=m,portfolio=port_mrp,optimize_method="ROI")
    
    min_rtn <- (port_mvp_out$weights%*%m_stats$mean)[1]
    max_rtn <- (port_mrp_opt$weights%*%m_stats$mean)[1]
    
    min_rtn <- if(is.na(min_rtn)){0} else {min_rtn}
    target_rtn <- seq(min_rtn,max_rtn,length.out=n_port)
  }
  
  list_eff <- lapply(seq_along(target_rtn),fn_get_eff_ports
                     ,target_rtn=target_rtn
                     ,m=m
                     ,cov_rtn=m_stats$cov
                     ,mean_rtn=m_stats$mean
                     ,port=port)
  
  df_eff <- bind_rows(list_eff,.id="eff_id") %>% as_tibble() %>% mutate(sr=avg_rtn/sd_rtn)
  
  df_single_port <- tibble(
    rtn = m_stats$mean
    ,sd = m_stats$sd
    ,sr = rtn/sd
    ,security_id = colnames(m)
  )
  
  if (type[1]==1){
    df_single_port <- tibble(
      rtn = m_stats$mean
      ,sd = m_stats$sd
      ,sr = rtn/sd
      ,security_id = colnames(m)
    )
    return(
      list(
        eff = df_eff
        ,stats_single_port = df_single_port
      )
    )
  } else if(type[1]==2){
    df_eff <- df_eff %>% mutate(tgt_w = w)
    return(
      eff = df_eff
    )
  }
}


fn_get_eff_ports <- function(k,target_rtn=target_rtn,m=m,cov_rtn=m_stats$cov,mean_rtn=m_stats$mean,port=port){
  l=length(target_rtn)
  eff_rtn <- double(l)
  eff_risk <- double(l)
  eff_w <- mat.or.vec(nr=l,nc=ncol(m))
  colnames(eff_w) <- colnames(m)
  
  eff_port <- add.constraint(port, type = "return", name = "mean", return_target = target_rtn[k])
  eff_port <- add.objective(eff_port, type = "risk", name = "var")
  eff_port_opt <- optimize.portfolio(m, eff_port,optimize_method = "ROI")
  
  res <-
    data.frame(
      sd_rtn = sqrt(t(eff_port_opt$weights) %*% cov_rtn %*% eff_port_opt$weights)
      ,avg_rtn = eff_port_opt$weights %*% mean_rtn
      ,w = t(eff_port_opt$weights)
    )
}